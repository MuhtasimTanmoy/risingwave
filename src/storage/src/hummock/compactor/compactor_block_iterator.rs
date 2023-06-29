// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, LinkedList};
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc};
use std::time::Instant;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{HummockEpoch, KeyComparator};
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::compactor::TaskConfig;
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::sstable_store::{BlockStream, SstableStoreRef};
use crate::hummock::value::HummockValue;
use crate::hummock::{Block, BlockHolder, BlockIterator, HummockResult, TableHolder};
use crate::monitor::StoreLocalStatistic;

/// Iterates over the KV-pairs of an SST while downloading it.
pub struct BlockStreamIterator {
    /// The downloading stream.
    block_stream: BlockStream,

    next_block_index: usize,

    /// For key sanity check of divided SST and debugging
    sstable: TableHolder,
    existing_table_ids: HashSet<StateTableId>,
    task_progress: Arc<TaskProgress>,
    iter: Option<BlockIterator>,
}

impl BlockStreamIterator {
    // We have to handle two internal iterators.
    //   `block_stream`: iterates over the blocks of the table.
    //     `block_iter`: iterates over the KV-pairs of the current block.
    // These iterators work in different ways.

    // BlockIterator works as follows: After new(), we call seek(). That brings us
    // to the first element. Calling next() then brings us to the second element and does not
    // return anything.

    // BlockStream follows a different approach. After new(), we do not seek, instead next()
    // returns the first value.

    /// Initialises a new [`BlockStreamIterator`] which iterates over the given [`BlockStream`].
    /// The iterator reads at most `max_block_count` from the stream.
    pub fn new(
        sstable: TableHolder,
        existing_table_ids: HashSet<StateTableId>,
        block_stream: BlockStream,
        stats: &StoreLocalStatistic,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        Self {
            block_stream,
            next_block_index: 0,
            sstable,
            existing_table_ids,
            task_progress,
            iter: None,
        }
    }

    /// Wrapper function for `self.block_stream.next()` which allows us to measure the time needed.
    async fn download_next_block(&mut self) -> HummockResult<Option<(Bytes, usize)>> {
        let result = self.block_stream.next().await;
        self.next_block_index += 1;
        result
    }

    fn next_block_smallest(&self) -> &[u8] {
        self.sstable.value().meta.block_metas[self.next_block_index]
            .smallest_key
            .as_ref()
    }

    fn next_block_largest(&self) -> &[u8] {
        if self.next_block_index + 1 < self.sstable.value().meta.block_metas.len() {
            self.sstable.value().meta.block_metas[self.next_block_index + 1]
                .smallest_key
                .as_ref()
        } else {
            self.sstable.value().meta.largest_key.as_ref()
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        match self.iter.as_ref() {
            Some(iter) => iter.key(),
            None => FullKey::decode(
                self.sstable.value().meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            ),
        }
    }

    fn is_valid(&self) -> bool {
        self.next_block_index < self.sstable.value().meta.block_metas.len()
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    key_range: KeyRange,

    /// The iterator of the current table.
    sstable_iter: Option<BlockStreamIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    sstables: Vec<SstableInfo>,

    existing_table_ids: HashSet<StateTableId>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    task_progress: Arc<TaskProgress>,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        existing_table_ids: Vec<StateTableId>,
        sst_infos: Vec<SstableInfo>,
        key_range: KeyRange,
        sstable_store: SstableStoreRef,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        Self {
            key_range,
            sstable_iter: None,
            cur_idx: 0,
            sstables: sst_infos,
            existing_table_ids: HashSet::from_iter(existing_table_ids),
            sstable_store,
            task_progress,
            stats: StoreLocalStatistic::default(),
        }
    }

    #[cfg(test)]
    pub fn for_test(
        existing_table_ids: Vec<StateTableId>,
        sst_infos: Vec<SstableInfo>,
        key_range: KeyRange,
        sstable_store: SstableStoreRef,
    ) -> Self {
        Self::new(
            existing_table_ids,
            sst_infos,
            key_range,
            sstable_store,
            Arc::new(TaskProgress::default()),
        )
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0).await
    }

    pub async fn next_sstable(&mut self) -> HummockResult<()> {
        self.seek_idx(self.cur_idx + 1).await
    }

    pub async fn next_block(&mut self) -> HummockResult<()> {
        if let Some(sstable) = self.sstable_iter.as_mut() {
            sstable.iter.take();
            if sstable.is_valid() {
                return Ok(());
            }
        }
        self.seek_idx(self.cur_idx + 1).await
    }

    pub fn current_sstable(&mut self) -> &mut BlockStreamIterator {
        self.sstable_iter.as_mut().unwrap()
    }

    pub async fn init_block_iter(&mut self) -> HummockResult<()> {
        if let Some(sstable) = self.sstable_iter.as_mut() {
            let (buf, uncompressed_capacity) = sstable.download_next_block().await?.unwrap();
            let block = Block::decode(buf, uncompressed_capacity)?;
            let mut iter = BlockIterator::new(BlockHolder::from_owned_block(Box::new(block)));
            iter.seek_to_first();
            sstable.iter = Some(iter);
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.sstable_iter
            .as_ref()
            .map(|iter| iter.is_valid())
            .unwrap_or(false)
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(&mut self, idx: usize) -> HummockResult<()> {
        self.sstable_iter.take();
        self.cur_idx = idx;
        while self.cur_idx < self.sstables.len() {
            let table_info = &self.sstables[self.cur_idx];
            let found = table_info
                .table_ids
                .iter()
                .any(|table_id| self.existing_table_ids.contains(table_id));
            if !found {
                self.cur_idx += 1;
                continue;
            }
            let sstable = self
                .sstable_store
                .sstable(table_info, &mut self.stats)
                .verbose_instrument_await("stream_iter_sstable")
                .await?;
            let stats_ptr = self.stats.remote_io_time.clone();
            let now = Instant::now();
            let block_stream = self
                .sstable_store
                .get_stream(sstable.value(), None)
                .verbose_instrument_await("stream_iter_get_stream")
                .await?;

            // Determine time needed to open stream.
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);

            let sstable_iter = BlockStreamIterator::new(
                sstable,
                self.existing_table_ids.clone(),
                block_stream,
                &self.stats,
                self.task_progress.clone(),
            );
            self.sstable_iter = Some(sstable_iter);
            return Ok(());
        }
        Ok(())
    }
}

pub struct CompactorRunner<F: TableBuilderFactory> {
    left: Box<ConcatSstableIterator>,
    right: Box<ConcatSstableIterator>,
    executor: CompactTaskExecutor<F>,
}

impl<F: TableBuilderFactory> CompactorRunner<F> {
    pub fn new(
        left: ConcatSstableIterator,
        right: ConcatSstableIterator,
        builder: CapacitySplitTableBuilder<F>,
        task_config: TaskConfig,
    ) -> Self {
        Self {
            left,
            right,
            executor: CompactTaskExecutor::new(builder, task_config),
        }
    }

    pub async fn run(&mut self) -> HummockResult<()> {
        while self.left.is_valid() && self.right.is_valid() {
            let ret = self
                .left
                .current_sstable()
                .key()
                .cmp(&self.right.current_sstable().key());
            if ret == Ordering::Greater {
                std::mem::swap(&mut self.left, &mut self.right);
            }
            if self.left.current_sstable().iter.is_none() {
                let right_key = self.right.current_sstable().key();
                while self.left.current_sstable().is_valid() {
                    let full_key =
                        FullKey::decode(self.left.current_sstable().next_block_largest());
                    if full_key.ge(&right_key) {
                        break;
                    }
                    let full_key =
                        FullKey::decode(self.left.current_sstable().next_block_smallest()).to_vec();
                    let (block, uncompressed_size) = self
                        .left
                        .current_sstable()
                        .download_next_block()
                        .await?
                        .unwrap();
                    self.executor
                        .builder
                        .add_raw_block(block, full_key, false, uncompressed_size)
                        .await?;
                    if !self.executor.last_key.is_empty() {
                        self.executor.last_key = FullKey::default();
                    }
                }
                if !self.left.current_sstable().is_valid() {
                    self.left.next_sstable().await?;
                    continue;
                }
                self.left.init_block_iter().await?;
                if !self.executor.last_key.is_empty() {
                    self.executor.last_key = FullKey::default();
                }
            }

            assert!(self.left.is_valid());
            let target_key = self.right.current_sstable().key();
            let mut iter = self
                .left
                .sstable_iter
                .as_mut()
                .unwrap()
                .iter
                .as_mut()
                .unwrap();
            self.executor.run(&mut iter, target_key).await?;
        }
        if !self.left.is_valid() {
            if !self.right.is_valid() {
                return Ok(());
            }
            std::mem::swap(&mut self.left, &mut self.right);
        }
        {
            let sstable_iter = self.left.sstable_iter.as_mut().unwrap();
            let target_key = FullKey::decode(&sstable_iter.sstable.value().meta.largest_key);
            if let Some(iter) = sstable_iter.iter.as_mut() {
                self.executor.run(iter, target_key).await?;
                assert!(!iter.is_valid());
            }
        }

        while self.left.is_valid() {
            let sstable_iter = self.left.sstable_iter.as_mut().unwrap();
            while sstable_iter.is_valid() {
                let full_key =
                    FullKey::decode(self.left.current_sstable().next_block_smallest()).to_vec();
                let (block, uncompressed_size) = sstable_iter.download_next_block().await?.unwrap();
                self.executor
                    .builder
                    .add_raw_block(block, full_key, false, uncompressed_size)
                    .await?;
            }
            self.left.next_sstable().await;
        }
        Ok(())
    }
}

pub struct CompactTaskExecutor<F: TableBuilderFactory> {
    last_key: FullKey<Vec<u8>>,
    watermark_can_see_last_key: bool,
    user_key_last_delete_epoch: HummockEpoch,
    builder: CapacitySplitTableBuilder<F>,
    task_config: TaskConfig,
}

impl<F: TableBuilderFactory> CompactTaskExecutor<F> {
    pub fn new(builder: CapacitySplitTableBuilder<F>, task_config: TaskConfig) -> Self {
        Self {
            builder,
            task_config,
            last_key: FullKey::default(),
            watermark_can_see_last_key: false,
            user_key_last_delete_epoch: HummockEpoch::MAX,
        }
    }

    pub async fn run(
        &mut self,
        iter: &mut BlockIterator,
        target_key: FullKey<&[u8]>,
    ) -> HummockResult<()> {
        while iter.is_valid() && iter.key().le(&target_key) {
            let is_new_user_key =
                !self.last_key.is_empty() && iter.key().user_key != self.last_key.user_key.as_ref();
            let mut drop = false;
            let epoch = iter.key().epoch;
            let value = HummockValue::from_slice(iter.value()).unwrap();
            if is_new_user_key || self.last_key.is_empty() {
                self.last_key.set(iter.key());
                self.watermark_can_see_last_key = false;
                self.user_key_last_delete_epoch = HummockEpoch::MAX;
            }
            if (epoch <= self.task_config.watermark
                && self.task_config.gc_delete_keys
                && value.is_delete())
                || (epoch < self.task_config.watermark && self.watermark_can_see_last_key)
            {
                drop = true;
            }
            if epoch <= self.task_config.watermark {
                self.watermark_can_see_last_key = true;
            }
            if drop {
                iter.next();
                continue;
            }
            if value.is_delete() {
                self.user_key_last_delete_epoch = iter.key().epoch;
            }
            self.builder
                .add_full_key(iter.key(), value, is_new_user_key)
                .await?;
            iter.next();
        }
        Ok(())
    }
}
