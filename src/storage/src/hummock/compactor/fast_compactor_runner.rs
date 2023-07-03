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
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc};
use std::time::Instant;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{CompactTask, SstableInfo};

use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::compactor::{CompactorContext, RemoteBuilderFactory, TaskConfig};
use crate::hummock::multi_builder::{
    CapacitySplitTableBuilder, SplitTableOutput, TableBuilderFactory,
};
use crate::hummock::sstable_store::{BlockStream, SstableStoreRef};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    Block, BlockHolder, BlockIterator, CachePolicy, CompactionDeleteRanges, CompressionAlgorithm,
    HummockResult, SstableBuilderOptions, StreamingSstableWriterFactory, TableHolder,
    Xor8FilterBuilder,
};
use crate::monitor::StoreLocalStatistic;

/// Iterates over the KV-pairs of an SST while downloading it.
pub struct BlockStreamIterator {
    /// The downloading stream.
    block_stream: BlockStream,

    next_block_index: usize,

    /// For key sanity check of divided SST and debugging
    sstable: TableHolder,
    iter: Option<BlockIterator>,
    task_progress: Arc<TaskProgress>,
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
        block_stream: BlockStream,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        Self {
            block_stream,
            next_block_index: 0,
            sstable,
            iter: None,
            task_progress,
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

impl Drop for BlockStreamIterator {
    fn drop(&mut self) {
        self.task_progress
            .num_pending_read_io
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<BlockStreamIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    sstables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    task_progress: Arc<TaskProgress>,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        sst_infos: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            sstables: sst_infos,
            sstable_store,
            task_progress,
            stats: StoreLocalStatistic::default(),
        }
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0).await
    }

    pub async fn next_sstable(&mut self) -> HummockResult<()> {
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
        self.cur_idx < self.sstables.len()
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(&mut self, idx: usize) -> HummockResult<()> {
        self.sstable_iter.take();
        self.cur_idx = idx;
        if self.cur_idx < self.sstables.len() {
            let table_info = &self.sstables[self.cur_idx];
            let sstable = self
                .sstable_store
                .sstable(table_info, &mut self.stats)
                .verbose_instrument_await("stream_iter_sstable")
                .await?;
            let stats_ptr = self.stats.remote_io_time.clone();
            let now = Instant::now();
            self.task_progress
                .num_pending_read_io
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let block_stream = self
                .sstable_store
                .get_stream(sstable.value(), None)
                .verbose_instrument_await("stream_iter_get_stream")
                .await?;

            // Determine time needed to open stream.
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);

            let sstable_iter =
                BlockStreamIterator::new(sstable, block_stream, self.task_progress.clone());
            self.sstable_iter = Some(sstable_iter);
        }
        Ok(())
    }
}

pub struct CompactorRunner {
    left: Box<ConcatSstableIterator>,
    right: Box<ConcatSstableIterator>,
    executor:
        CompactTaskExecutor<RemoteBuilderFactory<StreamingSstableWriterFactory, Xor8FilterBuilder>>,
}

impl CompactorRunner {
    pub fn new(
        context: Arc<CompactorContext>,
        task: CompactTask,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        options.capacity = task.target_file_size as usize;
        let get_id_time = Arc::new(AtomicU64::new(0));

        let key_range = KeyRange::inf();

        let task_config = TaskConfig {
            key_range: key_range.clone(),
            cache_policy: CachePolicy::NotFill,
            gc_delete_keys: task.gc_delete_keys,
            watermark: task.watermark,
            stats_target_table_ids: Some(HashSet::from_iter(task.existing_table_ids.clone())),
            task_type: task.task_type(),
            is_target_l0_or_lbase: task.target_level == 0 || task.target_level == task.base_level,
            split_by_table: task.split_by_state_table,
            split_weight_by_vnode: task.split_weight_by_vnode,
        };
        let factory = StreamingSstableWriterFactory::new(context.sstable_store.clone());
        let builder_factory = RemoteBuilderFactory::<_, Xor8FilterBuilder> {
            sstable_object_id_manager: context.sstable_object_id_manager.clone(),
            limiter: context.output_memory_limiter.clone(),
            options,
            policy: task_config.cache_policy,
            remote_rpc_cost: get_id_time.clone(),
            filter_key_extractor,
            sstable_writer_factory: factory,
            _phantom: PhantomData,
        };
        let sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            context.compactor_metrics.clone(),
            Some(task_progress.clone()),
            Arc::new(CompactionDeleteRanges::default()),
            task_config.key_range.clone(),
            task_config.is_target_l0_or_lbase,
            task_config.split_by_table,
            task_config.split_weight_by_vnode,
        );
        let left = Box::new(ConcatSstableIterator::new(
            task.input_ssts[0].table_infos.clone(),
            context.sstable_store.clone(),
            task_progress.clone(),
        ));
        let right = Box::new(ConcatSstableIterator::new(
            task.input_ssts[1].table_infos.clone(),
            context.sstable_store.clone(),
            task_progress.clone(),
        ));

        Self {
            executor: CompactTaskExecutor::new(sst_builder, task_config),
            left,
            right,
        }
    }

    pub async fn run(mut self) -> HummockResult<Vec<SplitTableOutput>> {
        self.left.rewind().await?;
        self.right.rewind().await?;
        while self.left.is_valid() && self.right.is_valid() {
            let ret = self
                .left
                .current_sstable()
                .key()
                .cmp(&self.right.current_sstable().key());
            let (first, second) = if ret == Ordering::Less {
                (&mut self.left, &mut self.right)
            } else {
                (&mut self.right, &mut self.left)
            };
            assert!(ret != Ordering::Equal);
            if first.current_sstable().iter.is_none() {
                let right_key = second.current_sstable().key();
                while first.current_sstable().is_valid() {
                    let full_key = FullKey::decode(first.current_sstable().next_block_largest());
                    if full_key.ge(&right_key) {
                        break;
                    }
                    let full_key =
                        FullKey::decode(first.current_sstable().next_block_smallest()).to_vec();
                    let (block, uncompressed_size) = first
                        .current_sstable()
                        .download_next_block()
                        .await?
                        .unwrap();
                    self.executor
                        .builder
                        .add_raw_block(block, full_key, false, uncompressed_size)
                        .await?;
                    self.executor.clear();
                }
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                    continue;
                }
                first.init_block_iter().await?;
            }

            let target_key = second.current_sstable().key();
            let iter = first.sstable_iter.as_mut().unwrap().iter.as_mut().unwrap();
            self.executor.run(iter, target_key).await?;
            if !iter.is_valid() {
                first.sstable_iter.as_mut().unwrap().iter.take();
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                }
            }
        }
        let rest_data = if !self.left.is_valid() {
            if !self.right.is_valid() {
                return self.executor.builder.finish().await;
            }
            &mut self.right
        } else {
            &mut self.left
        };
        {
            let sstable_iter = rest_data.sstable_iter.as_mut().unwrap();
            let target_key = FullKey::decode(&sstable_iter.sstable.value().meta.largest_key);
            if let Some(iter) = sstable_iter.iter.as_mut() {
                self.executor.run(iter, target_key).await?;
                assert!(!iter.is_valid());
            }
        }

        while rest_data.is_valid() {
            let sstable_iter = rest_data.sstable_iter.as_mut().unwrap();
            while sstable_iter.is_valid() {
                let full_key = FullKey::decode(sstable_iter.next_block_smallest()).to_vec();
                let (block, uncompressed_size) = sstable_iter.download_next_block().await?.unwrap();
                self.executor
                    .builder
                    .add_raw_block(block, full_key, false, uncompressed_size)
                    .await?;
            }
            rest_data.next_sstable().await?;
        }
        self.executor.builder.finish().await
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

    fn clear(&mut self) {
        if !self.last_key.is_empty() {
            self.last_key = FullKey::default();
        }
        self.watermark_can_see_last_key = false;
        self.user_key_last_delete_epoch = HummockEpoch::MAX;
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

            // if let HummockValue::Put(v) = value {
            //     let k =
            // u64::from_be_bytes(iter.key().user_key.table_key.as_ref()[0..8].try_into().unwrap());
            //     if k < 110000 && k > 106000 {
            //         println!("epoch {}, table key: {}, value: {}, drop: {}, is_new_user_key: {},
            // ",  iter.key().epoch,                  k,
            // String::from_utf8(v.to_vec()).unwrap(), drop, is_new_user_key,         );
            //     }
            // }
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
