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

use std::collections::HashMap;

use anyhow::anyhow;
use clickhouse::Client;
use futures_async_stream::for_await;
use serde_json::Value;
use crate::common::ClickHouseCommon;
use serde_with::serde_as;
use serde_derive::{Deserialize};
use serde::ser::{Serialize, Serializer, SerializeStruct};
use crate::sink::{
    Result, Sink, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION,
    SINK_TYPE_UPSERT,
};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use core::fmt::Debug;
use clickhouse::Row;

use super::utils::{gen_append_only_message_stream, AppendOnlyAdapterOpts};



#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct ClickHouseConfig {
    #[serde(flatten)]
    pub common: ClickHouseCommon,

    pub r#type: String, // accept "append-only", "debezium", or "upsert"
}

#[derive(Clone)]
pub struct ClickHouseSink<const APPEND_ONLY: bool> {
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: Client,
}

impl ClickHouseConfig{
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<ClickHouseConfig>(serde_json::to_value(properties).unwrap()).map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_TYPE_DEBEZIUM
            && config.r#type != SINK_TYPE_UPSERT
        {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_DEBEZIUM,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}
// impl<const APPEND_ONLY: bool> KinesisSink<APPEND_ONLY> {
//     pub async fn new(
//         config: KinesisSinkConfig,
//         schema: Schema,
//         pk_indices: Vec<usize>,
//     ) -> Result<Self> {
//         let client = config
//             .common
//             .build_client()
//             .await
//             .map_err(SinkError::Kinesis)?;
//         Ok(Self {
//             config,
//             schema,
//             pk_indices,
//             client,
//         })
//     }

//     pub async fn validate(config: KinesisSinkConfig, pk_indices: Vec<usize>) -> Result<()> {
//         // For upsert Kafka sink, the primary key must be defined.
//         if !APPEND_ONLY && pk_indices.is_empty() {
//             return Err(SinkError::Config(anyhow!(
//                 "primary key not defined for {} kafka sink (please define in `primary_key` field)",
//                 config.r#type
//             )));
//         }

//         // check reachability
//         let client = config.common.build_client().await?;
//         client
//             .list_shards()
//             .stream_name(&config.common.stream_name)
//             .send()
//             .await
//             .map_err(|e| {
//                 tracing::warn!("failed to list shards: {}", DisplayErrorContext(&e));
//                 SinkError::Kinesis(anyhow!("failed to list shards: {}", DisplayErrorContext(e)))
//             })?;
//         Ok(())
//     }

//     async fn put_record(&self, key: &str, payload: Blob) -> Result<PutRecordOutput> {
//         // todo: switch to put_records() for batching
//         Retry::spawn(
//             ExponentialBackoff::from_millis(100).map(jitter).take(3),
//             || async {
//                 self.client
//                     .put_record()
//                     .stream_name(&self.config.common.stream_name)
//                     .partition_key(key)
//                     .data(payload.clone())
//                     .send()
//                     .await
//             },
//         )
//         .await
//         .map_err(|e| {
//             tracing::warn!(
//                 "failed to put record: {} to {}",
//                 DisplayErrorContext(&e),
//                 self.config.common.stream_name
//             );
//             SinkError::Kinesis(anyhow!(
//                 "failed to put record: {} to {}",
//                 DisplayErrorContext(e),
//                 self.config.common.stream_name
//             ))
//         })
//     }

//     async fn upsert(&self, chunk: StreamChunk) -> Result<()> {
//         let upsert_stream = gen_upsert_message_stream(
//             &self.schema,
//             &self.pk_indices,
//             chunk,
//             UpsertAdapterOpts::default(),
//         );

//         crate::impl_load_stream_write_record!(upsert_stream, self.put_record);
//         Ok(())
//     }

//     async fn append_only(&self, chunk: StreamChunk) -> Result<()> {
//         let append_only_stream = gen_append_only_message_stream(
//             &self.schema,
//             &self.pk_indices,
//             chunk,
//             AppendOnlyAdapterOpts::default(),
//         );

//         crate::impl_load_stream_write_record!(append_only_stream, self.put_record);
//         Ok(())
//     }

//     async fn debezium_update(&self, chunk: StreamChunk, ts_ms: u64) -> Result<()> {
//         let dbz_stream = gen_debezium_message_stream(
//             &self.schema,
//             &self.pk_indices,
//             chunk,
//             ts_ms,
//             DebeziumAdapterOpts::default(),
//         );

//         crate::impl_load_stream_write_record!(dbz_stream, self.put_record);

//         Ok(())
//     }
// }
impl<const APPEND_ONLY: bool> ClickHouseSink<APPEND_ONLY> {
    pub async fn new(
            config: ClickHouseConfig,
            schema: Schema,
            pk_indices: Vec<usize>,
        ) -> Result<Self> {
            let client = config
                .common
                .build_client()
                .await
                .map_err(SinkError::Kinesis)?;
            Ok(Self {
                config,
                schema,
                pk_indices,
                client,
            })
    }
    pub async fn validate(config: ClickHouseConfig, pk_indices: Vec<usize>) -> Result<()> {
    // For upsert Kafka sink, the primary key must be defined.
        if !APPEND_ONLY && pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {} kafka sink (please define in `primary_key` field)",
                config.r#type
            )));
        }

        // check reachability
        let client = config.common.build_client().await?;
        // client.query(query)
        Ok(())
    }   

    async fn append_only(&self, chunk: StreamChunk) -> Result<()> {
        let append_only_stream = gen_append_only_message_stream(
            &self.schema,
            &self.pk_indices,
            chunk,
            AppendOnlyAdapterOpts::default(),
        );

        #[for_await]
        for msg in append_only_stream {
            let (event_key_object, event_object) = msg?;
            self.load_stream_write_record(event_object,&self.schema)
                .await?;
        }
        Ok(())
    }

    async fn load_stream_write_record( &self,
        event_object: Option<Value>,
    schema: &Schema) -> Result<()>{
            let mut insert = self.client.insert::<Rows>(&self.config.common.table.as_str())
            .map_err(|e| SinkError::JsonParse(e.to_string()))?;
            let rows = Rows{
                schema,
                event_object,
            };
            insert.write(&rows).await.map_err(|e| SinkError::JsonParse(e.to_string()))?;
            Ok(())
    }

}
#[derive(Row)]
struct Rows<'a>{
    schema: &'a Schema,
    event_object: Option<Value>,
}
impl Serialize for Rows<'_>{

    
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer, 
    {
            let mut s = serializer.serialize_struct("rows", self.schema.len())?;
            if let Some(Value::Object(a1)) = &self.event_object{
                for (_,value) in a1.iter(){
                    s.serialize_field("",&value)?;
                }
            }
            s.end()
    }
}

