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

use core::mem;
use std::collections::HashMap;

use anyhow::anyhow;
use clickhouse::Client;
use futures_async_stream::for_await;
use risingwave_common::row::Row;
use risingwave_common::types::{DatumRef, ScalarRefImpl};
use serde_json::Value;
use crate::common::ClickHouseCommon;
use serde_with::serde_as;
use serde_derive::{Deserialize};
use serde::ser::{Serialize, Serializer, SerializeStruct};
use crate::sink::{
    Result, Sink, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION,
    SINK_TYPE_UPSERT,
};
use risingwave_common::array::{StreamChunk, RowRef, Op};
use risingwave_common::catalog::Schema;
use core::fmt::Debug;
use clickhouse::Row as ClickHouseRow;
use bytes::{BytesMut, BufMut};

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
    buffer: BytesMut,
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
            let buffer_size = client.get_buffer_size();
            Ok(Self {
                config,
                schema,
                pk_indices,
                client,
                buffer: BytesMut::with_capacity(buffer_size),
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

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut inter = self.client.insert::<Rows>(&self.config.common.table).map_err(|e|SinkError::JsonParse(e.to_string()))?;
        for (op,row) in chunk.rows(){
            if op != Op::Insert {
                continue;
            }
            self.build_row_binary(row);
            let buffer = mem::replace(&mut self.buffer,BytesMut::with_capacity(self.client.get_buffer_size()));
            inter.write_row_binary(buffer).await.map_err(|e|SinkError::JsonParse(e.to_string()))?;
        }
        inter.end().await.map_err(|e|SinkError::JsonParse(e.to_string()))?;
        Ok(())
    }

    fn build_row_binary(&mut self, row : RowRef<'_>) {
        for i in row.iter(){
            self.build_data_binary(i.unwrap())
        }
    }


    fn build_data_binary(&mut self,data: ScalarRefImpl<'_>){
        match data{
            ScalarRefImpl::Int16(v) => self.buffer.put_i16_le(v),
            ScalarRefImpl::Int32(v) => self.buffer.put_i32_le(v),
            ScalarRefImpl::Int64(v) => self.buffer.put_i64_le(v),
            ScalarRefImpl::Int256(v) => todo!(),
            ScalarRefImpl::Serial(v) => todo!(),
            ScalarRefImpl::Float32(v) => self.buffer.put_f32_le(v.into_inner()),
            ScalarRefImpl::Float64(v) => self.buffer.put_f64_le(v.into_inner()),
            ScalarRefImpl::Utf8(v) => {
                Self::put_unsigned_leb128(&mut self.buffer, v.len() as u64);
                self.buffer.put_slice(v.as_bytes());
            },
            ScalarRefImpl::Bool(v) => self.buffer.put_u8(v as _),
            ScalarRefImpl::Decimal(v) => todo!(),
            ScalarRefImpl::Interval(v) => todo!(),
            ScalarRefImpl::Date(v) => {
                let days = v.get_nums_days();
                self.buffer.put_i32_le(days);
            },
            ScalarRefImpl::Time(v) => todo!(),
            ScalarRefImpl::Timestamp(v) => {
                let micros = v.get_timestamp_micros();
                self.buffer.put_i64_le(micros);
            },
            ScalarRefImpl::Timestamptz(v) => todo!(),
            ScalarRefImpl::Jsonb(v) => todo!(),
            ScalarRefImpl::Struct(v) => todo!(),
            ScalarRefImpl::List(v) => {
                Self::put_unsigned_leb128(&mut self.buffer, v.len() as u64);
                for i in v.iter(){
                    if let Some(date) = i{
                        self.build_data_binary(date);
                    }
                }
            },
            ScalarRefImpl::Bytea(v) => todo!(),
        }
    }
    fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
        while {
            let mut byte = value as u8 & 0x7f;
            value >>= 7;
    
            if value != 0 {
                byte |= 0x80;
            }
    
            buffer.put_u8(byte);
    
            value != 0
        } {}
    }

    // async fn load_stream_write_record( &self,
    //     event_object: Option<Value>,
    // schema: &Schema) -> Result<()>{
    //         let mut insert = self.client.insert::<Rows>(&self.config.common.table.as_str())
    //         .map_err(|e| SinkError::JsonParse(e.to_string()))?;
    //         let rows = Rows{
    //             schema,
    //             event_object,
    //         };
    //         insert.write(&rows).await.map_err(|e| SinkError::JsonParse(e.to_string()))?;
    //         Ok(())
    // }

}
#[derive(ClickHouseRow)]
struct Rows{
}

