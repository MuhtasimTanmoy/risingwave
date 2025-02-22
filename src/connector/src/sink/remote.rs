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
use async_trait::async_trait;
use itertools::Itertools;
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_pb::connector_service::sink_stream_request::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_stream_request::write_batch::{
    JsonPayload, Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::table_schema::Column;
use risingwave_pb::connector_service::{SinkPayloadFormat, TableSchema};
#[cfg(test)]
use risingwave_pb::connector_service::{SinkResponse, SinkStreamRequest};
use risingwave_rpc_client::{ConnectorClient, SinkWriterStreamHandle};
#[cfg(test)]
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
#[cfg(test)]
use tonic::Status;
use tracing::error;

use crate::sink::catalog::{SinkId, SinkType};
use crate::sink::utils::{record_to_json, TimestampHandlingMode};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkWriterParam, SinkWriterV1,
    SinkWriterV1Adapter,
};
use crate::ConnectorParams;

pub const VALID_REMOTE_SINKS: [&str; 4] = ["jdbc", "iceberg", "deltalake", "elasticsearch-7"];

pub fn is_valid_remote_sink(connector_type: &str) -> bool {
    VALID_REMOTE_SINKS.contains(&connector_type)
}

#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub connector_type: String,
    pub properties: HashMap<String, String>,
}

impl RemoteConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let connector_type = values
            .get("connector")
            .expect("sink type must be specified")
            .to_string();

        if !is_valid_remote_sink(connector_type.as_str()) {
            return Err(SinkError::Config(anyhow!(
                "invalid connector type: {connector_type}"
            )));
        }

        Ok(RemoteConfig {
            connector_type,
            properties: values,
        })
    }
}

#[derive(Debug)]
pub struct RemoteSink {
    config: RemoteConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    sink_id: SinkId,
    sink_type: SinkType,
}

impl RemoteSink {
    pub fn new(
        config: RemoteConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        sink_id: SinkId,
        sink_type: SinkType,
    ) -> Self {
        Self {
            config,
            schema,
            pk_indices,
            sink_id,
            sink_type,
        }
    }
}

#[async_trait]
impl Sink for RemoteSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = SinkWriterV1Adapter<RemoteSinkWriter>;

    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer> {
        Ok(SinkWriterV1Adapter::new(
            RemoteSinkWriter::new(
                self.config.clone(),
                self.schema.clone(),
                self.pk_indices.clone(),
                writer_param.connector_params,
                self.sink_id,
            )
            .await?,
        ))
    }

    async fn validate(&self, client: Option<ConnectorClient>) -> Result<()> {
        // FIXME: support struct and array in stream sink
        let columns = self.schema.fields
            .iter()
            .map(|field| {
                if matches!(
                field.data_type,
                DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Decimal
                    | DataType::Timestamp
                    | DataType::Timestamptz
                    | DataType::Varchar
                    | DataType::Date
                    | DataType::Time
                    | DataType::Interval
                    | DataType::Jsonb
                    | DataType::Bytea
                    | DataType::List(_)
            ) {
                    Ok( Column {
                        name: field.name.clone(),
                        data_type: Some(field.data_type.to_protobuf()),
                    })
                } else {
                    Err(SinkError::Remote(format!(
                        "remote sink supports Int16, Int32, Int64, Float32, Float64, Boolean, Decimal, Time, Date, Interval, Jsonb, Timestamp, Timestamptz, List, Bytea and Varchar, got {:?}: {:?}",
                        field.name,
                        field.data_type
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let client = client.ok_or(SinkError::Remote(
            "connector node endpoint not specified or unable to connect to connector node"
                .to_string(),
        ))?;

        let table_schema = TableSchema {
            columns,
            pk_indices: self.pk_indices.iter().map(|i| *i as u32).collect_vec(),
        };

        // We validate a remote sink's accessibility as well as the pk.
        client
            .validate_sink_properties(
                self.config.connector_type.clone(),
                self.config.properties.clone(),
                Some(table_schema),
                self.sink_type.to_proto(),
            )
            .await
            .map_err(SinkError::from)
    }
}

#[derive(Debug)]
pub struct RemoteSinkWriter {
    pub connector_type: String,
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    schema: Schema,
    payload_format: SinkPayloadFormat,
    stream_handle: SinkWriterStreamHandle,
}

impl RemoteSinkWriter {
    pub async fn new(
        config: RemoteConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector_params: ConnectorParams,
        sink_id: SinkId,
    ) -> Result<Self> {
        let client = connector_params.connector_client.ok_or(SinkError::Remote(
            "connector node endpoint not specified or unable to connect to connector node"
                .to_string(),
        ))?;
        let table_schema = Some(TableSchema {
            columns: schema
                .fields()
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    data_type: Some(c.data_type().to_protobuf()),
                })
                .collect(),
            pk_indices: pk_indices.iter().map(|i| *i as u32).collect(),
        });
        let stream_handle = client
            .start_sink_writer_stream(
                config.connector_type.clone(),
                sink_id.sink_id as u64,
                config.properties.clone(),
                table_schema,
                connector_params.sink_payload_format,
            )
            .await
            .inspect_err(|e| {
                error!(
                    "failed to start sink stream for connector `{}`: {:?}",
                    &config.connector_type, e
                )
            })?;
        tracing::trace!(
            "{:?} sink stream started with properties: {:?}",
            &config.connector_type,
            &config.properties
        );

        Ok(RemoteSinkWriter {
            connector_type: config.connector_type,
            properties: config.properties,
            epoch: None,
            batch_id: 0,
            schema,
            stream_handle,
            payload_format: connector_params.sink_payload_format,
        })
    }

    #[cfg(test)]
    fn for_test(
        response_receiver: UnboundedReceiver<std::result::Result<SinkResponse, Status>>,
        request_sender: UnboundedSender<SinkStreamRequest>,
    ) -> Self {
        use risingwave_common::catalog::Field;
        let properties = HashMap::from([("output.path".to_string(), "/tmp/rw".to_string())]);

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
        ]);

        use futures::StreamExt;
        use tokio_stream::wrappers::UnboundedReceiverStream;

        let stream_handle = SinkWriterStreamHandle::new(
            request_sender,
            UnboundedReceiverStream::new(response_receiver).boxed(),
        );

        Self {
            connector_type: "file".to_string(),
            properties,
            epoch: None,
            batch_id: 0,
            schema,
            stream_handle,
            payload_format: SinkPayloadFormat::Json,
        }
    }
}

#[async_trait]
impl SinkWriterV1 for RemoteSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let payload = match self.payload_format {
            SinkPayloadFormat::Json => {
                let mut row_ops = vec![];
                for (op, row_ref) in chunk.rows() {
                    let map = record_to_json(
                        row_ref,
                        &self.schema.fields,
                        TimestampHandlingMode::String,
                    )?;
                    let row_op = RowOp {
                        op_type: op.to_protobuf() as i32,
                        line: serde_json::to_string(&map)
                            .map_err(|e| SinkError::Remote(format!("{:?}", e)))?,
                    };

                    row_ops.push(row_op);
                }
                Payload::JsonPayload(JsonPayload { row_ops })
            }
            SinkPayloadFormat::StreamChunk => {
                let prost_stream_chunk = chunk.to_protobuf();
                let binary_data = Message::encode_to_vec(&prost_stream_chunk);
                Payload::StreamChunkPayload(StreamChunkPayload { binary_data })
            }
            SinkPayloadFormat::FormatUnspecified => {
                unreachable!("should specify sink payload format")
            }
        };

        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        let batch_id = self.batch_id;
        self.stream_handle
            .write_batch(epoch, batch_id, payload)
            .await?;
        self.batch_id += 1;
        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.stream_handle.start_epoch(epoch).await?;
        self.epoch = Some(epoch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        self.stream_handle.commit(epoch).await?;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_pb::connector_service::sink_response::{
        Response, StartEpochResponse, SyncResponse, WriteResponse,
    };
    use risingwave_pb::connector_service::sink_stream_request::write_batch::Payload;
    use risingwave_pb::connector_service::sink_stream_request::{Request, SyncBatch};
    use risingwave_pb::connector_service::{SinkResponse, SinkStreamRequest};
    use risingwave_pb::data;
    use tokio::sync::mpsc;

    use crate::sink::remote::RemoteSinkWriter;
    use crate::sink::SinkWriterV1;

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::unbounded_channel();
        let (_, resp_recv) = mpsc::unbounded_channel();

        let mut sink = RemoteSinkWriter::for_test(resp_recv, request_sender);
        let chunk = StreamChunk::from_pretty(
            " i T
            + 1 Ripper
        ",
        );

        // test epoch check
        assert!(
            tokio::time::timeout(Duration::from_secs(10), sink.commit())
                .await
                .expect("test failed: should not commit without epoch")
                .is_err(),
            "test failed: no epoch check for commit()"
        );
        assert!(
            request_recv.try_recv().is_err(),
            "test failed: unchecked epoch before request"
        );

        assert!(
            tokio::time::timeout(Duration::from_secs(1), sink.write_batch(chunk))
                .await
                .expect("test failed: should not write without epoch")
                .is_err(),
            "test failed: no epoch check for write_batch()"
        );
        assert!(
            request_recv.try_recv().is_err(),
            "test failed: unchecked epoch before request"
        );
    }

    #[tokio::test]
    async fn test_remote_sink() {
        let (request_sender, mut request_receiver) = mpsc::unbounded_channel();
        let (response_sender, response_receiver) = mpsc::unbounded_channel();
        let mut sink = RemoteSinkWriter::for_test(response_receiver, request_sender);

        let chunk_a = StreamChunk::from_pretty(
            " i T
            + 1 Alice
            + 2 Bob
            + 3 Clare
        ",
        );
        let chunk_b = StreamChunk::from_pretty(
            " i T
            + 4 David
            + 5 Eve
            + 6 Frank
        ",
        );

        // test write batch
        response_sender
            .send(Ok(SinkResponse {
                response: Some(Response::StartEpoch(StartEpochResponse { epoch: 2022 })),
            }))
            .expect("test failed: failed to start epoch");
        sink.begin_epoch(2022).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));

        request_receiver
            .recv()
            .await
            .expect("test failed: failed to construct start_epoch request");

        response_sender
            .send(Ok(SinkResponse {
                response: Some(Response::Write(WriteResponse {
                    epoch: 2022,
                    batch_id: 0,
                })),
            }))
            .expect("test failed: failed to start epoch");
        sink.write_batch(chunk_a.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));
        assert_eq!(sink.batch_id, 1);
        match request_receiver.recv().await {
            Some(SinkStreamRequest {
                request: Some(Request::Write(write)),
            }) => {
                assert_eq!(write.epoch, 2022);
                assert_eq!(write.batch_id, 0);
                match write.payload.unwrap() {
                    Payload::JsonPayload(json) => {
                        let row_0 = json.row_ops.get(0).unwrap();
                        assert_eq!(row_0.line, "{\"id\":1,\"name\":\"Alice\"}");
                        assert_eq!(row_0.op_type, data::Op::Insert as i32);
                        let row_1 = json.row_ops.get(1).unwrap();
                        assert_eq!(row_1.line, "{\"id\":2,\"name\":\"Bob\"}");
                        assert_eq!(row_1.op_type, data::Op::Insert as i32);
                        let row_2 = json.row_ops.get(2).unwrap();
                        assert_eq!(row_2.line, "{\"id\":3,\"name\":\"Clare\"}");
                        assert_eq!(row_2.op_type, data::Op::Insert as i32);
                    }
                    _ => unreachable!("should be json payload"),
                }
            }
            _ => panic!("test failed: failed to construct write request"),
        }

        // test commit
        response_sender
            .send(Ok(SinkResponse {
                response: Some(Response::Sync(SyncResponse { epoch: 2022 })),
            }))
            .expect("test failed: failed to sync epoch");
        sink.commit().await.unwrap();
        let commit_request = request_receiver.recv().await.unwrap();
        match commit_request.request {
            Some(Request::Sync(SyncBatch { epoch })) => {
                assert_eq!(epoch, 2022);
            }
            _ => panic!("test failed: failed to construct sync request "),
        }

        // begin another epoch
        response_sender
            .send(Ok(SinkResponse {
                response: Some(Response::StartEpoch(StartEpochResponse { epoch: 2023 })),
            }))
            .expect("test failed: failed to start epoch");
        sink.begin_epoch(2023).await.unwrap();
        // simply keep the channel empty since we've tested begin_epoch
        let _ = request_receiver.recv().await.unwrap();
        assert_eq!(sink.epoch, Some(2023));

        // test another write
        response_sender
            .send(Ok(SinkResponse {
                response: Some(Response::Write(WriteResponse {
                    epoch: 2022,
                    batch_id: 1,
                })),
            }))
            .expect("test failed: failed to start epoch");
        sink.write_batch(chunk_b.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2023));
        assert_eq!(sink.batch_id, 2);
        match request_receiver.recv().await {
            Some(SinkStreamRequest {
                request: Some(Request::Write(write)),
            }) => {
                assert_eq!(write.epoch, 2023);
                assert_eq!(write.batch_id, 1);
                match write.payload.unwrap() {
                    Payload::JsonPayload(json) => {
                        let row_0 = json.row_ops.get(0).unwrap();
                        assert_eq!(row_0.line, "{\"id\":4,\"name\":\"David\"}");
                        assert_eq!(row_0.op_type, data::Op::Insert as i32);
                        let row_1 = json.row_ops.get(1).unwrap();
                        assert_eq!(row_1.line, "{\"id\":5,\"name\":\"Eve\"}");
                        assert_eq!(row_1.op_type, data::Op::Insert as i32);
                        let row_2 = json.row_ops.get(2).unwrap();
                        assert_eq!(row_2.line, "{\"id\":6,\"name\":\"Frank\"}");
                        assert_eq!(row_2.op_type, data::Op::Insert as i32);
                    }
                    _ => unreachable!("should be json payload"),
                }
            }
            _ => panic!("test failed: failed to construct write request"),
        }
    }
}
