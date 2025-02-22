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

use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use futures_util::stream;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_udf::ArrowFlightUdfClient;

use super::*;

#[derive(Debug)]
pub struct UserDefinedTableFunction {
    children: Vec<BoxedExpression>,
    arg_schema: SchemaRef,
    return_type: DataType,
    client: Arc<ArrowFlightUdfClient>,
    identifier: String,
    #[allow(dead_code)]
    chunk_size: usize,
}

#[cfg(not(madsim))]
#[async_trait::async_trait]
impl TableFunction for UserDefinedTableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

#[cfg(not(madsim))]
impl UserDefinedTableFunction {
    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
        let mut columns = Vec::with_capacity(self.children.len());
        for c in &self.children {
            let val = c.eval_checked(input).await?.as_ref().try_into()?;
            columns.push(val);
        }

        let opts =
            arrow_array::RecordBatchOptions::default().with_row_count(Some(input.cardinality()));
        let input =
            arrow_array::RecordBatch::try_new_with_options(self.arg_schema.clone(), columns, &opts)
                .expect("failed to build record batch");
        #[for_await]
        for res in self
            .client
            .call_stream(&self.identifier, stream::once(async { input }))
            .await?
        {
            let output = DataChunk::try_from(&res?)?;
            yield output;
        }
    }
}

#[cfg(not(madsim))]
pub fn new_user_defined(prost: &PbTableFunction, chunk_size: usize) -> Result<BoxedTableFunction> {
    let Some(udtf) = &prost.udtf else {
        bail!("expect UDTF");
    };

    let arg_schema = Arc::new(Schema::new(
        udtf.arg_types
            .iter()
            .map::<Result<_>, _>(|t| {
                Ok(Field::new(
                    "",
                    DataType::from(t)
                        .try_into()
                        .map_err(risingwave_udf::Error::Unsupported)?,
                    true,
                ))
            })
            .try_collect()?,
    ));
    // connect to UDF service
    let client = crate::expr::expr_udf::get_or_create_client(&udtf.link)?;

    Ok(UserDefinedTableFunction {
        children: prost.args.iter().map(expr_build_from_prost).try_collect()?,
        return_type: prost.return_type.as_ref().expect("no return type").into(),
        arg_schema,
        client,
        identifier: udtf.identifier.clone(),
        chunk_size,
    }
    .boxed())
}

#[cfg(madsim)]
#[async_trait::async_trait]
impl TableFunction for UserDefinedTableFunction {
    fn return_type(&self) -> DataType {
        panic!("UDF is not supported in simulation yet");
    }

    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        panic!("UDF is not supported in simulation yet");
    }
}

#[cfg(madsim)]
pub fn new_user_defined(
    _prost: &PbTableFunction,
    _chunk_size: usize,
) -> Result<BoxedTableFunction> {
    panic!("UDF is not supported in simulation yet");
}
