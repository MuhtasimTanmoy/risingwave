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

use arrow_array::Array;

wit_bindgen::generate!({
    // optional, since there's only one world. We make it explicit here.
    world: "udf",
    // path is relative to Cargo.toml
    path:"../wit"
});

// Define a custom type and implement the generated `Udf` trait for it which
// represents implementing all the necesssary exported interfaces for this
// component.
struct MyUdf;

export_udf!(MyUdf);

// input I64Array, output BoolArray (true if > 0)
impl Udf for MyUdf {
    fn eval(batch: RecordBatch) -> Result<RecordBatch, EvalErrno> {
        // Read data from IPC buffer
        let batch = arrow_ipc::reader::StreamReader::try_new(batch.as_slice(), None).unwrap();

        // Do UDF computation (for each batch, for each row, do scalar -> scalar)
        let mut ret = arrow_array::builder::BooleanBuilder::new();
        for batch in batch {
            let batch = batch.unwrap();
            for i in 0..batch.num_rows() {
                let val = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect(format!("expected i64 array, got {:?}", batch.column(0).data_type()).as_str())
                    .value(i);
                ret.append_value(val > 0);
            }
        }

        // Write data to IPC buffer
        let mut buf = vec![];
        {
            let array = ret.finish();
            let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "result",
                arrow_schema::DataType::Boolean,
                false,
            )]);
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch =
                arrow_array::RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        Ok(buf)
    }

    fn input_schema() -> Schema {
        todo!()
    }

    fn output_schema() -> Schema {
        todo!()
    }
}
