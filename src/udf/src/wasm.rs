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

#![expect(dead_code)]

use std::sync::{Arc, Mutex};

use itertools::Itertools;
use tracing::debug;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Store};

pub mod component {
    mod bindgen {
        wasmtime::component::bindgen!({
            world: "udf",
            path: "wit/udf.wit"
        });
    }
    pub use bindgen::{EvalErrno, RecordBatch as WasmRecordBatch, Schema, Udf};
}

/// Host state
///
/// Currently this is only a placeholder. No states.
struct WasmState {}

type ArrowResult<T> = std::result::Result<T, arrow_schema::ArrowError>;
type WasmtimeResult<T> = std::result::Result<T, wasmtime::Error>;

pub struct InstantiatedComponent {
    store: Arc<Mutex<Store<WasmState>>>,
    bindings: component::Udf,
    #[expect(dead_code)]
    instance: wasmtime::component::Instance,
}

use convert::*;
mod convert {
    use super::*;

    pub fn to_wasm_batch(
        batch: arrow_array::RecordBatch,
    ) -> WasmUdfResult<component::WasmRecordBatch> {
        let mut buf = vec![];
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        Ok(buf)
    }

    pub fn from_wasm_batch(
        batch: &component::WasmRecordBatch,
    ) -> WasmUdfResult<impl Iterator<Item = ArrowResult<arrow_array::RecordBatch>> + '_> {
        let reader = arrow_ipc::reader::StreamReader::try_new(&batch[..], None).unwrap();

        Ok(reader)
    }

    // pub fn from_wasm_schema(schema: &component::Schema) -> WasmUdfResult<arrow_schema::SchemaRef>
    // { }
}

impl InstantiatedComponent {
    pub fn eval(&self, input: arrow_array::RecordBatch) -> WasmUdfResult<arrow_array::RecordBatch> {
        // let input_schema = self.bindings.call_input_schema(&mut self.store)?;
        // let output_schema = self.bindings.call_output_schema(&mut self.store)?;

        let input = to_wasm_batch(input)?;
        let result = self
            .bindings
            .call_eval(&mut *self.store.lock().unwrap(), &input)??;
        let result = from_wasm_batch(&result)?;
        let Some((record_batch,))= result.collect_tuple() else {
            return Err(WasmUdfError::Encoding("should return only one record batch in IPC buffer".to_string()));
        };
        Ok(record_batch?)
    }
}

/// The interface to interact with the wasm engine.
///
/// It can be safely shared across threads and is a cheap cloneable handle to the actual engine.
#[derive(Clone)]
pub struct WasmEngine {
    engine: wasmtime::Engine,
}

impl WasmEngine {
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        // Is this expensive?
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);

        Self {
            engine: wasmtime::Engine::new(&config).expect("failed to create wasm engine"),
        }
    }

    pub fn get_or_create() -> Self {
        use std::sync::LazyLock;
        static WASM_ENGINE: LazyLock<WasmEngine> = LazyLock::new(WasmEngine::new);
        WASM_ENGINE.clone()
    }

    #[tracing::instrument(skip_all)]
    pub fn compile_component(&self, binary: &[u8]) -> WasmUdfResult<Vec<u8>> {
        // This is expensive.
        let component = Component::from_binary(&self.engine, binary)?;
        tracing::info!("wasm component loaded");

        // This function is similar to the Engine::precompile_module method where it produces an
        // artifact of Wasmtime which is suitable to later pass into Module::deserialize. If a
        // module is never instantiated then it’s recommended to use Engine::precompile_module
        // instead of this method, but if a module is both instantiated and serialized then this
        // method can be useful to get the serialized version without compiling twice.
        let serialized = component.serialize()?;
        debug!(
            "compile component, size: {} -> {}",
            binary.len(),
            serialized.len()
        );

        // check the component can be instantiated
        let linker = Linker::new(&self.engine);
        // A Store is intended to be a short-lived object in a program. No form of GC is
        // implemented at this time so once an instance is created within a Store it will not be
        // deallocated until the Store itself is dropped. This makes Store unsuitable for
        // creating an unbounded number of instances in it because Store will never release this
        // memory. It's recommended to have a Store correspond roughly to the lifetime of a
        // "main instance" that an embedding is interested in executing.

        // So this is cheap?
        let mut store = Store::new(&self.engine, WasmState {});
        let (_bindings, _instance) = component::Udf::instantiate(&mut store, &component, &linker)?;

        // Ok(InstantiatedComponent {
        //     store: Arc::new(Mutex::new(store)),
        //     bindings,
        //     instance,
        // })
        Ok(serialized)
    }

    #[tracing::instrument(skip_all)]
    pub fn load_component(
        &self,
        serialized_component: &[u8],
    ) -> WasmUdfResult<InstantiatedComponent> {
        // This is fast.
        let component = unsafe {
            // safety: it's serialized by ourself
            // https://docs.rs/wasmtime/latest/wasmtime/struct.Module.html#unsafety-1
            Component::deserialize(&self.engine, serialized_component)?
        };

        let linker = Linker::new(&self.engine);
        let mut store = Store::new(&self.engine, WasmState {});
        let (bindings, instance) = component::Udf::instantiate(&mut store, &component, &linker)?;

        Ok(InstantiatedComponent {
            store: Arc::new(Mutex::new(store)),
            bindings,
            instance,
        })
    }
}

pub type WasmUdfResult<T> = std::result::Result<T, WasmUdfError>;

#[derive(thiserror::Error, Debug)]
pub enum WasmUdfError {
    #[error("wasm error: {0}")]
    Wasmtime(#[from] wasmtime::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("eval error: {0}")]
    Eval(#[from] component::EvalErrno),
    #[error("{0}")]
    Encoding(String),
}
