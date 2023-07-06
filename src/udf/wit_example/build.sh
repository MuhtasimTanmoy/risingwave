#!/bin/bash

set -e

path=$(dirname "$0")
cd "$path"

rustup target add wasm32-unknown-unknown

profile=release
if [ "$profile" == "dev" ]; then
    target_dir=debug
else
    target_dir=$profile
fi

cargo build --target=wasm32-unknown-unknown --profile "${profile}"

if [ "$(wasm-tools -V)" != "wasm-tools 1.0.35" ]; then
    echo "wasm-tools 1.0.35 is required"
    exit 1
fi

wasm-tools component new ./target/wasm32-unknown-unknown/"${target_dir}"/wit_example.wasm \
    -o wit_example.wasm
wasm-tools validate wit_example.wasm --features component-model

# WASI

# # if file not found, download from
# if [ ! -f wasi_snapshot_preview1.reactor.wasm ]; then
#     wget https://github.com/bytecodealliance/wasmtime/releases/download/v10.0.1/wasi_snapshot_preview1.reactor.wasm
# fi
# wasm-tools component new ./target/wasm/wasm32-wasi/debug/wasm_component.wasm \
#     -o wasm_component_wasi.wasm  \
#     --adapt wasi_snapshot_preview1=./wasi_snapshot_preview1.reactor.wasm

