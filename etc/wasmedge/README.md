Please note: using the MongoDB Rust driver on WasmEdge is unsupported!

This example program requires:
* WasmEdge (https://wasmedge.org/docs/start/install/)
* The Rust wasm toolchain (`rustup target add wasm32-wasi`)

To compile: `cargo build --target wasm32-wasi --release`

To run: `wasmedge target/wasm32-wasi/release/wasmedge_mongodb.wasm`