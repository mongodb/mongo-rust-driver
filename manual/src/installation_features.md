# Installation and Features

## Importing
The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in your application, simply add it to your project's `Cargo.toml`.
```toml
[dependencies]
mongodb = "2.1.0"
```

## Configuring the async runtime
The driver supports both of the most popular async runtime crates, namely [`tokio`](https://crates.io/crates/tokio) and [`async-std`](https://crates.io/crates/async-std). By default, the driver will use [`tokio`](https://crates.io/crates/tokio), but you can explicitly choose a runtime by specifying one of `"tokio-runtime"` or `"async-std-runtime"` feature flags in your `Cargo.toml`.

For example, to instruct the driver to work with [`async-std`](https://crates.io/crates/async-std), add the following to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.8.2"
default-features = false
features = ["async-std-runtime"]
```

## Enabling the sync API
The driver also provides a blocking sync API. To enable this, add the `"sync"` or `"tokio-sync"` feature to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.8.2"
features = ["tokio-sync"]
```
Using the `"sync"` feature also requires using `default-features = false`.
**Note:** The sync-specific types can be imported from `mongodb::sync` (e.g. `mongodb::sync::Client`).

## All Feature Flags

| Feature              | Description                                                                                                                           | Extra dependencies                  | Default |
|:---------------------|:--------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------|:--------|
| `tokio-runtime`      | Enable support for the `tokio` async runtime                                                                                          | `tokio` 1.0 with the `full` feature | yes     |
| `async-std-runtime`  | Enable support for the `async-std` runtime                                                                                            | `async-std` 1.0                     | no      |
| `sync`               | Expose the synchronous API (`mongodb::sync`), using an async-std backend. Cannot be used with the `tokio-runtime` feature flag.       | `async-std` 1.0                     | no      |
| `tokio-sync`         | Expose the synchronous API (`mongodb::sync`), using a tokio backend. Cannot be used with the `async-std-runtime` feature flag.        | `tokio` 1.0 with the `full` feature | no      |
| `aws-auth`           | Enable support for the MONGODB-AWS authentication mechanism.                                                                          | `reqwest` 0.11                      | no      |
| `bson-uuid-0_8`      | Enable support for v0.8 of the [`uuid`](docs.rs/uuid/0.8) crate in the public API of the re-exported `bson` crate.                    | n/a                                 | no      |
| `bson-uuid-1`        | Enable support for v1.x of the [`uuid`](docs.rs/uuid/1.0) crate in the public API of the re-exported `bson` crate.                    | n/a                                 | no      |
| `bson-chrono-0_4`    | Enable support for v0.4 of the [`chrono`](docs.rs/chrono/0.4) crate in the public API of the re-exported `bson` crate.                | n/a                                 | no      |
| `bson-serde_with`    | Enable support for the [`serde_with`](docs.rs/serde_with/latest) crate in the public API of the re-exported `bson` crate.             | `serde_with` 1.0                    | no      |
| `zlib-compression`   | Enable support for compressing messages with [`zlib`](https://zlib.net/)                                                              | `flate2` 1.0                        | no      |
| `zstd-compression`   | Enable support for compressing messages with [`zstd`](http://facebook.github.io/zstd/).  This flag requires Rust version 1.54.        | `zstd` 0.9.0                        | no      |
| `snappy-compression` | Enable support for compressing messages with [`snappy`](http://google.github.io/snappy/)                                              | `snap` 1.0.5                        | no      |
| `openssl-tls`        | Switch TLS connection handling to use ['openssl'](https://docs.rs/openssl/0.10.38/).                                                  | `openssl` 0.10.38                       | no      |