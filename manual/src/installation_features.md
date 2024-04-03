# Installation and Features

## Importing
The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in your application, simply add it to your project's `Cargo.toml`.
```toml
[dependencies]
mongodb = "2.1.0"
```

## Enabling the sync API
The driver also provides a blocking sync API. To enable this, add the `"sync"` feature to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.8.0"
features = ["sync"]
```
**Note:** The sync-specific types can be imported from `mongodb::sync` (e.g. `mongodb::sync::Client`).

## All Feature Flags

| Feature              | Description                                                                                                                           | Extra dependencies                  | Default |
|:---------------------|:--------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------|:--------|
| `sync`               | Expose the synchronous API (`mongodb::sync`).                                                                                         | n/a                                 | no      |
| `aws-auth`           | Enable support for the MONGODB-AWS authentication mechanism.                                                                          | `reqwest` 0.11                      | no      |
| `zlib-compression`   | Enable support for compressing messages with [`zlib`](https://zlib.net/)                                                              | `flate2` 1.0                        | no      |
| `zstd-compression`   | Enable support for compressing messages with [`zstd`](http://facebook.github.io/zstd/).  This flag requires Rust version 1.54.        | `zstd` 0.9.0                        | no      |
| `snappy-compression` | Enable support for compressing messages with [`snappy`](http://google.github.io/snappy/)                                              | `snap` 1.0.5                        | no      |
| `openssl-tls`        | Switch TLS connection handling to use ['openssl'](https://docs.rs/openssl/0.10.38/).                                                  | `openssl` 0.10.38                       | no      |