# Introduction

[![Crates.io](https://img.shields.io/crates/v/mongodb.svg)](https://crates.io/crates/mongodb) [![docs.rs](https://docs.rs/mongodb/badge.svg)](https://docs.rs/mongodb) [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

This is the manual for the officially supported MongoDB Rust driver, a client side library that can be used to interact with MongoDB deployments in Rust applications. It uses the [`bson`](https://docs.rs/bson/latest) crate for BSON support. The driver contains a fully async API that requires [`tokio`](https://docs.rs/tokio). The driver also has a sync API that may be enabled via feature flag.

## Warning about timeouts / cancellation

In async Rust, it is common to implement cancellation and timeouts by dropping a future after a certain period of time instead of polling it to completion. This is how [`tokio::time::timeout`](https://docs.rs/tokio/latest/tokio/time/fn.timeout.html) works, for example. However, doing this with futures returned by the driver can leave the driver's internals in an inconsistent state, which may lead to unpredictable or incorrect behavior (see [RUST-937](https://jira.mongodb.org/browse/RUST-937) for more details). As such, it is **_highly_** recommended to poll all futures returned from the driver to completion. In order to still use timeout mechanisms like `tokio::time::timeout` with the driver, one option is to spawn tasks and time out on their [`JoinHandle`](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html) futures instead of on the driver's futures directly. This will ensure the driver's futures will always be completely polled while also allowing the application to continue in the event of a timeout.

e.g.
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# use std::time::Duration;
# use mongodb::{
#     Client,
#     bson::doc,
# };
#
# async fn foo() -> std::result::Result<(), Box<dyn std::error::Error>> {
#
# let client = Client::with_uri_str("mongodb://example.com").await?;
let collection = client.database("foo").collection("bar");
let handle = tokio::task::spawn(async move {
    collection.insert_one(doc! { "x": 1 }, None).await
});

tokio::time::timeout(Duration::from_secs(5), handle).await???;
# Ok(())
# }
```

## Minimum supported Rust version (MSRV)

The MSRV for this crate is currently 1.64.0. This will rarely be increased, and if it ever is,
it will only happen in a minor or major version release.
