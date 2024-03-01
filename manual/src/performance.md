# Performance

## `Client` Best Practices

The [`Client`](https://docs.rs/mongodb/latest/mongodb/struct.Client.html) handles many aspects of database connection behind the scenes that can require manual management for other database drivers; it discovers server topology, monitors it for any changes, and maintains an internal connection pool.  This has implications for how a `Client` should be used for best performance.

### Lifetime
A `Client` should be as long-lived as possible.  Establishing a new `Client` is relatively slow and resource-intensive, so ideally that should only be done once at application startup.  Because `Client` is implemented using an internal [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html), it can safely be shared across threads or tasks, and `clone`ing it to pass to new contexts is extremely cheap.
```rust,no_run
# extern crate mongodb;
# use mongodb::Client;
# use std::error::Error;
// This will be very slow because it's constructing and tearing down a `Client`
// with every request.
async fn handle_request_bad() -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str("mongodb://example.com").await?;
    // Do something with the client
    Ok(())
}

// This will be much faster.
async fn handle_request_good(client: &Client) -> Result<(), Box<dyn Error>> {
    // Do something with the client
    Ok(())
}
```

This is especially noticeable when using a framework that provides connection pooling; because `Client` does its own pooling internally, attempting to maintain a pool of `Client`s will (somewhat counter-intuitively) result in worse performance than using a single one.

### Runtime

A `Client` is implicitly bound to the instance of the `tokio` runtime in which it was created.  Attempting to execute operations on a different runtime instance will cause incorrect behavior and unpredictable failures.  This is easy to accidentally invoke when testing, as the `tokio::test` helper macro creates a new runtime for each test.
```rust,no_run
# extern crate mongodb;
# extern crate once_cell;
# extern crate tokio;
# use mongodb::Client;
# use std::error::Error;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;

static CLIENT: Lazy<Client> = Lazy::new(|| {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        Client::with_uri_str("mongodb://example.com").await.unwrap()
    })
});

// This will inconsistently fail.
#[tokio::test]
async fn test_list_dbs() -> Result<(), Box<dyn Error>> {
    CLIENT.list_database_names(None, None).await?;
    Ok(())
}
```
To work around this issue, either create a new `Client` for every async test, or bundle the `Runtime` along with the client and don't use the test helper macros.
```rust,no_run
# extern crate mongodb;
# extern crate once_cell;
# extern crate tokio;
# use mongodb::Client;
# use std::error::Error;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;

static CLIENT_RUNTIME: Lazy<(Client, Runtime)> = Lazy::new(|| {
    let rt = Runtime::new().unwrap();
    let client = rt.block_on(async {
        Client::with_uri_str("mongodb://example.com").await.unwrap()
    });
    (client, rt)
});

#[test]
fn test_list_dbs() -> Result<(), Box<dyn Error>> {
    let (client, rt) = &*CLIENT_RUNTIME;
    rt.block_on(async {
        client.list_database_names(None, None).await
    })?;
    Ok(())
}
```
or
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# use mongodb::Client;
# use std::error::Error;
#[tokio::test]
async fn test_list_dbs() -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str("mongodb://example.com").await?;
    client.list_database_names(None, None).await?;
    Ok(())
}
```

## Parallelism

Where data operations are naturally parallelizable, spawning many asynchronous tasks that use the driver concurrently is often the best way to achieve maximum performance, as the driver is designed to work well in such situations.
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# use mongodb::{bson::Document, Client, error::Result};
# use tokio::task;
#
# async fn start_workers() -> Result<()> {
let client = Client::with_uri_str("mongodb://example.com").await?;

for i in 0..5 {
    let client_ref = client.clone();

    task::spawn(async move {
        let collection = client_ref.database("items").collection::<Document>(&format!("coll{}", i));

        // Do something with the collection
    });
}
#
# Ok(())
# }
```