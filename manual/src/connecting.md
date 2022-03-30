# Connecting to the Database

## Connection String
Connecting to a MongoDB database requires using a [connection string](https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-formats), a URI of the form:
```uri
mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]
```
At its simplest this can just specify the host and port, e.g.
```uri
mongodb://mongodb0.example.com:27017
```
For the full range of options supported by the Rust driver, see the documentation for the [`ClientOptions::parse`](https://docs.rs/mongodb/latest/mongodb/options/struct.ClientOptions.html#method.parse) method.  That method will return a `ClientOptions` struct, allowing for directly querying or setting any of the options supported by the Rust driver:
```rust,no_run
# extern crate mongodb;
# use mongodb::options::ClientOptions;
# async fn run() -> mongodb::error::Result<()> {
let mut options = ClientOptions::parse("mongodb://mongodb0.example.com:27017").await?;
options.app_name = Some("My App".to_string());
# Ok(())
# }
```

## Creating a `Client`
The `Client` struct is the main entry point for the driver.  You can create one from a `ClientOptions` struct:
```rust,no_run
# extern crate mongodb;
# use mongodb::{Client, options::ClientOptions};
# async fn run() -> mongodb::error::Result<()> {
# let options = ClientOptions::parse("mongodb://mongodb0.example.com:27017").await?;
let client = Client::with_options(options)?;
# Ok(())
# }
```
As a convenience, if you don't need to modify the `ClientOptions` before creating the `Client`, you can directly create one from the connection string:
```rust,no_run
# extern crate mongodb;
# use mongodb::Client;
# async fn run() -> mongodb::error::Result<()> {
let client = Client::with_uri_str("mongodb://mongodb0.example.com:27017").await?;
# Ok(())
# }
```
`Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally, so it can safely be shared across threads or async tasks. For example:
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
For more information on the client and parallel operation, see the [Performance](performance.md) chapter.