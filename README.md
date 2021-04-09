# MongoDB Rust Driver
[![Crates.io](https://img.shields.io/crates/v/mongodb.svg)](https://crates.io/crates/mongodb) [![docs.rs](https://docs.rs/mongodb/badge.svg)](https://docs.rs/mongodb) [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

This repository contains the officially supported MongoDB Rust driver, a client side library that can be used to interact with MongoDB deployments in Rust applications. It uses the [`bson`](https://docs.rs/bson) crate for BSON support. The driver contains a fully async API that supports either [`tokio`](https://crates.io/crates/tokio) (default) or [`async-std`](https://crates.io/crates/async-std), depending on the feature flags set. The driver also has a sync API that may be enabled via feature flag. 

## Index
- [Installation](#installation)
    - [Requirements](#requirements)
    - [Importing](#importing)
        - [Configuring the async runtime](#configuring-the-async-runtime)
        - [Enabling the sync API](#enabling-the-sync-api)
- [Example Usage](#example-usage)
    - [Using the async API](#using-the-async-api)
        - [Connecting to a MongoDB deployment](#connecting-to-a-mongodb-deployment)
        - [Getting a handle to a database](#getting-a-handle-to-a-database)
        - [Inserting documents into a collection](#inserting-documents-into-a-collection)
        - [Finding documents in a collection](#finding-documents-in-a-collection)
    - [Using the sync API](#using-the-sync-api)
- [Platforms](#platforms)
- [Atlas note](#atlas-note)
- [Windows DNS note](#windows-dns-note)
- [Bug Reporting / Feature Requests](#bug-reporting--feature-requests)
- [Contributing](#contributing)
- [Running the tests](#running-the-tests)
- [Continuous Integration](#continuous-integration)
- [License](#license)

## Installation
### Requirements
- Rust 1.47+
- MongoDB 3.6+

**Note**: A bug affecting Rust 1.46-1.47 may cause out-of-memory errors when compiling an application that uses the 1.1
version of the driver with a framework like actix-web. Upgrading Rust to 1.48+ or the driver to 1.2+ fixes this
issue. For more information, see https://github.com/rust-lang/rust/issues/75992.

### Importing
The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in your application, simply add it to your project's `Cargo.toml`.
```toml
[dependencies]
mongodb = "2.0.0-alpha.1"
```

#### Configuring the async runtime
The driver supports both of the most popular async runtime crates, namely [`tokio`](https://crates.io/crates/tokio) and [`async-std`](https://crates.io/crates/async-std). By default, the driver will use [`tokio`](https://crates.io/crates/tokio), but you can explicitly choose a runtime by specifying one of `"tokio-runtime"` or `"async-std-runtime"` feature flags in your `Cargo.toml`.

For example, to instruct the driver to work with [`async-std`](https://crates.io/crates/async-std), add the following to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.0.0-alpha.1"
default-features = false
features = ["async-std-runtime"]
```

#### Enabling the sync API
The driver also provides a blocking sync API. To enable this, add the `"sync"` feature to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.0.0-alpha.1"
default-features = false
features = ["sync"]
```
**Note:** if the sync API is enabled, the async-specific types will be privatized (e.g. `mongodb::Client`). The sync-specific types can be imported from `mongodb::sync` (e.g. `mongodb::sync::Client`).

## Example Usage
Below are simple examples of using the driver. For more specific examples and the API reference, see the driver's [docs.rs page](https://docs.rs/mongodb).

### Using the async API
#### Connecting to a MongoDB deployment
```rust
use mongodb::{Client, options::ClientOptions};
```
```rust
// Parse a connection string into an options struct.
let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;

// Manually set an option.
client_options.app_name = Some("My App".to_string());

// Get a handle to the deployment.
let client = Client::with_options(client_options)?;

// List the names of the databases in that deployment.
for db_name in client.list_database_names(None, None).await? {
    println!("{}", db_name);
}
```
#### Getting a handle to a database
```rust
// Get a handle to a database.
let db = client.database("mydb");

// List the names of the collections in that database.
for collection_name in db.list_collection_names(None).await? {
    println!("{}", collection_name);
}
```
#### Inserting documents into a collection
```rust
use mongodb::bson::{doc, Document};
```
```rust
// Get a handle to a collection in the database.
let collection = db.collection::<Document>("books");

let docs = vec![
    doc! { "title": "1984", "author": "George Orwell" },
    doc! { "title": "Animal Farm", "author": "George Orwell" },
    doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
];

// Insert some documents into the "mydb.books" collection.
collection.insert_many(docs, None).await?;
```
#### Finding documents in a collection
```rust
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Bson},
    options::FindOptions,
};
```
```rust
// Query the documents in the collection with a filter and an option.
let filter = doc! { "author": "George Orwell" };
let find_options = FindOptions::builder().sort(doc! { "title": 1 }).build();
let mut cursor = collection.find(filter, find_options).await?;

// Iterate over the results of the cursor.
while let Some(result) = cursor.next().await {
    match result {
        Ok(document) => {
            if let Some(title) = document.get("title").and_then(Bson::as_str) {
                println!("title: {}", title);
            }  else {
                println!("no title found");
            }
        }
        Err(e) => return Err(e.into()),
    }
}
```

### Using the sync API
The driver also provides a blocking sync API. See the [Installation](#enabling-the-sync-api) section for instructions on how to enable it.

The various sync-specific types are found in the `mongodb::sync` submodule rather than in the crate's top level like in the async API. The sync API calls through to the async API internally though, so it looks and behaves similarly to it.
```rust
use mongodb::{
    bson::{doc, Bson, Document},
    sync::Client,
};
```
```rust
let client = Client::with_uri_str("mongodb://localhost:27017")?;
let database = client.database("mydb");
let collection = database.collection::<Document>("books");

let docs = vec![
    doc! { "title": "1984", "author": "George Orwell" },
    doc! { "title": "Animal Farm", "author": "George Orwell" },
    doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
];

// Insert some documents into the "mydb.books" collection.
collection.insert_many(docs, None)?;

let cursor = collection.find(doc! { "author": "George Orwell" }, None)?;
for result in cursor {
    match result {
        Ok(document) => {
            if let Some(title) = document.get("title").and_then(Bson::as_str) {
                println!("title: {}", title);
            } else {
                println!("no title found");
            }
        }
        Err(e) => return Err(e.into()),
    }
}
```

## Platforms

The driver tests against Linux, MacOS, and Windows in CI.

## Atlas note

Currently, the driver has issues connecting to Atlas tiers above M2 unless the server version is at least 4.2. We're working on fixing this, but in the meantime, a workaround is to upgrade your cluster to 4.2. The driver has no known issues with either M0 or M2 instances.

## Windows DNS note

On Windows, there is a known issue in the `trust-dns-resolver` crate, which the driver uses to perform DNS lookups, that causes severe performance degradation in resolvers that use the system configuration. Since the driver uses the system configuration by default, users are recommended to specify an alternate resolver configuration on Windows until that issue is resolved. This only has an effect when connecting to deployments using a `mongodb+srv` connection string.

e.g.

``` rust
let options = ClientOptions::parse_with_resolver_config(
    "mongodb+srv://my.host.com",
    ResolverConfig::cloudflare(),
)
.await?;
let client = Client::with_options(options)?;
```

## Bug Reporting / Feature Requests
To file a bug report or submit a feature request, please open a ticket on our [Jira project](https://jira.mongodb.org/browse/RUST):
- Create an account and login at [jira.mongodb.org](https://jira.mongodb.org)
- Navigate to the RUST project at [jira.mongodb.org/browse/RUST](https://jira.mongodb.org/browse/RUST)
- Click **Create Issue** - If the ticket you are filing is a bug report, please include as much detail as possible about the issue and how to reproduce it.

Before filing a ticket, please use the search functionality of Jira to see if a similar issue has already been filed.

## Contributing

We encourage and would happily accept contributions in the form of GitHub pull requests. Before opening one, be sure to run the tests locally; check out the [testing section](#running-the-tests) for information on how to do that. Once you open a pull request, your branch will be run against the same testing matrix that we use for our [continuous integration](#continuous-integration) system, so it is usually sufficient to only run the integration tests locally against a standalone. Remember to always run the linter tests before opening a pull request.

## Running the tests
### Integration and unit tests
In order to run the tests (which are mostly integration tests), you must have access to a MongoDB deployment. You may specify a [MongoDB connection string](https://docs.mongodb.com/manual/reference/connection-string/) in the `MONGODB_URI` environment variable, and the tests will use it to connect to the deployment. If `MONGODB_URI` is unset, the tests will attempt to connect to a local deployment on port 27017.

**Note:** The integration tests will clear out the databases/collections they need to use, but they do not clean up after themselves. 

To actually run the tests, you can use `cargo` like you would in any other crate:
```bash
cargo test --verbose # runs against localhost:27017
export MONGODB_URI="mongodb://localhost:123" 
cargo test --verbose # runs against localhost:123
```

#### Auth tests
The authentication tests will only be included in the test run if certain requirements are met: 
- The deployment must have `--auth` enabled
- Credentials must be specified in `MONGODB_URI`
- The credentials specified in `MONGODB_URI` must be valid and have root privileges on the deployment
```bash
export MONGODB_URI="mongodb://user:pass@localhost:27017"
cargo test --verbose # auth tests included
```
#### Topology-specific tests
Certain tests will only be run against certain topologies. To ensure that the entire test suite is run, make sure to run the tests separately against standalone, replicated, and sharded deployments.
```bash
export MONGODB_URI="mongodb://my-standalone-host:20717" # mongod running on 27017
cargo test --verbose
export MONGODB_URI="mongodb://localhost:27018,localhost:27019,localhost:27020/?replSet=repl"
cargo test --verbose
export MONGODB_URI="mongodb://localhost:27021" # mongos running on 27021
cargo test --verbose
```

#### Run the tests with TLS/SSL
To run the tests with TLS/SSL enabled, you must enable it on the deployment and in `MONGODB_URI`. 
```bash
export MONGODB_URI="mongodb://localhost:27017/?tls=true&tlsCertificateKeyFile=cert.pem&tlsCAFile=ca.pem"
cargo test --verbose
```
**Note:** When you open a pull request, your code will be run against a comprehensive testing matrix, so it is usually not necessary to run the integration tests against all combinations of topology/auth/TLS locally.

### Linter Tests
Our linter tests use the nightly version of `rustfmt` to verify that the source is formatted properly and the stable version of `clippy` to statically detect any common mistakes.
You can use `rustup` to install them both:
```bash
rustup component add clippy --toolchain stable
rustup component add rustfmt --toolchain nightly
```
Our linter tests also use `rustdoc` to verify that all necessary documentation is present and properly formatted. `rustdoc` is included in the standard Rust distribution.

To run the linter tests, run the `check-clippy.sh`, `check-rustfmt.sh`, and `check-rustdoc.sh` scripts in the `.evergreen` directory:
```bash
bash .evergreen/check-clippy.sh && bash .evergreen/check-rustfmt.sh && bash .evergreen/check-rustdoc.sh
```

## Continuous Integration
Commits to master are run automatically on [evergreen](https://evergreen.mongodb.com/waterfall/mongo-rust-driver-stable).

## License

This project is licensed under the [Apache License 2.0](https://github.com/10gen/mongo-rust-driver/blob/master/LICENSE).
