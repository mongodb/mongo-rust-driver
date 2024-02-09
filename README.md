# MongoDB Rust Driver
[![Crates.io](https://img.shields.io/crates/v/mongodb.svg)](https://crates.io/crates/mongodb) [![docs.rs](https://docs.rs/mongodb/badge.svg)](https://docs.rs/mongodb) [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

This repository contains the officially supported MongoDB Rust driver, a client side library that can be used to interact with MongoDB deployments in Rust applications. It uses the [`bson`](https://docs.rs/bson/latest) crate for BSON support. The driver contains a fully async API that supports either [`tokio`](https://crates.io/crates/tokio) (default) or [`async-std`](https://crates.io/crates/async-std), depending on the feature flags set. The driver also has a sync API that may be enabled via feature flags.

For more detailed documentation, see https://www.mongodb.com/docs/drivers/rust/current/. This documentation includes detailed content about features, runnable examples, troubleshooting resources, and more.

## Index
- [Installation](#installation)
    - [Requirements](#requirements)
      - [Supported platforms](#supported-platforms)
    - [Importing](#importing)
        - [Configuring the async runtime](#configuring-the-async-runtime)
        - [Enabling the sync API](#enabling-the-sync-api)
        - [All feature flags](#all-feature-flags)
- [Example Usage](#example-usage)
    - [Using the async API](#using-the-async-api)
        - [Connecting to a MongoDB deployment](#connecting-to-a-mongodb-deployment)
        - [Getting a handle to a database](#getting-a-handle-to-a-database)
        - [Inserting documents into a collection](#inserting-documents-into-a-collection)
        - [Finding documents in a collection](#finding-documents-in-a-collection)
    - [Using the sync API](#using-the-sync-api)
- [Web Framework Examples](#web-framework-examples)
- [Note on connecting to Atlas deployments](#note-on-connecting-to-atlas-deployments)
- [Windows DNS note](#windows-dns-note)
- [Warning about timeouts / cancellation](#warning-about-timeouts--cancellation)
- [Bug Reporting / Feature Requests](#bug-reporting--feature-requests)
- [Contributing](#contributing)
- [Running the tests](#running-the-tests)
- [Continuous Integration](#continuous-integration)
- [Minimum supported Rust version (MSRV) policy](#minimum-supported-rust-version-msrv-policy)
- [License](#license)

## Installation
### Requirements
- Rust 1.61+ (See the [MSRV policy](#minimum-supported-rust-version-msrv-policy) for more information)
- MongoDB 3.6+

#### Supported Platforms

The driver tests against Linux, MacOS, and Windows in CI.

### Importing
The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in your application, simply add it to your project's `Cargo.toml`.
```toml
[dependencies]
mongodb = "2.8.1"
```

Version 1 of this crate has reached end of life and will no longer be receiving any updates or bug fixes, so all users are recommended to always depend on the latest 2.x release. See the [2.0.0 release notes](https://github.com/mongodb/mongo-rust-driver/releases/tag/v2.0.0) for migration information if upgrading from a 1.x version.

#### Configuring the async runtime
The driver supports both of the most popular async runtime crates, namely [`tokio`](https://crates.io/crates/tokio) and [`async-std`](https://crates.io/crates/async-std). By default, the driver will use [`tokio`](https://crates.io/crates/tokio), but you can explicitly choose a runtime by specifying one of `"tokio-runtime"` or `"async-std-runtime"` feature flags in your `Cargo.toml`.

For example, to instruct the driver to work with [`async-std`](https://crates.io/crates/async-std), add the following to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.8.1"
default-features = false
features = ["async-std-runtime"]
```

#### Enabling the sync API
The driver also provides a blocking sync API. To enable this, add the `"sync"` or `"tokio-sync"` feature to your `Cargo.toml`:
```toml
[dependencies.mongodb]
version = "2.8.1"
features = ["tokio-sync"]
```
Using the `"sync"` feature also requires using `default-features = false`.
**Note:** The sync-specific types can be imported from `mongodb::sync` (e.g. `mongodb::sync::Client`).

### All Feature Flags

| Feature              | Description                                                                                                                           | Extra dependencies              | Default |
|:---------------------|:--------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------|:--------|
| `tokio-runtime`      | Enable support for the `tokio` async runtime                                                                                          | `tokio` with the `full` feature | yes     |
| `async-std-runtime`  | Enable support for the `async-std` runtime                                                                                            | `async-std`                     | no      |
| `sync`               | Expose the synchronous API (`mongodb::sync`). This flag cannot be used in conjunction with either of the async runtime feature flags. | `async-std`                     | no      |
| `aws-auth`           | Enable support for the MONGODB-AWS authentication mechanism.                                                                          | `reqwest`                       | no      |
| `bson-uuid-0_8`      | Enable support for v0.8 of the [`uuid`](docs.rs/uuid/0.8) crate in the public API of the re-exported `bson` crate.                    | n/a                             | no      |
| `bson-uuid-1`        | Enable support for v1.x of the [`uuid`](docs.rs/uuid/1.0) crate in the public API of the re-exported `bson` crate.                    | n/a                             | no      |
| `bson-chrono-0_4`    | Enable support for v0.4 of the [`chrono`](docs.rs/chrono/0.4) crate in the public API of the re-exported `bson` crate.                | n/a                             | no      |
| `bson-serde_with`    | Enable support for the [`serde_with`](docs.rs/serde_with/latest) crate in the public API of the re-exported `bson` crate.             | `serde_with`                    | no      |
| `zlib-compression`   | Enable support for compressing messages with [`zlib`](https://zlib.net/)                                                              | `flate2`                        | no      |
| `zstd-compression`   | Enable support for compressing messages with [`zstd`](http://facebook.github.io/zstd/).                                               | `zstd`                          | no      |
| `snappy-compression` | Enable support for compressing messages with [`snappy`](http://google.github.io/snappy/)                                              | `snap`                          | no      |
| `openssl-tls`        | Switch TLS connection handling to use ['openssl'](https://docs.rs/openssl/0.10.38/).                                                  | `openssl`                       | no      |

## Example Usage
Below are simple examples of using the driver. For more specific examples and the API reference, see the driver's [docs.rs page](https://docs.rs/mongodb/latest).

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

A [`Collection`](https://docs.rs/mongodb/latest/mongodb/struct.Collection.html) can be parameterized with any type that implements the `Serialize` and `Deserialize` traits from the [`serde`](https://serde.rs/) crate, not just `Document`:

``` toml
# In Cargo.toml, add the following dependency.
serde = { version = "1.0", features = ["derive"] }
```

``` rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}
```

``` rust
// Get a handle to a collection of `Book`.
let typed_collection = db.collection::<Book>("books");

let books = vec![
    Book {
        title: "The Grapes of Wrath".to_string(),
        author: "John Steinbeck".to_string(),
    },
    Book {
        title: "To Kill a Mockingbird".to_string(),
        author: "Harper Lee".to_string(),
    },
];

// Insert the books into "mydb.books" collection, no manual conversion to BSON necessary.
typed_collection.insert_many(books, None).await?;
```

#### Finding documents in a collection
Results from queries are generally returned via [`Cursor`](https://docs.rs/mongodb/latest/mongodb/struct.Cursor.html), a struct which streams the results back from the server as requested. The [`Cursor`](https://docs.rs/mongodb/latest/mongodb/struct.Cursor.html) type implements the [`Stream`](https://docs.rs/futures/latest/futures/stream/index.html) trait from the [`futures`](https://crates.io/crates/futures) crate, and in order to access its streaming functionality you need to import at least one of the [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html) or [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html) traits.

``` toml
# In Cargo.toml, add the following dependency.
futures = "0.3"
```
```rust
// This trait is required to use `try_next()` on the cursor
use futures::stream::TryStreamExt;
use mongodb::{bson::doc, options::FindOptions};
```
```rust
// Query the books in the collection with a filter and an option.
let filter = doc! { "author": "George Orwell" };
let find_options = FindOptions::builder().sort(doc! { "title": 1 }).build();
let mut cursor = typed_collection.find(filter, find_options).await?;

// Iterate over the results of the cursor.
while let Some(book) = cursor.try_next().await? {
    println!("title: {}", book.title);
}
```

### Using the sync API
The driver also provides a blocking sync API. See the [Installation](#enabling-the-sync-api) section for instructions on how to enable it.

The various sync-specific types are found in the `mongodb::sync` submodule rather than in the crate's top level like in the async API. The sync API calls through to the async API internally though, so it looks and behaves similarly to it.
```rust
use mongodb::{
    bson::doc,
    sync::Client,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}
```
```rust
let client = Client::with_uri_str("mongodb://localhost:27017")?;
let database = client.database("mydb");
let collection = database.collection::<Book>("books");

let docs = vec![
    Book {
        title: "1984".to_string(),
        author: "George Orwell".to_string(),
    },
    Book {
        title: "Animal Farm".to_string(),
        author: "George Orwell".to_string(),
    },
    Book {
        title: "The Great Gatsby".to_string(),
        author: "F. Scott Fitzgerald".to_string(),
    },
];

// Insert some books into the "mydb.books" collection.
collection.insert_many(docs, None)?;

let cursor = collection.find(doc! { "author": "George Orwell" }, None)?;
for result in cursor {
    println!("title: {}", result?.title);
}
```

## Web Framework Examples
### Actix
The driver can be used easily with the Actix web framework by storing a `Client` in Actix application data. A full example application for using MongoDB with Actix can be found [here](https://github.com/actix/examples/tree/master/databases/mongodb).

### Rocket
The Rocket web framework provides built-in support for MongoDB via the Rust driver. The documentation for the [`rocket_db_pools`](https://api.rocket.rs/v0.5-rc/rocket_db_pools/index.html) crate contains instructions for using MongoDB with your Rocket application.

## Note on connecting to Atlas deployments

In order to connect to a pre-4.2 Atlas instance that's M2 or bigger, the `openssl-tls` feature flag must be enabled. The flag is not required for clusters smaller than M2 or running server versions 4.2 or newer.

## Windows DNS note

On Windows, there is a known issue in the `trust-dns-resolver` crate, which the driver uses to perform DNS lookups, that causes severe performance degradation in resolvers that use the system configuration. Since the driver uses the system configuration by default, users are recommended to specify an alternate resolver configuration on Windows until that issue is resolved. This only has an effect when connecting to deployments using a `mongodb+srv` connection string.

e.g.

``` rust
use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client,
};
```
``` rust
let options = ClientOptions::parse_with_resolver_config(
    "mongodb+srv://my.host.com",
    ResolverConfig::cloudflare(),
)
.await?;
let client = Client::with_options(options)?;
```

## Warning about timeouts / cancellation

In async Rust, it is common to implement cancellation and timeouts by dropping a future after a
certain period of time instead of polling it to completion. This is how
[`tokio::time::timeout`](https://docs.rs/tokio/1.10.1/tokio/time/fn.timeout.html) works, for
example. However, doing this with futures returned by the driver can leave the driver's internals in
an inconsistent state, which may lead to unpredictable or incorrect behavior (see [RUST-937](https://jira.mongodb.org/browse/RUST-937) for more
details). As such, it is **_highly_** recommended to poll all futures returned from the driver to
completion. In order to still use timeout mechanisms like `tokio::time::timeout` with the driver,
one option is to spawn tasks and time out on their
[`JoinHandle`](https://docs.rs/tokio/1.10.1/tokio/task/struct.JoinHandle.html) futures instead of on
the driver's futures directly. This will ensure the driver's futures will always be completely polled
while also allowing the application to continue in the event of a timeout.

e.g.
``` rust
let collection = client.database("ok").collection("ok");
let handle = tokio::task::spawn(async move {
    collection.insert_one(doc! { "x": 1 }, None).await
});

tokio::time::timeout(Duration::from_secs(5), handle).await???;
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
In order to run the tests (which are mostly integration tests), you must have access to a MongoDB deployment. You may specify a [MongoDB connection string](https://www.mongodb.com/docs/manual/reference/connection-string/) in the `MONGODB_URI` environment variable, and the tests will use it to connect to the deployment. If `MONGODB_URI` is unset, the tests will attempt to connect to a local deployment on port 27017.

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
export MONGODB_URI="mongodb://my-standalone-host:27017" # mongod running on 27017
cargo test --verbose
export MONGODB_URI="mongodb://localhost:27018,localhost:27019,localhost:27020/?replicaSet=repl" # replicaset running on ports 27018, 27019, 27020 with name repl
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

To run the linter tests, run the `check-clippy.sh`, `check-rustfmt.sh`, and `check-rustdoc.sh` scripts in the `.evergreen` directory. To run all three, use the `check-all.sh` script.
```bash
bash .evergreen/check-all.sh
```

## Continuous Integration
Commits to main are run automatically on [evergreen](https://evergreen.mongodb.com/waterfall/mongo-rust-driver).

## Minimum supported Rust version (MSRV) policy

The MSRV for this crate is currently 1.61.0. This will rarely be increased, and if it ever is,
it will only happen in a minor or major version release.

## License

This project is licensed under the [Apache License 2.0](https://github.com/10gen/mongo-rust-driver/blob/main/LICENSE).

This product includes software developed by the OpenSSL Project for use in the OpenSSL Toolkit (http://www.openssl.org/).
