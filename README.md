# MongoDB Rust Driver
[![Crates.io](https://img.shields.io/crates/v/mongodb.svg)](https://crates.io/crates/mongodb) [![docs.rs](https://docs.rs/mongodb/badge.svg)](https://docs.rs/mongodb) [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

This repository contains the officially supported MongoDB Rust driver, a client side library that can be used to interact with MongoDB deployments in Rust applications. It depends on the community-supported [`bson`](https://docs.rs/bson) library for BSON support.

## Index
- [Installation](#Installation)
- [Example Usage](#example-usage)
    - [Connecting to a MongoDB deployment](#connecting-to-a-mongodb-deployment)
    - [Getting a handle to a database](#getting-a-handle-to-a-database)
    - [Inserting documents into a collection](#inserting-documents-into-a-collection)
    - [Finding documents in a collection](#finding-documents-in-a-collection)
- [Bug Reporting / Feature Requests](#bug-reporting--feature-requests)
- [Contributing](#contributing)
- [Running the tests](#running-the-tests)
- [Continuous Integration](#continuous-integration)
- [License](#license)

## Installation
### Requirements
 - Rust 1.39+
- MongoDB 3.6+

### Importing
The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in your application, simply add it to your project's `Cargo.toml`. You will also want to add [`bson`](https://docs.rs/bson) as well.
```toml
[dependencies]
mongodb = "0.9.1"
bson = "0.14.0"
```

## Example Usage
Below are simple examples of using the driver. For more specific examples and the API reference, see the driver's [docs.rs page](https://docs.rs/mongodb).
### Connecting to a MongoDB deployment
```rust
use mongodb::{Client, options::ClientOptions};
```
```rust
// Parse a connection string into an options struct.
let mut client_options = ClientOptions::parse("mongodb://localhost:27017")?;

// Manually set an option.
client_options.app_name = Some("My App".to_string());

// Get a handle to the deployment.
let client = Client::with_options(client_options)?;

// List the names of the databases in that deployment.
for db_name in client.list_database_names(None)? {
    println!("{}", db_name);
}
```
### Getting a handle to a database
```rust
// Get a handle to a database.
let db = client.database("mydb");

// List the names of the collections in that database.
for collection_name in db.list_collection_names(None)? {
    println!("{}", collection_name);
}
```
### Inserting documents into a collection
```rust
use bson::{doc, bson};
```
```rust
// Get a handle to a collection in the database.
let collection = db.collection("books");

let docs = vec![
    doc! { "title": "1984", "author": "George Orwell" },
    doc! { "title": "Animal Farm", "author": "George Orwell" },
    doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
];

// Insert some documents into the "mydb.books" collection.
collection.insert_many(docs, None)?;
```
### Finding documents in a collection
```rust
use bson::{doc, bson};
use mongodb::options::FindOptions;
```
```rust
// Query the documents in the collection with a filter and an option.
let filter = doc! { "author": "George Orwell" };
let find_options = FindOptions::builder().sort(doc! { "title": 1 }).build();
let cursor = collection.find(filter, find_options)?;

// Iterate over the results of the cursor.
for result in cursor {
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

## Atlas note

Currently, the driver has issues connecting to Atlas tiers above M2 unless the server version is at least 4.2. We're working on fixing this, but in the meantime, a workaround is to upgrade your cluster to 4.2. The driver has no known issues with either M0 or M2 instances.

## Bug Reporting / Feature Requests
To file a bug report or submit a feature request, please open a ticket on our [Jira project](https://jira.mongodb.org/browse/RUST):
- Create an account and login at [jira.mongodb.org](https://jira.mongodb.org)
- Navigate to the RUST project at [jira.mongorb.org/browse/RUST](https://jira.mongodb.org/browse/RUST)
- Click **Create Issue** - If the ticket you are filing is a bug report, please include as much detail as possible about the issue and how to reproduce it.

Before filing a ticket, please use the search functionality of Jira to see if a similar issue has already been filed.

## Contributing

We encourage and would happily accept contributions in the form of GitHub pull requests. Before opening one, be sure to run the tests locally; check out the [testing section](#running-the-tests) for information on how to do that. Once you open a pull request, your branch will be run against the same testing matrix that we use for our [continuous integration](#continuous-integration) system, so it is usually sufficient to only run the integration tests locally against a standalone. Remember to always run the linter tests before opening a pull request.

## Running the tests
### Integration and unit tests
In order to run the tests (which are mostly integration tests), you must have access to a MongoDB deployment. You may specify a [MongoDB connection string](https://docs.mongodb.com/manual/reference/connection-string/) in the `MONGODB_URI` environment variable, and the the tests will use it to connect to the deployment. If `MONGODB_URI` is unset, the tests will attempt to connect to a local deployment on port 27017.

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
- The credentials specified in `MONGODB_URI` must be valid and have root privledges on the deployment
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
**Note:** When you open a pull request, your code will be run against a comprehensive testing matrix, so it is usually not necessary run the integration tests against all combinations of topology/auth/TLS locally.

### Linter Tests
Our linter tests use the nightly version of `rustfmt` to verify that the source is formatted properly and the stable version of `clippy` to statically detect any common mistakes.
You can use `rustup` to install them both:
```bash
rustup component add clippy --toolchain stable
rustup component add rustfmt --toolchain nightly
```
To run the linter tests, run the `check-clippy.sh` and `check-rustfmt.sh` scripts in the `.evergreen` directory:
```bash
bash .evergreen/check-clippy.sh && bash .evergreen/check-rustfmt.sh
```

## Continuous Integration
Commits to master are run automatically on [evergreen](https://evergreen.mongodb.com/waterfall/mongo-rust-driver-stable).

## License

This project is licensed under the [Apache License 2.0](https://github.com/10gen/mongo-rust-driver/blob/master/LICENSE).
