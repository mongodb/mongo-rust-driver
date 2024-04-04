# Tracing and Logging

The driver utilizes the [`tracing`](https://crates.io/crates/tracing) crate to emit events at points of interest. To enable this, you must turn on the `tracing-unstable` feature flag.

## Stability Guarantees
This functionality is considered unstable as the `tracing` crate has not reached 1.0 yet. Future minor versions of the driver may upgrade the `tracing` dependency
to a new version which is not backwards-compatible with `Subscriber`s that depend on older versions of `tracing`.
Additionally, future minor releases may make changes such as:
* add or remove tracing events
* add or remove values attached to tracing events
* change the types and/or names of values attached to tracing events
* add or remove driver-defined tracing spans
* change the severity level of tracing events

Such changes will be called out in release notes.

## Event Targets 

Currently, events are emitted under the following targets:

| Target                      | Description                                                                                                   |
|-----------------------------|---------------------------------------------------------------------------------------------------------------|
| `mongodb::command`          | Events describing commands sent to the database and their success or failure.                                 |
| `mongodb::server_selection` | Events describing the driver's process of selecting a server in the database deployment to send a command to. |
| `mongodb::connection`       | Events describing the behavior of driver connection pools and the connections they contain.                   |

## Consuming Events
To consume events in your application, in addition to enabling the `tracing-unstable` feature flag, you must either register a `tracing`-compatible subscriber or a `log`-compatible logger, as detailed in the following sections.

### Consuming Events with `tracing`

To consume events with `tracing`, you will need to register a type implementing the `tracing::Subscriber` trait in your application, as [discussed in the `tracing` docs](https://docs.rs/tracing/latest/tracing/#in-executables).

Here's a minimal example of a program using the driver which uses a tracing subscriber.

First, add the following to `Cargo.toml`:
```toml,no_run
tracing = "LATEST_VERSION_HERE"
tracing-subscriber = "LATEST_VERSION_HERE"
mongodb = { version = "LATEST_VERSION_HERE", features = ["tracing-unstable"] }
```

And then in `main.rs`:

```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate tracing_subscriber;
# use std::env;
use mongodb::{bson::doc, error::Result, Client};

#[tokio::main]
async fn main() -> Result<()> {
    // Register a global tracing subscriber which will obey the RUST_LOG environment variable
    // config.
    tracing_subscriber::fmt::init();

    // Create a MongoDB client.
    let mongodb_uri =
        env::var("MONGODB_URI").expect("The MONGODB_URI environment variable was not set.");
    let client = Client::with_uri_str(mongodb_uri).await?;

    // Insert a document.
    let coll = client.database("test").collection("test_coll");
    coll.insert_one(doc! { "x" : 1 }).await?;

    Ok(())
}
```

This program can be run from the command line as follows, using the [`RUST_LOG`](https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/fmt/index.html#filtering-events-with-environment-variables) environment variable to configure verbosity levels and observe command-related events with severity debug or higher:
```sh,no_run
RUST_LOG='mongodb::command=debug' MONGODB_URI='YOUR_URI_HERE' cargo run
```

The output will look something like the following:
```text
2023-02-03T19:20:16.091822Z DEBUG mongodb::command: Command started topologyId="63dd5e706af9908fc834fd94" command="{\"insert\":\"test_coll\",\"ordered\":true,\"$db\":\"test\",\"lsid\":{\"id\":{\"$binary\":{\"base64\":\"y/v7PiLaRwOhT0RBFRDtNw==\",\"subType\":\"04\"}}},\"documents\":[{\"_id\":{\"$oid\":\"63dd5e706af9908fc834fd95\"},\"x\":1}]}" databaseName="test" commandName="insert" requestId=4 driverConnectionId=1 serverConnectionId=16 serverHost="localhost" serverPort=27017
2023-02-03T19:20:16.092700Z DEBUG mongodb::command: Command succeeded topologyId="63dd5e706af9908fc834fd94" reply="{\"n\":1,\"ok\":1.0}" commandName="insert" requestId=4 driverConnectionId=1 serverConnectionId=16 serverHost="localhost" serverPort=27017 durationMS=0
```

### Consuming Events with `log`

Alternatively, to consume events with `log`, you will need to add `tracing` as a dependency of your application, and enable either its `log` or `log-always` feature.
Those features are described in detail [here](https://docs.rs/tracing/latest/tracing/#log-compatibility). 

Here's a minimal example of a program using the driver which uses [`env_logger`](https://crates.io/crates/env_logger).

In `Cargo.toml`:
```toml,no_run
tracing = { version = "LATEST_VERSION_HERE", features = ["log"] }
mongodb = { version = "LATEST_VERSION_HERE", features = ["tracing-unstable"] }
env_logger = "LATEST_VERSION_HERE"
```

And in `main.rs`:

```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate env_logger;
use std::env;
use mongodb::{bson::doc, error::Result, Client};

#[tokio::main]
async fn main() -> Result<()> {
    // Register a global logger.
    env_logger::init();

    // Create a MongoDB client.
    let mongodb_uri =
        env::var("MONGODB_URI").expect("The MONGODB_URI environment variable was not set.");
    let client = Client::with_uri_str(mongodb_uri).await?;

    // Insert a document.
    let coll = client.database("test").collection("test_coll");
    coll.insert_one(doc! { "x" : 1 }).await?;

    Ok(())
}
```

This program can be run from the command line as follows, using the [`RUST_LOG`](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) environment variable to configure verbosity levels and observe command-related messages with severity debug or higher:
```sh,no_run
RUST_LOG='mongodb::command=debug' MONGODB_URI='YOUR_URI_HERE' cargo run
```

The output will look something like the following:
```text
2023-02-03T19:20:16.091822Z DEBUG mongodb::command: Command started topologyId="63dd5e706af9908fc834fd94" command="{\"insert\":\"test_coll\",\"ordered\":true,\"$db\":\"test\",\"lsid\":{\"id\":{\"$binary\":{\"base64\":\"y/v7PiLaRwOhT0RBFRDtNw==\",\"subType\":\"04\"}}},\"documents\":[{\"_id\":{\"$oid\":\"63dd5e706af9908fc834fd95\"},\"x\":1}]}" databaseName="test" commandName="insert" requestId=4 driverConnectionId=1 serverConnectionId=16 serverHost="localhost" serverPort=27017
2023-02-03T19:20:16.092700Z DEBUG mongodb::command: Command succeeded topologyId="63dd5e706af9908fc834fd94" reply="{\"n\":1,\"ok\":1.0}" commandName="insert" requestId=4 driverConnectionId=1 serverConnectionId=16 serverHost="localhost" serverPort=27017 durationMS=0
```
