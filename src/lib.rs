//! This crate contains the officially supported MongoDB Rust driver, a
//! client side library that can be used to interact with MongoDB deployments
//! in Rust applications. It uses the [`bson`] crate for BSON support.
//! The driver contains a fully async API that supports either [`tokio`](https://docs.rs/tokio) (default)
//! or [`async-std`](https://docs.rs/async_std), depending on the feature flags set. The driver also has
//! a sync API that may be enabled via the `"sync"` or `"tokio-sync"` feature flag.
//!
//! # Installation
//!
//! ## Requirements
//! - Rust 1.61+
//! - MongoDB 3.6+
//!
//! ## Importing
//! The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in
//! your application, simply add it to your project's `Cargo.toml`.
//! ```toml
//! [dependencies]
//! mongodb = "2.8.2"
//! ```
//!
//! ### Configuring the async runtime
//! The driver supports both of the most popular async runtime crates, namely
//! [`tokio`](https://crates.io/crates/tokio) and [`async-std`](https://crates.io/crates/async-std). By
//! default, the driver will use [`tokio`](https://crates.io/crates/tokio), but you can explicitly choose
//! a runtime by specifying one of `"tokio-runtime"` or `"async-std-runtime"` feature flags in your
//! `Cargo.toml`.
//!
//! For example, to instruct the driver to work with [`async-std`](https://crates.io/crates/async-std),
//! add the following to your `Cargo.toml`:
//! ```toml
//! [dependencies.mongodb]
//! version = "2.8.2"
//! default-features = false
//! features = ["async-std-runtime"]
//! ```
//!
//! ### Enabling the sync API
//! The driver also provides a blocking sync API. To enable this, add the `"sync"` or `"tokio-sync"`
//! feature to your `Cargo.toml`:
//! ```toml
//! [dependencies.mongodb]
//! version = "2.8.2"
//! features = ["tokio-sync"]
//! ```
//! Using the `"sync"` feature also requires using `default-features = false`.
//! **Note:** The sync-specific types can be imported from `mongodb::sync` (e.g.
//! `mongodb::sync::Client`).
//!
//! ### All Feature flags
//!
//! | Feature                      | Description                                                                                                                                                             | Default |
//! |:-----------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------|
//! | `tokio-runtime`              | Enable support for the `tokio` async runtime.                                                                                                                           | yes     |
//! | `async-std-runtime`          | Enable support for the `async-std` runtime.                                                                                                                             | no      |
//! | `sync`                       | Expose the synchronous API (`mongodb::sync`), using an async-std backend. Cannot be used with the `tokio-runtime` feature flag.                                         | no      |
//! | `tokio-sync`                 | Expose the synchronous API (`mongodb::sync`), using a tokio backend. Cannot be used with the `async-std-runtime` feature flag.                                          | no      |
//! | `aws-auth`                   | Enable support for the MONGODB-AWS authentication mechanism.                                                                                                            | no      |
//! | `bson-uuid-0_8`              | Enable support for v0.8 of the [`uuid`](docs.rs/uuid/0.8) crate in the public API of the re-exported `bson` crate.                                                      | no      |
//! | `bson-uuid-1`                | Enable support for v1.x of the [`uuid`](docs.rs/uuid/1.0) crate in the public API of the re-exported `bson` crate.                                                      | no      |
//! | `bson-chrono-0_4`            | Enable support for v0.4 of the [`chrono`](docs.rs/chrono/0.4) crate in the public API of the re-exported `bson` crate.                                                  | no      |
//! | `bson-serde_with`            | Enable support for the [`serde_with`](docs.rs/serde_with/latest) crate in the public API of the re-exported `bson` crate.                                               | no      |
//! | `zlib-compression`           | Enable support for compressing messages with [`zlib`](https://zlib.net/).                                                                                               | no      |
//! | `zstd-compression`           | Enable support for compressing messages with [`zstd`](http://facebook.github.io/zstd/).                                                                                 | no      |
//! | `snappy-compression`         | Enable support for compressing messages with [`snappy`](http://google.github.io/snappy/).                                                                               | no      |
//! | `openssl-tls`                | Switch TLS connection handling to use [`openssl`](https://docs.rs/openssl/0.10.38/).                                                                                    | no      |
//! | `in-use-encryption-unstable` | Enable support for client-side field level encryption and queryable encryption. This API is unstable and may be subject to breaking changes in minor releases.          | no      |
//! | `tracing-unstable`           | Enable support for emitting [`tracing`](https://docs.rs/tracing/latest/tracing/) events. This API is unstable and may be subject to breaking changes in minor releases. | no      |
//!
//! # Example Usage
//!
//! ## Using the async API
//! ### Connecting to a MongoDB deployment
//! ```no_run
//! # #[cfg(not(any(feature = "sync", feature = "tokio-sync")))]
//! # async fn foo() -> mongodb::error::Result<()> {
//! use mongodb::{Client, options::ClientOptions};
//!
//! // Parse a connection string into an options struct.
//! let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
//!
//! // Manually set an option.
//! client_options.app_name = Some("My App".to_string());
//!
//! // Get a handle to the deployment.
//! let client = Client::with_options(client_options)?;
//!
//! // List the names of the databases in that deployment.
//! for db_name in client.list_database_names(None, None).await? {
//!     println!("{}", db_name);
//! }
//! # Ok(()) }
//! ```
//! ### Getting a handle to a database
//! ```no_run
//! # async fn foo() -> mongodb::error::Result<()> {
//! # let client = mongodb::Client::with_uri_str("").await?;
//! // Get a handle to a database.
//! let db = client.database("mydb");
//!
//! // List the names of the collections in that database.
//! for collection_name in db.list_collection_names(None).await? {
//!     println!("{}", collection_name);
//! }
//! # Ok(()) }
//! ```
//! ### Inserting documents into a collection
//! ```no_run
//! # async fn foo() -> mongodb::error::Result<()> {
//! # let db = mongodb::Client::with_uri_str("").await?.database("");
//! use mongodb::bson::{doc, Document};
//!
//! // Get a handle to a collection in the database.
//! let collection = db.collection::<Document>("books");
//!
//! let docs = vec![
//!     doc! { "title": "1984", "author": "George Orwell" },
//!     doc! { "title": "Animal Farm", "author": "George Orwell" },
//!     doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
//! ];
//!
//! // Insert some documents into the "mydb.books" collection.
//! collection.insert_many(docs, None).await?;
//! # Ok(()) }
//! ```
//!
//! A [`Collection`](struct.Collection.html) can be parameterized with any type that implements the
//! `Serialize` and `Deserialize` traits from the [`serde`](https://serde.rs/) crate,
//! not just `Document`:
//!
//! ```toml
//! # In Cargo.toml, add the following dependency.
//! serde = { version = "1.0", features = ["derive"] }
//! ```
//!
//! ```no_run
//! # async fn foo() -> mongodb::error::Result<()> {
//! # let db = mongodb::Client::with_uri_str("").await?.database("");
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Book {
//!     title: String,
//!     author: String,
//! }
//!
//! // Get a handle to a collection of `Book`.
//! let typed_collection = db.collection::<Book>("books");
//!
//! let books = vec![
//!     Book {
//!         title: "The Grapes of Wrath".to_string(),
//!         author: "John Steinbeck".to_string(),
//!     },
//!     Book {
//!         title: "To Kill a Mockingbird".to_string(),
//!         author: "Harper Lee".to_string(),
//!     },
//! ];
//!
//! // Insert the books into "mydb.books" collection, no manual conversion to BSON necessary.
//! typed_collection.insert_many(books, None).await?;
//! # Ok(()) }
//! ```
//!
//! ### Finding documents in a collection
//! Results from queries are generally returned via [`Cursor`](struct.Cursor.html), a struct which streams
//! the results back from the server as requested. The [`Cursor`](struct.Cursor.html) type implements the
//! [`Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) trait from
//! the [`futures`](https://crates.io/crates/futures) crate, and in order to access its streaming
//! functionality you need to import at least one of the
//! [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html) or
//! [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html) traits.
//!
//! ```toml
//! # In Cargo.toml, add the following dependency.
//! futures = "0.3"
//! ```
//! ```no_run
//! # use serde::Deserialize;
//! # #[derive(Deserialize)]
//! # struct Book { title: String }
//! # async fn foo() -> mongodb::error::Result<()> {
//! # let typed_collection = mongodb::Client::with_uri_str("").await?.database("").collection::<Book>("");
//! // This trait is required to use `try_next()` on the cursor
//! use futures::stream::TryStreamExt;
//! use mongodb::{bson::doc, options::FindOptions};
//!
//! // Query the books in the collection with a filter and an option.
//! let filter = doc! { "author": "George Orwell" };
//! let find_options = FindOptions::builder().sort(doc! { "title": 1 }).build();
//! let mut cursor = typed_collection.find(filter, find_options).await?;
//!
//! // Iterate over the results of the cursor.
//! while let Some(book) = cursor.try_next().await? {
//!     println!("title: {}", book.title);
//! }
//! # Ok(()) }
//! ```
//!
//! ### Using the sync API
//! The driver also provides a blocking sync API. See the [Installation](#enabling-the-sync-api)
//! section for instructions on how to enable it.
//!
//! The various sync-specific types are found in the `mongodb::sync` submodule rather than in the
//! crate's top level like in the async API. The sync API calls through to the async API internally
//! though, so it looks and behaves similarly to it.
//! ```no_run
//! # #[cfg(any(feature = "sync", feature = "tokio-sync"))]
//! # {
//! use mongodb::{
//!     bson::doc,
//!     sync::Client,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Book {
//!     title: String,
//!     author: String,
//! }
//! # fn wrapper() -> mongodb::error::Result<()> {
//! let client = Client::with_uri_str("mongodb://localhost:27017")?;
//! let database = client.database("mydb");
//! let collection = database.collection::<Book>("books");
//!
//! let docs = vec![
//!     Book {
//!         title: "1984".to_string(),
//!         author: "George Orwell".to_string(),
//!     },
//!     Book {
//!         title: "Animal Farm".to_string(),
//!         author: "George Orwell".to_string(),
//!     },
//!     Book {
//!         title: "The Great Gatsby".to_string(),
//!         author: "F. Scott Fitzgerald".to_string(),
//!     },
//! ];
//!
//! // Insert some books into the "mydb.books" collection.
//! collection.insert_many(docs, None)?;
//!
//! let cursor = collection.find(doc! { "author": "George Orwell" }, None)?;
//! for result in cursor {
//!     println!("title: {}", result?.title);
//! }
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! ## Warning about timeouts / cancellation
//!
//! In async Rust, it is common to implement cancellation and timeouts by dropping a future after a
//! certain period of time instead of polling it to completion. This is how
//! [`tokio::time::timeout`](https://docs.rs/tokio/1.10.1/tokio/time/fn.timeout.html) works, for
//! example. However, doing this with futures returned by the driver can leave the driver's internals in
//! an inconsistent state, which may lead to unpredictable or incorrect behavior (see RUST-937 for more
//! details). As such, it is **_highly_** recommended to poll all futures returned from the driver to
//! completion. In order to still use timeout mechanisms like `tokio::time::timeout` with the driver,
//! one option is to spawn tasks and time out on their
//! [`JoinHandle`](https://docs.rs/tokio/1.10.1/tokio/task/struct.JoinHandle.html) futures instead of on
//! the driver's futures directly. This will ensure the driver's futures will always be completely polled
//! while also allowing the application to continue in the event of a timeout.
//!
//! e.g.
//! ``` rust
//! # use std::time::Duration;
//! # use mongodb::{
//! #     Client,
//! #     bson::doc,
//! # };
//! #
//! # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync"), feature = "tokio-runtime"))]
//! # async fn foo() -> std::result::Result<(), Box<dyn std::error::Error>> {
//! #
//! # let client = Client::with_uri_str("mongodb://example.com").await?;
//! let collection = client.database("foo").collection("bar");
//! let handle = tokio::task::spawn(async move {
//!     collection.insert_one(doc! { "x": 1 }, None).await
//! });
//!
//! tokio::time::timeout(Duration::from_secs(5), handle).await???;
//! # Ok(())
//! # }
//! ```
//!
//! ## Minimum supported Rust version (MSRV)
//!
//! The MSRV for this crate is currently 1.61.0. This will be rarely be increased, and if it ever is,
//! it will only happen in a minor or major version release.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(clippy::cast_possible_truncation)]
#![warn(clippy::cast_possible_wrap)]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(
        clippy::unreadable_literal,
        clippy::cognitive_complexity,
        clippy::float_cmp,
        clippy::match_like_matches_macro,
        clippy::derive_partial_eq_without_eq
    )
)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(test, type_length_limit = "80000000")]
#![doc(html_root_url = "https://docs.rs/mongodb/2.8.2")]

#[cfg(all(feature = "aws-auth", feature = "async-std-runtime"))]
compile_error!("The `aws-auth` feature flag is only supported on the tokio runtime.");

#[macro_use]
pub mod options;

pub use ::bson;
#[cfg(feature = "in-use-encryption-unstable")]
pub use ::mongocrypt;

mod bson_util;
pub mod change_stream;
pub(crate) mod checked;
mod client;
mod cmap;
mod coll;
mod collation;
mod compression;
mod concern;
mod cursor;
mod db;
pub mod error;
pub mod event;
pub mod gridfs;
mod hello;
pub(crate) mod id_set;
mod index;
mod operation;
pub mod results;
pub(crate) mod runtime;
mod search_index;
mod sdam;
mod selection_criteria;
mod serde_util;
mod srv;
#[cfg(any(feature = "sync", feature = "tokio-sync"))]
pub mod sync;
#[cfg(test)]
mod test;
#[cfg(feature = "tracing-unstable")]
mod trace;
pub(crate) mod tracking_arc;

#[cfg(feature = "in-use-encryption-unstable")]
pub use crate::client::csfle::client_encryption;
pub use crate::{
    client::{session::ClientSession, Client},
    coll::Collection,
    cursor::{
        session::{SessionCursor, SessionCursorStream},
        Cursor,
    },
    db::Database,
    gridfs::{GridFsBucket, GridFsDownloadStream, GridFsUploadStream},
};

pub use {client::session::ClusterTime, coll::Namespace, index::IndexModel, sdam::public::*, search_index::SearchIndexModel};

#[cfg(all(feature = "tokio-runtime", feature = "sync",))]
compile_error!(
    "`tokio-runtime` and `sync` can't both be enabled; either switch to using `tokio-sync` or set \
     `default-features = false` in your Cargo.toml"
);

#[cfg(all(
    feature = "tokio-runtime",
    feature = "async-std-runtime",
    not(feature = "sync")
))]
compile_error!(
    "`tokio-runtime` and `async-std-runtime` can't both be enabled; either disable \
     `async-std-runtime` or set `default-features = false` in your Cargo.toml"
);

#[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
compile_error!(
    "one of `tokio-runtime`, `async-std-runtime`, `sync`, or `tokio-sync` must be enabled; \
     either enable `default-features`, or enable one of those features specifically in your \
     Cargo.toml"
);
