//! This crate contains the officially supported MongoDB Rust driver, a
//! client side library that can be used to interact with MongoDB deployments
//! in Rust applications. It uses the [`bson`](docs.rs/bson) crate for BSON support.
//! The driver contains a fully async API that supports either [`tokio`](docs.rs/tokio) (default)
//! or [`async-std`](docs.rs/async-std), depending on the feature flags set. The driver also has
//! a sync API that may be enabled via the `"sync"` feature flag.
//!
//! # Installation
//!
//! ## Requirements
//! - Rust 1.48+
//! - MongoDB 3.6+
//!
//! ## Importing
//! The driver is available on [crates.io](https://crates.io/crates/mongodb). To use the driver in
//! your application, simply add it to your project's `Cargo.toml`.
//! ```toml
//! [dependencies]
//! mongodb = "2.0.0"
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
//! version = "2.0.0"
//! default-features = false
//! features = ["async-std-runtime"]
//! ```
//!
//! ### Enabling the sync API
//! The driver also provides a blocking sync API. To enable this, add the `"sync"` feature to your
//! `Cargo.toml`:
//! ```toml
//! [dependencies.mongodb]
//! version = "2.0.0"
//! default-features = false
//! features = ["sync"]
//! ```
//! **Note:** if the sync API is enabled, the async-specific types will be privatized (e.g.
//! `mongodb::Client`). The sync-specific types can be imported from `mongodb::sync` (e.g.
//! `mongodb::sync::Client`).
//!
//! ### All Feature flags
//!
//! | Feature             | Description                                                                                                                           | Extra dependencies                  | Default |
//! |:--------------------|:--------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------|:--------|
//! | `tokio-runtime`     | Enable support for the `tokio` async runtime                                                                                          | `tokio` 1.0 with the `full` feature | yes     |
//! | `async-std-runtime` | Enable support for the `async-std` runtime                                                                                            | `async-std` 1.0                     | no      |
//! | `sync`              | Expose the synchronous API (`mongodb::sync`). This flag cannot be used in conjunction with either of the async runtime feature flags. | `async-std` 1.0                     | no      |
//! | `aws-auth`          | Enable support for the MONGODB-AWS authentication mechanism.                                                                          | `reqwest` 0.11                      | no      |
//! | `bson-uuid-0_8`     | Enable support for v0.8 of the [`uuid`](docs.rs/uuid/0.8) crate in the public API of the re-exported `bson` crate.                    | n/a                                 | no      |
//! | `bson-chrono-0_4`   | Enable support for v0.4 of the [`chrono`](docs.rs/chrono/0.4) crate in the public API of the re-exported `bson` crate.                | n/a                                 | no      |
//!
//! # Example Usage
//!
//! ## Using the async API
//! ### Connecting to a MongoDB deployment
//! ```no_run
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
//! # #[cfg(feature = "sync")]
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
//!
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
//! # #[cfg(all(not(feature = "sync"), feature = "tokio-runtime"))]
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
//! The MSRV for this crate is currently 1.48.0. This will be rarely be increased, and if it ever is,
//! it will only happen in a minor or major version release.

#![warn(missing_docs)]
#![allow(renamed_and_removed_lints)]  // `missing_crate_level_docs` was renamed with a `rustdoc::` prefix
#![warn(missing_crate_level_docs)]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(
        clippy::unreadable_literal,
        clippy::cognitive_complexity,
        clippy::float_cmp
    )
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(test, type_length_limit = "80000000")]
#![doc(html_root_url = "https://docs.rs/mongodb/2.0.0")]

#[cfg(all(feature = "aws-auth", feature = "async-std-runtime"))]
compile_error!("The `aws-auth` feature flag is only supported on the tokio runtime.");

macro_rules! define_if_single_runtime_enabled {
    ( $( $def:item )+ ) => {
        $(
            #[cfg(any(
                all(feature = "tokio-runtime", not(feature = "async-std-runtime")),
                all(not(feature = "tokio-runtime"), feature = "async-std-runtime")
            ))]
            $def
        )+
    }
}

// In the case that neither tokio nor async-std is enabled, we want to disable all compiler errors
// and warnings other than our custom ones.
#[warn(renamed_and_removed_lints)]
define_if_single_runtime_enabled! {
    #[macro_use]
    pub mod options;

    pub use ::bson;

    mod bson_util;
    mod client;
    mod cmap;
    mod coll;
    mod collation;
    mod concern;
    mod cursor;
    mod db;
    pub mod error;
    pub mod event;
    mod index;
    mod is_master;
    mod operation;
    pub mod results;
    pub(crate) mod runtime;
    mod sdam;
    mod selection_criteria;
    mod srv;
    #[cfg(any(feature = "sync", docsrs))]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    pub mod sync;
    #[cfg(test)]
    mod test;

    #[cfg(test)]
    #[macro_use]
    extern crate derive_more;

    #[cfg(not(feature = "sync"))]
    pub use crate::{
        client::{Client, session::ClientSession},
        coll::Collection,
        cursor::{Cursor, session::{SessionCursor, SessionCursorStream}},
        db::Database,
    };

    #[cfg(feature = "sync")]
    pub(crate) use crate::{
        client::{Client, session::ClientSession},
        coll::Collection,
        cursor::{Cursor, session::{SessionCursor, SessionCursorStream}},
        db::Database,
    };

    pub use {coll::Namespace, index::IndexModel, client::session::ClusterTime};
}

#[cfg(all(
    feature = "tokio-runtime",
    feature = "async-std-runtime",
    not(feature = "sync")
))]
compile_error!(
    "`tokio-runtime` and `async-std-runtime` can't both be enabled; either disable \
     `async-std-runtime` or set `default-features = false` in your Cargo.toml"
);

#[cfg(all(feature = "tokio-runtime", feature = "sync"))]
compile_error!(
    "`tokio-runtime` and `sync` can't both be enabled; either disable `sync` or set \
     `default-features = false` in your Cargo.toml"
);

#[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
compile_error!(
    "one of `tokio-runtime`, `async-std-runtime`, or `sync` must be enabled; either enable \
     `default-features`, or enable one of those features specifically in your Cargo.toml"
);

#[cfg(all(feature = "tokio-runtime", not(feature = "async-std-runtime")))]
pub(crate) static RUNTIME: runtime::AsyncRuntime = runtime::AsyncRuntime::Tokio;

#[cfg(all(not(feature = "tokio-runtime"), feature = "async-std-runtime"))]
pub(crate) static RUNTIME: runtime::AsyncRuntime = runtime::AsyncRuntime::AsyncStd;
