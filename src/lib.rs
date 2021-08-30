//! This crate is a pure Rust MongoDB driver. It follows the
//! [MongoDB driver API and feature specifications](https://github.com/mongodb/specifications).
//!
//! To connect to a MongoDB database, pass a MongoDB connection string to
//! [`Client::with_uri_str`](struct.Client.html#method.with_uri_str):
//!
//! ```rust
//! # #[cfg(not(feature = "sync"))]
//! # use mongodb::{Client, error::Result};
//! #
//! # #[cfg(not(feature = "sync"))]
//! # async fn make_client() -> Result<Client> {
//! let client = Client::with_uri_str("mongodb://localhost:27017/").await?;
//! # Ok(client)
//! # }
//! ```
//! Alternately, create an instance of [`ClientOptions`](options/struct.ClientOptions.html) and pass
//! it to [`Client::with_options`](struct.Client.html#method.with_options):
//!
//! ```rust
//! # use mongodb::{
//! #     error::Result,
//! #     options::{ServerAddress, ClientOptions},
//! # };
//! # #[cfg(feature = "sync")]
//! # use mongodb::sync::Client;
//! # #[cfg(not(feature = "sync"))]
//! # use mongodb::Client;
//! #
//! # fn make_client() -> Result<Client> {
//! let options = ClientOptions::builder()
//!                   .hosts(vec![
//!                       ServerAddress::Tcp {
//!                           host: "localhost".into(),
//!                           port: Some(27017),
//!                       }
//!                   ])
//!                   .build();
//!
//! let client = Client::with_options(options)?;
//! # Ok(client)
//! # }
//! ```
//!
//! Operations can be performed by obtaining a [`Database`](struct.Database.html) or
//! [`Collection`](struct.Collection.html) from the [`Client`](struct.Client.html):
//!
//! ```rust
//! # use mongodb::{
//! #   bson::doc,
//! #   error::Result,
//! # };
//! # #[cfg(not(feature = "sync"))]
//! # use mongodb::Client;
//! #
//! # #[cfg(not(feature = "sync"))]
//! # async fn do_stuff() -> Result<()> {
//! # let client = Client::with_uri_str("mongodb://example.com").await?;
//! #
//! let db = client.database("some_db");
//! for coll_name in db.list_collection_names(None).await? {
//!     println!("collection: {}", coll_name);
//! }
//!
//! let coll = db.collection("some-coll");
//! let result = coll.insert_one(doc! { "x": 1 }, None).await?;
//! println!("{:#?}", result);
//!
//! # Ok(())
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

#![warn(missing_docs)]
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
#![doc(html_root_url = "https://docs.rs/mongodb/2.0.0-beta.3")]

#[cfg(all(
    feature = "aws-auth",
    feature = "async-std-runtime"
))]
compile_error!(
    "The `aws-auth` feature flag is only supported on the tokio runtime."
);

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
        index::IndexModel,
    };

    #[cfg(feature = "sync")]
    pub(crate) use crate::{
        client::{Client, session::ClientSession},
        coll::Collection,
        cursor::{Cursor, session::{SessionCursor, SessionCursorStream}},
        db::Database,
    };

    pub use coll::Namespace;
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
