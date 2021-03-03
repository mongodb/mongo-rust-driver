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
//! #     options::{StreamAddress, ClientOptions},
//! # };
//! # #[cfg(feature = "sync")]
//! # use mongodb::sync::Client;
//! # #[cfg(not(feature = "sync"))]
//! # use mongodb::Client;
//! #
//! # fn make_client() -> Result<Client> {
//! let options = ClientOptions::builder()
//!                   .hosts(vec![
//!                       StreamAddress {
//!                           hostname: "localhost".into(),
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
        client::Client,
        coll::Collection,
        cursor::{Cursor, session::{SessionCursor, SessionCursorHandle}},
        db::Database,
    };

    #[cfg(feature = "sync")]
    pub(crate) use crate::{
        client::Client,
        coll::Collection,
        cursor::{Cursor, session::{SessionCursor, SessionCursorHandle}},
        db::Database,
    };

    pub use client::session::ClientSession;

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
