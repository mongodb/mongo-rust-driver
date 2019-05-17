//! This crate is a pure Rust MongoDB driver. It follows the
//! [MongoDB driver API and feature specifications](https://github.com/mongodb/specifications).
//!
//! To connect to a MongoDB database, pass a MongoDB connection string to `Client::connect`:
//!
//! ```rust
//! # use mongodb::{Client, error::Result};
//! #
//! # fn make_client() -> Result<Client> {
//! let client = Client::with_uri("mongodb://localhost:27017/")?;
//! # Ok(client)
//! # }
//! ```
//!
//! Operations can be performed by obtaining a `Database` or `Collection` from the `Client`:
//!
//! ```rust
//! # use mongodb::{Client, error::Result};
//! #
//! # fn do_stuff() -> Result<()> {
//! # let client = Client::with_uri("mongodb://localhost:27017")?;
//!
//! let db = client.database("some_db");
//! for coll_name in db.list_collection_names(None)? {
//!     println!("collection: {}", coll_name);
//! }
//!
//! let coll = db.collection("some-coll");
//! if let Some(doc) = coll.find_one(None, None)? {
//!     println!("document: {}", doc);
//! }
//! # Ok(())
//! # }
//! ```

#![allow(unused_variables)]

#[macro_use]
extern crate typed_builder;

mod client;
mod coll;
pub mod concern;
mod cursor;
mod db;
pub mod error;
pub mod options;
mod read_preference;

pub use crate::{client::Client, coll::Collection, db::Database};
