//! This crate is a pure Rust MongoDB driver. It follows the
//! [MongoDB driver API and feature specifications](https://github.com/mongodb/specifications).
//!
//! To connect to a MongoDB database, pass a MongoDB connection string to `Client::connect`:
//!
//! ```rust
//! let client = Client::connect("mongodb://localhost:27017/")?;
//! ```
//!
//! Operations can be performed by obtaining a `Database` or `Collection` from the `Client`:
//!
//! ```rust
//! let db = client.database("some_db");
//! for coll_name in db.list_collection_names(None)? {
//!     println!("collection: {}", coll_name);
//! }
//!
//! let coll = db.collection("some-coll");
//! if let Some(doc) = coll.find_one(None, None)? {
//!     println!("document: {}", doc);
//! }
//! ```

#![allow(unused_variables)]

mod client;
mod concern;
mod db;
pub mod error;
pub mod options;
mod read_preference;

pub use crate::{client::Client, db::Database};
