# Reading From the Database

## Database and Collection Handles

Once you have a `Client`, you can call [`Client::database`](https://docs.rs/mongodb/latest/mongodb/struct.Client.html#method.database) to create a handle to a particular database on the server, and [`Database::collection`](https://docs.rs/mongodb/latest/mongodb/struct.Database.html#method.collection) to create a handle to a particular collection in that database.  [`Database`](https://docs.rs/mongodb/latest/mongodb/struct.Database.html) and [`Collection`](https://docs.rs/mongodb/latest/mongodb/struct.Collection.html) handles are lightweight - creating them requires no IO, `clone`ing them is cheap, and they can be safely shared across threads or async tasks.  For example:
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# use mongodb::{bson::Document, Client, error::Result};
# use tokio::task;
#
# async fn start_workers() -> Result<()> {
# let client = Client::with_uri_str("mongodb://example.com").await?;
let db = client.database("items");

for i in 0..5 {
    let db_ref = db.clone();

    task::spawn(async move {
        let collection = db_ref.collection::<Document>(&format!("coll{}", i));

        // Do something with the collection
    });
}
#
# Ok(())
# }
```

A `Collection` can be parameterized with a type for the documents in the collection; this includes but is not limited to just `Document`.  The various methods that accept instances of the documents (e.g. [`Collection::insert_one`](https://docs.rs/mongodb/latest/mongodb/struct.Collection.html#method.insert_one)) require that it implement the `Serialize` trait from the [`serde`](http://serde.rs/) crate.  Similarly, the methods that return instances (e.g. [`Collection::find_one`](https://docs.rs/mongodb/latest/mongodb/struct.Collection.html#method.find_one)) require that it implement `Deserialize`.

`Document` implements both and can always be used as the type parameter.  However, it is recommended to define types that model your data which you can parameterize your `Collection`s with instead, since doing so eliminates a lot of boilerplate deserialization code and is often more performant.

```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate serde;
# use mongodb::{
#     bson::doc,
#     error::Result,
# };
# use tokio::task;
#
# async fn start_workers() -> Result<()> {
# use mongodb::Client;
#
# let client = Client::with_uri_str("mongodb://example.com").await?;
use serde::{Deserialize, Serialize};

// Define a type that models our data.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Item {
    id: u32,
}

// Parameterize our collection with the model.
let coll = client.database("items").collection::<Item>("in_stock");

for i in 0..5 {
    // Perform operations that work with directly our model.
    coll.insert_one(Item { id: i }).await;
}
#
# Ok(())
# }
```

For more information, see the [Serde Integration](serde_integration.md) section.

## Cursors

Results from queries are generally returned via [`Cursor`](https://docs.rs/mongodb/latest/mongodb/struct.Cursor.html), a struct which streams the results back from the server as requested. The `Cursor` type implements the [`Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) trait from the [`futures`](https://crates.io/crates/futures) crate, and in order to access its streaming functionality you need to import at least one of the [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html) or [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html) traits.

```toml
# In Cargo.toml, add the following dependency.
futures = "0.3"
```
```rust,no_run
# extern crate mongodb;
# extern crate serde;
# extern crate futures;
# use serde::Deserialize;
# #[derive(Deserialize)]
# struct Book { title: String }
# async fn foo() -> mongodb::error::Result<()> {
# let typed_collection = mongodb::Client::with_uri_str("").await?.database("").collection::<Book>("");
// This trait is required to use `try_next()` on the cursor
use futures::stream::TryStreamExt;
use mongodb::{bson::doc, options::FindOptions};

// Query the books in the collection with a filter and an option.
let mut cursor = typed_collection
    .find(doc! { "author": "George Orwell" })
    .sort(doc! { "title": 1 })
    .await?;

// Iterate over the results of the cursor.
while let Some(book) = cursor.try_next().await? {
    println!("title: {}", book.title);
}
# Ok(()) }
```

If a [`Cursor`](https://docs.rs/mongodb/latest/mongodb/struct.Cursor.html) is still open when it goes out of scope, it will automatically be closed via an asynchronous [killCursors](https://www.mongodb.com/docs/manual/reference/command/killCursors/) command executed from its [`Drop`](https://doc.rust-lang.org/std/ops/trait.Drop.html) implementation.
