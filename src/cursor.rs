use std::collections::VecDeque;

use bson::Document;

use crate::{error::Result, Collection};

/// A `Cursor` streams the result of a query. When a query is made, a `Cursor` will be returned with
/// the first batch of results from the server; the documents will be returned as the `Cursor` is
/// iterated. When the batch is exhausted and if there are more results, the `Cursor` will fetch the
/// next batch of documents, and so forth until the results are exhausted.
///
/// The batch size of the `Cursor` can be configured using the options to the method that returns
/// it. For example, setting the `batch_size` field of `FindOptions` will set the batch size of the
/// `Cursor` returned by `Collection::find`.
///
/// Note that the batch size determines both the number of documents stored in memory by the
/// `Cursor` at a given time as well as the total number of network round-trips needed to fetch all
/// results from the server; both of these factors should be taken into account when choosing the
/// optimal batch size.
///
/// A cursor can be used like any other `Iterator`. The simplest way is just to iterate over the
/// documents it yields:
///
/// ```rust
/// # use mongodb::{Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// # let cursor = coll.find(None, None)?;
/// #
/// for doc in cursor {
///   println!("{}", doc?)
/// }
/// #
/// # Ok(())
/// # }
/// ```
///
/// Additionally, all the other methods that an `Iterator` has are available on `Cursor` as well.
/// For instance, if the number of results from a query is known to be small, it might make sense
/// to collect them into a vector:
///
/// ```rust
/// # use bson::{doc, bson, Document};
/// # use mongodb::{Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// # let cursor = coll.find(Some(doc! { "x": 1 }), None)?;
/// #
/// let results: Vec<Result<Document>> = cursor.collect();
/// # Ok(())
/// # }
/// ```
#[allow(dead_code)]
pub struct Cursor {
    address: String,
    coll: Collection,
    cursor_id: i64,
    batch_size: Option<i32>,
    buffer: VecDeque<Document>,
}

impl Iterator for Cursor {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
