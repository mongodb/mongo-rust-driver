use std::collections::VecDeque;

use bson::Document;

use crate::{
    command_responses::{FindCommandResponse, FindCommandResponseInner},
    error::Result,
    Collection,
};

/// A `Cursor` streams the result of a query. When a query is made, a `Cursor` will be returned with
/// the first batch of results from the server; the documents will be returned as the `Cursor` is
/// iterated. When the batch is exhausted and if there are more results, the `Cursor` will fetch the
/// next batch of documents, and so forth until the results are exhausted. Note that because of this
/// batching, additional network I/O may occur on any given call to `Cursor::next`. Because of this,
/// a `Cursor` iterates over `Result<Document>` items rather than simply `Document` items.
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
/// # let client = Client::with_uri_str("mongodb://example.com")?;
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
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// # let cursor = coll.find(Some(doc! { "x": 1 }), None)?;
/// #
/// let results: Vec<Result<Document>> = cursor.collect();
/// # Ok(())
/// # }
/// ```
#[allow(dead_code)]
pub struct Cursor {
    coll: Collection,
    address: String,
    cursor_id: i64,
    batch_size: Option<i32>,
    buffer: VecDeque<Document>,
}

pub struct Tail<'a> {
    cursor: &'a mut Cursor,
}

impl Cursor {
    pub(crate) fn new(
        address: String,
        coll: Collection,
        reply: FindCommandResponse,
        batch_size: Option<i32>,
    ) -> Self {
        Self {
            address,
            coll,
            cursor_id: reply.cursor.id,
            batch_size,
            buffer: reply.cursor.first_batch.into_iter().collect(),
        }
    }

    pub(crate) fn empty(address: String, coll: Collection) -> Self {
        Self::new(
            address,
            coll,
            FindCommandResponse {
                cursor: FindCommandResponseInner {
                    first_batch: Vec::new(),
                    id: 0,
                },
            },
            None,
        )
    }

    fn next_batch(&mut self) -> Result<()> {
        let result = self
            .coll
            .get_more_command(&self.address, self.cursor_id, self.batch_size)?;
        self.cursor_id = result.cursor.id;
        self.buffer.extend(result.cursor.next_batch.into_iter());

        Ok(())
    }

    pub fn tail(&mut self) -> Tail {
        Tail { cursor: self }
    }
}

impl Iterator for Cursor {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return the next document from the current batch if one is available.
        //
        // Otherwise, if the cursor ID is 0, then the server has closed the cursor, so there are no
        // more results.
        match self.buffer.pop_front() {
            Some(doc) => return Some(Ok(doc)),
            None if self.cursor_id == 0 => return None,
            None => {}
        };

        // Fetch the next batch, returning and error if one returns.
        if let Err(err) = self.next_batch() {
            return Some(Err(err));
        }

        // Return the first document from the next batch if it's available.
        self.buffer.pop_front().map(Ok)
    }
}

impl<'a> Iterator for Tail<'a> {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next()
    }
}
