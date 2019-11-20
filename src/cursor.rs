use std::{collections::VecDeque, time::Duration};

use bson::Document;

use crate::{error::Result, operation::GetMore, options::StreamAddress, Client, Namespace};

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
/// # use bson::Document;
/// # use mongodb::{Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// # let cursor = coll.find(Document::new(), None)?;
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
/// # let cursor = coll.find(doc! { "x": 1 }, None)?;
/// #
/// let results: Vec<Result<Document>> = cursor.collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Cursor {
    client: Client,
    buffer: VecDeque<Document>,
    get_more: GetMore,
    exhausted: bool,
}

impl Cursor {
    pub(crate) fn new(client: Client, spec: CursorSpecification) -> Self {
        let get_more = GetMore::new(
            spec.ns,
            spec.id,
            spec.address,
            spec.batch_size,
            spec.max_time,
        );

        Self {
            client,
            buffer: spec.buffer,
            get_more,
            exhausted: spec.id == 0,
        }
    }
}

impl Iterator for Cursor {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() && !self.exhausted {
            match self.client.execute_operation(&self.get_more, None) {
                Ok(get_more_result) => {
                    self.buffer.extend(get_more_result.batch);
                    self.exhausted = get_more_result.exhausted;
                }
                Err(e) => return Some(Err(e)),
            };
        }
        self.buffer.pop_front().map(Ok)
    }
}

#[derive(Debug)]
pub(crate) struct CursorSpecification {
    pub(crate) ns: Namespace,
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
    pub(crate) buffer: VecDeque<Document>,
}
