pub mod document;
pub mod options;

use bson::Document;

use self::document::*;
use self::options::*;
use crate::{error::Result, read_preference::ReadPreference, Cursor};

/// A `ChangeStream` streams the ongoing changes of its associated collection,
/// database or deployment. `ChangeStream` instances should be created with
/// method `watch` against the relevant target. Issuing a raw change stream
/// aggregation is discouraged unless users wish to explicitly opt out of
/// resumability.
///
/// A `ChangeStream` can be iterated to return batches of instances of
/// `ChangeStreamDocument`. These documents correspond to changes in the
/// associated collection, database or deployment. A change stream can be
/// iterated like any other `Iterator`:
///
/// ```rust
/// # use mongodb::{Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// # let change_stream = coll.watch(None, None)?;
/// #
/// for change in change_stream {
///   println!("{}", change?)
/// }
/// #
/// # Ok(())
/// # }
/// ```
///
/// See the documentation [here](https://docs.mongodb.com/manual/changeStreams/index.html) for more
/// details. Also see the documentation on [usage recommendations](https://docs.mongodb.com/manual/administration/change-streams-production-recommendations/).
pub struct ChangeStream {
    /// The cursor to iterate over `ChangeStreamDocument` instances
    cursor: Cursor,

    /// The cached resume token
    resume_token: Option<ChangeStreamToken>,

    /// The pipeline of stages to append to an initial `$changeStream` stage
    pipeline: Vec<Document>,

    /// The options provided to the initial `$changeStream` stage
    options: Option<ChangeStreamOptions>,

    /// The read preference for the initial `$changeStream` aggregation, used
    /// for server selection during an automatic resume.
    read_preference: Option<ReadPreference>,
}

impl ChangeStream {
    /// Returns the cached resume token that will be used to resume after the
    /// most recently returned change.
    pub(crate) fn resume_token(&self) -> Option<ChangeStreamToken> {
        self.resume_token.clone()
    }

    /// Tail the change stream.
    pub fn tail(&mut self) -> ChangeStreamTail {
        ChangeStreamTail {
            change_stream: self,
        }
    }

    /// Attempt to resume the change stream.
    fn resume(&mut self) -> Result<()> {
        unimplemented!();
    }
}

impl Iterator for ChangeStream {
    type Item = Result<ChangeStreamDocument>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!();
    }
}

/// A `ChangeStreamTail` is a temporary `Iterator` created from
/// `ChangeStream::tail` to facilitate using methods that require ownership
/// of `self` (such as `map` and `fold`) while still being able to use the
/// `ChangeStream` afterwards.
///
/// Similar to a `Tail` for a `Cursor`, the only way to create a `ChangeStreamTail` is
/// with `ChangeStream::tail`. See the `Cursor` type documentation for more details on
/// how to use a tail.
pub struct ChangeStreamTail<'a> {
    change_stream: &'a mut ChangeStream,
}

impl<'a> Iterator for ChangeStreamTail<'a> {
    type Item = Result<ChangeStreamDocument>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!();
    }
}
