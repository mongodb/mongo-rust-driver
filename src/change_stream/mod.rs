pub mod document;
pub mod option;

use std::marker::PhantomData;

use bson::Document;
use serde::Deserialize;

use self::document::*;
use crate::{
    change_stream::option::ChangeStreamOptions,
    error::Result,
    options::AggregateOptions,
    selection_criteria::ReadPreference,
    Client,
    Collection,
    Cursor,
    Database,
};
use futures::{stream::Stream, task::Context};
use std::{pin::Pin, task::Poll};

/// A `ChangeStream` streams the ongoing changes of its associated collection, database or
/// deployment. `ChangeStream` instances should be created with method `watch` against the relevant
/// target. Issuing a raw change stream aggregation is discouraged unless users wish to explicitly
/// opt out of resumability.
///
/// A `ChangeStream` can be iterated to return batches of instances of any type that implements the
/// Deserialize trait. By default, it returns batches of `ChangeStreamEventDocument`. These
/// documents correspond to changes in the associated collection, database or deployment.
///
/// A change stream can be iterated like any other `Iterator`:
///
/// ```rust
// / # use mongodb::{Client, error::Result};
/// #
/// # fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// # let coll = client.database("foo").collection("bar");
/// let change_stream = coll.watch(None, None)?;
///
/// for change in change_stream {
///   println!("{:?}", change?)
/// }
/// #
/// # Ok(())
/// # }
/// ```
/// 
/// See the documentation [here](https://docs.mongodb.com/manual/changeStreams) for more
/// details. Also see the documentation on [usage recommendations](https://docs.mongodb.com/manual/administration/change-streams-production-recommendations/).
pub struct ChangeStream<'a, T: Deserialize<'a> = ChangeStreamEventDocument> {
    /// The cursor to iterate over `ChangeStreamEventDocument` instances.
    cursor: Cursor,

    /// The pipeline of stages to append to an initial `$changeStream` stage.
    pipeline: Vec<Document>,

    /// The client that was used for the initial `$changeStream` aggregation, used for server
    /// selection during an automatic resume.
    client: Client,

    /// The original target of the change stream, used for re-issuing the aggregation during
    /// an automatic resume.
    target: ChangeStreamTarget,

    /// The cached resume token.
    resume_token: Option<ResumeToken>,

    /// The options provided to the initial `$changeStream` stage.
    options: Option<ChangeStreamOptions>,

    /// The read preference for the initial `$changeStream` aggregation, used for server selection
    /// during an automatic resume.
    read_preference: Option<ReadPreference>,

    /// Whether or not the change stream has attempted a resume, used to attempt a resume only
    /// once.
    resume_attempted: bool,

    /// Whether or not the change stream has returned a document, used to update resume token
    /// during an automatic resume.
    document_returned: bool,

    /// Necessary to satisfy the compiler that `T` and `'a` are used in the struct definition.
    ///
    /// See the [Rust docs](https://doc.rust-lang.org/std/marker/struct.PhantomData.html) for more details.
    phantom: PhantomData<&'a T>,
}

impl<'a, T> ChangeStream<'a, T>
where
    T: Deserialize<'a>,
{
    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        todo!();
    }

    fn resume(&mut self) -> Result<()> {
        todo!();
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ChangeStreamTarget {
    Collection(Collection),
    Database(Database),
    Cluster(Database),
}

impl ChangeStreamTarget {
    fn aggregate(
        &self,
        pipeline: Vec<Document>,
        options: Option<AggregateOptions>,
    ) -> Result<Cursor> {
        todo!();
    }
}

impl<'a, T> Stream for ChangeStream<'a, T>
where
    T: Deserialize<'a>,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!();
    }
}
