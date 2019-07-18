pub mod document;
pub mod options;

use std::marker::PhantomData;

use bson::{Bson, Document};
use serde::Deserialize;

use self::document::*;
use self::options::*;
use crate::{
    error::{Error, ErrorKind, Result},
    read_preference::ReadPreference,
    Cursor,
};

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
pub struct ChangeStream<'a, T: Deserialize<'a>> {
    /// The cursor to iterate over `ChangeStreamDocument` instances
    cursor: Cursor,

    /// The pipeline of stages to append to an initial `$changeStream` stage
    pipeline: Vec<Document>,

    /// The cached resume token
    resume_token: Option<ChangeStreamToken>,

    /// The options provided to the initial `$changeStream` stage
    options: Option<ChangeStreamOptions>,

    /// The read preference for the initial `$changeStream` aggregation, used
    /// for server selection during an automatic resume.
    read_preference: Option<ReadPreference>,

    phantom: PhantomData<&'a T>,
}

impl<'a, T> ChangeStream<'a, T>
where
    T: Deserialize<'a>,
{
    /// Creates a new ChangeStream instance
    pub(crate) fn new(
        cursor: Cursor,
        pipeline: Vec<Document>,
        resume_token: Option<ChangeStreamToken>,
        options: Option<ChangeStreamOptions>,
        read_preference: Option<ReadPreference>,
    ) -> Self {
        Self {
            cursor,
            pipeline,
            resume_token,
            options,
            read_preference,
            phantom: PhantomData,
        }
    }

    /// Returns the cached resume token that will be used to resume after the
    /// most recently returned change.
    pub fn resume_token(&self) -> Option<ChangeStreamToken> {
        self.resume_token.clone()
    }

    /// Tail the change stream.
    pub fn tail(&'a mut self) -> ChangeStreamTail<'a, T> {
        ChangeStreamTail {
            change_stream: self,
        }
    }

    fn try_resume(&mut self, error: Error) -> Option<Result<T>> {
        unimplemented!();
    }

    fn resume(&mut self) -> Result<()> {
        // TODO: perform server selection and connect to selected server

        // let mut new_options = self.options.clone();
        // let start_at_operation_time = match new_options {
        // Some(new_options) => new_options.start_at_operation_time,
        // None => None,
        // };
        // // TODO: find actual max_wire_version
        // let max_wire_version = 7;

        // if self.resume_token().is_some() {
        // if let Some(new_options) = new_options {
        // new_options.resume_after = self.resume_token();
        // new_options.start_after = None;
        // new_options.start_at_operation_time = None;
        // } else {
        // new_options = Some(
        // ChangeStreamOptions::builder()
        // .resume_after(self.resume_token())
        // .build(),
        // );
        // }
        // } else if self.resume_token().is_none()
        // && start_at_operation_time.is_some()
        // && max_wire_version >= 7
        // {
        // if let Some(new_options) = new_options {
        // new_options.start_at_operation_time = start_at_operation_time;
        // }
        // }

        // run aggregate command against original target

        Ok(())
    }
}

impl<'a, T> Iterator for ChangeStream<'a, T>
where
    T: Deserialize<'a>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.next() {
            Some(Ok(doc)) => Some(match bson::from_bson(Bson::Document(doc)) {
                Ok(result) => Ok(result),
                Err(_) => Err(Error::from_kind(ErrorKind::ResponseError(
                    "invalid server response to change stream getMore".to_string(),
                ))),
            }),
            Some(Err(e)) => self.try_resume(e),
            None => None,
        }
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
pub struct ChangeStreamTail<'a, T: Deserialize<'a>> {
    change_stream: &'a mut ChangeStream<'a, T>,
}

impl<'a, T> Iterator for ChangeStreamTail<'a, T>
where
    T: Deserialize<'a>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.change_stream.next()
    }
}
