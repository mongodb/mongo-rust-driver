pub mod document;
pub mod options;
#[cfg(test)]
mod test;

use std::marker::PhantomData;

use bson::{Bson, Document};
use serde::Deserialize;

use self::{document::*, options::*};
use crate::{
    error::{Error, ErrorKind, Result},
    options::AggregateOptions,
    read_preference::ReadPreference,
    Client, Collection, Cursor, Database,
};

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
/// # use mongodb::{Client, error::Result};
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
/// See the documentation [here](https://docs.mongodb.com/manual/changeStreams/index.html) for more
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

    phantom: PhantomData<&'a T>,
}

impl<'a, T> ChangeStream<'a, T>
where
    T: Deserialize<'a>,
{
    pub(crate) fn new(
        cursor: Cursor,
        pipeline: Vec<Document>,
        client: Client,
        target: ChangeStreamTarget,
        resume_token: Option<ResumeToken>,
        options: Option<ChangeStreamOptions>,
        read_preference: Option<ReadPreference>,
    ) -> Self {
        Self {
            cursor,
            pipeline,
            client,
            target,
            resume_token,
            options,
            read_preference,
            resume_attempted: false,
            document_returned: false,
            phantom: PhantomData,
        }
    }

    /// Returns the cached resume token that can be used to resume after the most recently returned
    /// change.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
    /// information on change stream resume tokens.
    pub fn resume_token(&self) -> Option<ResumeToken> {
        self.resume_token.clone()
    }

    /// Tail the change stream.
    pub fn tail(&'a mut self) -> ChangeStreamTail<'a, T> {
        ChangeStreamTail {
            change_stream: self,
        }
    }

    fn is_resumable(&self, operation: &str, msg: &str) -> bool {
        !(operation == "getMore"
            && (msg.contains("Interrupted")
                || msg.contains("CappedPositionLost")
                || msg.contains("CursorKilled")
                || msg.contains("NonResumableChangeStreamError")))
    }

    fn make_pipeline(&self) -> Result<Vec<Document>> {
        let mut watch_pipeline = Vec::new();

        if let Some(ref options) = self.options {
            match self.target {
                ChangeStreamTarget::Collection(_) | ChangeStreamTarget::Database(_) => {
                    watch_pipeline.push(doc! { "$changeStream": bson::to_bson(&options)? })
                }
                ChangeStreamTarget::Cluster(_) => {
                    let options_bson = bson::to_bson(&options)?;
                    if let bson::Bson::Document(mut options_doc) = options_bson {
                        options_doc.insert("allChangesForCluster", true);
                        watch_pipeline.push(doc! { "$changeStream": options_doc });
                    } else {
                        // TODO: Throw the correct error here (options cannot be parsed as Document)
                        unreachable!();
                    }
                }
            }
        } else {
            match self.target {
                ChangeStreamTarget::Collection(_) | ChangeStreamTarget::Database(_) => {
                    watch_pipeline.push(doc! { "$changeStream": {} });
                }
                ChangeStreamTarget::Cluster(_) => {
                    watch_pipeline.push(doc! { "$changeStream": { "allChangesForCluster": true } });
                }
            }
        }

        watch_pipeline.extend(self.pipeline.clone());
        Ok(watch_pipeline)
    }

    fn try_resume(&mut self, error: Error) -> Result<()> {
        self.resume_attempted = true;

        match error.kind() {
            ErrorKind::ServerError(ref operation, ref msg) => {
                if self.is_resumable(operation, msg) {
                    self.resume()
                } else {
                    Err(error)
                }
            }
            _ => self.resume(),
        }
    }

    fn resume(&mut self) -> Result<()> {
        let (address, _) = self
            .client
            .acquire_stream(self.read_preference.as_ref(), None)?;

        let max_wire_version = match self.client.get_max_wire_version(&address) {
            Some(v) => v,
            None => bail!(ErrorKind::ServerSelectionError(
                "selected server removed from cluster before change stream resume could occur"
                    .into(),
            )),
        };

        let resume_token = self.resume_token();
        let op_time = self.cursor.operation_time().and_then(|b| b.as_timestamp());
        let options = self.options.get_or_insert_with(Default::default);
        let aggregate_options = AggregateOptions::builder()
            .collation(options.collation.clone())
            .build();

        if resume_token.is_some() {
            if options.start_after.is_some() && !self.document_returned {
                options.start_after = resume_token;
                options.start_at_operation_time = None;
            } else {
                options.resume_after = resume_token;
                options.start_after = None;
                options.start_at_operation_time = None;
            }
        } else if options.start_at_operation_time.is_some() && max_wire_version >= 7 {
            options.start_at_operation_time = options.start_at_operation_time.or(op_time);
        }

        self.cursor = self
            .target
            .aggregate(self.make_pipeline()?, Some(aggregate_options))?;

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
                Ok(result) => {
                    self.resume_attempted = false;
                    self.document_returned = true;
                    Ok(result)
                }
                Err(_) => Err(Error::from_kind(ErrorKind::ResponseError(
                    "invalid server response to change stream getMore".to_string(),
                ))),
            }),
            Some(Err(e)) => {
                if !self.resume_attempted {
                    match self.try_resume(e) {
                        Ok(_) => self.next(),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    Some(Err(e))
                }
            }
            None => None,
        }
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
        Ok(match self {
            ChangeStreamTarget::Collection(coll) => coll.aggregate(pipeline, options)?,
            ChangeStreamTarget::Database(db) => db.aggregate(pipeline, options)?,
            ChangeStreamTarget::Cluster(db) => db.aggregate(pipeline, options)?,
        })
    }
}

/// A `ChangeStreamTail` is a temporary `Iterator` created from `ChangeStream::tail` to facilitate
/// using methods that require ownership of `self` (such as `map` and `fold`) while still being able
/// to use the `ChangeStream` afterwards.
///
/// Similar to a `Tail` for a `Cursor`, the only way to create a `ChangeStreamTail` is with
/// `ChangeStream::tail`. See the `Cursor` type documentation for more details on how to use a tail.
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
