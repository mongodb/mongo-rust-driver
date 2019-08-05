pub mod document;
pub mod options;
#[cfg(test)]
mod test;

use std::marker::PhantomData;

use bson::{Bson, Document};
use serde::Deserialize;

use self::{document::*, options::*};
use crate::{
    error::{CommandError, Error, ErrorKind, Result},
    options::AggregateOptions,
    read_preference::ReadPreference,
    Client, Collection, Cursor, Database,
};

const NON_RESUMABLE_ERROR_LABEL: &str = "NonResumableChangeStreamError";
const INTERRUPTED_ERROR_CODE: i32 = 11601;
const CAPPED_POSITION_LOST_ERROR_CODE: i32 = 136;
const CURSOR_KILLED_ERROR_CODE: i32 = 237;

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
    pub(crate) fn new(
        mut cursor: Cursor,
        pipeline: Vec<Document>,
        client: Client,
        target: ChangeStreamTarget,
        resume_token: Option<ResumeToken>,
        options: Option<ChangeStreamOptions>,
        read_preference: Option<ReadPreference>,
    ) -> Self {
        Self {
            pipeline,
            client,
            target,
            resume_token: cursor.resume_token().or(resume_token),
            options,
            read_preference,
            resume_attempted: false,
            document_returned: false,
            phantom: PhantomData,
            cursor,
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

    fn try_resume(&mut self, error: Error) -> Result<()> {
        self.resume_attempted = true;

        match error.kind() {
            ErrorKind::CommandError(err) => {
                if err.is_resumable() {
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

        if resume_token.is_some() {
            if options.start_after.is_some() && !self.document_returned {
                options.start_after = resume_token;
                options.start_at_operation_time = None;
            } else {
                options.resume_after = resume_token;
                options.start_after = None;
                options.start_at_operation_time = None;
            }
        } else if max_wire_version >= 7 {
            if let Some(time) = options.start_at_operation_time.or(op_time) {
                options.start_at_operation_time = Some(time)
            }
        }

        let aggregate_options = options.get_aggregation_options();

        self.cursor = self.target.aggregate(
            make_pipeline(&self.target, self.pipeline.clone(), self.options.as_ref())?,
            Some(aggregate_options),
        )?;

        if let Some(resume_token) = self.cursor.resume_token() {
            self.resume_token = Some(resume_token);
        }

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
            Some(Ok(doc)) => {
                self.resume_attempted = false;
                self.document_returned = true;

                if self.cursor.batch_exhausted() {
                    self.resume_token = match doc.get("postBatchResumeToken") {
                        Some(token) => Some(ResumeToken(token.clone())),
                        None => doc.get("_id").map(|id| ResumeToken(id.clone())),
                    };
                }

                Some(match bson::from_bson(Bson::Document(doc)) {
                    Ok(result) => Ok(result),
                    Err(_) => Err(Error::from_kind(ErrorKind::ResponseError(
                        "unable to deserialize change stream event document".to_string(),
                    ))),
                })
            }
            Some(Err(error)) => {
                if !self.resume_attempted {
                    match self.try_resume(error) {
                        Ok(_) => self.next(),
                        Err(resume_error) => Some(Err(resume_error)),
                    }
                } else {
                    Some(Err(error))
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

pub(crate) fn make_pipeline(
    target: &ChangeStreamTarget,
    pipeline: Vec<Document>,
    options: Option<&ChangeStreamOptions>,
) -> Result<Vec<Document>> {
    let mut options = match options {
        Some(opts) => crate::bson_util::bson_into_document(bson::to_bson(opts)?)?,
        None => Document::new(),
    };

    if let ChangeStreamTarget::Cluster(_) = target {
        options.insert("allChangesForCluster", true);
    };

    let mut watch_pipeline = vec![doc! { "$changeStream": options }];
    watch_pipeline.extend(pipeline);

    Ok(watch_pipeline)
}

impl CommandError {
    fn is_resumable(&self) -> bool {
        if self
            .labels
            .iter()
            .any(|label| label == NON_RESUMABLE_ERROR_LABEL)
        {
            return false;
        }

        if self.code == INTERRUPTED_ERROR_CODE
            || self.code == CAPPED_POSITION_LOST_ERROR_CODE
            || self.code == CURSOR_KILLED_ERROR_CODE
        {
            return false;
        }

        true
    }
}
