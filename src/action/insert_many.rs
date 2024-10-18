use std::{borrow::Borrow, collections::HashSet, ops::Deref};

use bson::{Bson, RawDocumentBuf};
use serde::Serialize;

use crate::{
    coll::options::InsertManyOptions,
    error::{Error, ErrorKind, InsertManyError, Result},
    operation::Insert as Op,
    options::WriteConcern,
    results::InsertManyResult,
    ClientSession,
    Collection,
};

use super::{action_impl, deeplink, option_setters, CollRef};

impl<T: Serialize + Send + Sync> Collection<T> {
    /// Inserts the data in `docs` into the collection.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return d[`Result<InsertManyResult>`].
    #[deeplink]
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> InsertMany {
        InsertMany {
            coll: CollRef::new(self),
            docs: docs
                .into_iter()
                .map(|v| bson::to_raw_document_buf(v.borrow()).map_err(Into::into))
                .collect(),
            options: None,
            session: None,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: Serialize + Send + Sync> crate::sync::Collection<T> {
    /// Inserts the data in `docs` into the collection.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](InsertMany::run) will return d[`Result<InsertManyResult>`].
    #[deeplink]
    pub fn insert_many(&self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> InsertMany {
        self.async_collection.insert_many(docs)
    }
}

/// Inserts documents into a collection.  Construct with [`Collection::insert_many`].
#[must_use]
pub struct InsertMany<'a> {
    coll: CollRef<'a>,
    docs: Result<Vec<RawDocumentBuf>>,
    options: Option<InsertManyOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> InsertMany<'a> {
    option_setters! { options: InsertManyOptions;
        bypass_document_validation: bool,
        ordered: bool,
        write_concern: WriteConcern,
        comment: Bson,
    }

    /// Use the provided session when running the operation.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

struct Cumulative {
    /// number of document results processed in prior batches
    offset: usize,
    /// if only `inserted_ids` is populated, does not actually represent an error
    maybe_failure: InsertManyError,
    labels: HashSet<String>,
}
impl Cumulative {
    fn new() -> Self {
        Self {
            offset: Default::default(),
            maybe_failure: InsertManyError {
                write_errors: Default::default(),
                write_concern_error: Default::default(),
                inserted_ids: Default::default(),
            },
            labels: Default::default(),
        }
    }
    fn labels<S: Into<String>>(&mut self, other: impl IntoIterator<Item = S>) {
        self.labels.extend(other.into_iter().map(Into::into))
    }
    fn result(self) -> Result<InsertManyResult> {
        // destructuring here so compilation fails if new fields are added
        // unused variable warnings would indicate non-exhaustive checking
        let Cumulative {
            offset: _,
            maybe_failure:
                InsertManyError {
                    write_errors,
                    write_concern_error,
                    inserted_ids,
                },
            labels,
        } = self;
        if write_errors.as_ref().map_or(false, |we| !we.is_empty())
            || write_concern_error.is_some()
            || !labels.is_empty()
        {
            Err(Error::new(
                ErrorKind::InsertMany(InsertManyError {
                    write_errors,
                    write_concern_error,
                    inserted_ids,
                }),
                (!labels.is_empty()).then_some(labels),
            ))
        } else {
            Ok(InsertManyResult { inserted_ids })
        }
    }
}
impl std::ops::AddAssign<InsertManyResult> for Cumulative {
    fn add_assign(&mut self, other: InsertManyResult) {
        self.maybe_failure.inserted_ids.extend(
            other
                .inserted_ids
                .into_iter()
                .map(|(id, v)| (id + self.offset, v)),
        )
    }
}
impl std::ops::AddAssign<InsertManyError> for Cumulative {
    fn add_assign(&mut self, other: InsertManyError) {
        let this = &mut self.maybe_failure;
        this.inserted_ids.extend(
            other
                .inserted_ids
                .into_iter()
                .map(|(id, v)| (id + self.offset, v)),
        );
        if let Some(mut other) = other.write_errors {
            if let Some(this) = &mut this.write_errors {
                this.extend(other.into_iter().map(|mut x| {
                    x.index += self.offset;
                    x
                }))
            } else {
                other.iter_mut().for_each(|x| x.index += self.offset);
                this.write_errors = Some(other)
            }
        }
        if other.write_concern_error.is_some() {
            // technically the left error gets overwritten, but we should never get here
            debug_assert!(this.write_concern_error.is_none());
            this.write_concern_error = other.write_concern_error
        };
        self.offset = this.inserted_ids.len() + this.write_errors.as_ref().map_or(0, |we| we.len());
    }
}

#[action_impl]
impl<'a> Action for InsertMany<'a> {
    type Future = InsertManyFuture;

    async fn execute(mut self) -> Result<InsertManyResult> {
        resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;

        let ds = self.docs?;
        if ds.is_empty() {
            return Err(ErrorKind::InvalidArgument {
                message: "No documents provided to insert_many".to_string(),
            }
            .into());
        }
        let ordered = self
            .options
            .as_ref()
            .and_then(|o| o.ordered)
            .unwrap_or(true);
        let encrypted = self.coll.client().should_auto_encrypt().await;

        let mut cumulative = Cumulative::new();

        while cumulative.offset < ds.len() {
            let docs: Vec<_> = ds
                .iter()
                .skip(cumulative.offset)
                .map(Deref::deref)
                .collect();
            let insert = Op::new(self.coll.namespace(), docs, self.options.clone(), encrypted);

            match self
                .coll
                .client()
                .execute_operation(insert, self.session.as_deref_mut())
                .await
            {
                Ok(result) => {
                    cumulative += result;
                }
                Err(e) => match &*e.kind {
                    ErrorKind::InsertMany(_) => {
                        cumulative.labels(e.labels());
                        if let ErrorKind::InsertMany(bw) = *e.kind {
                            cumulative += bw;
                        }
                        if ordered {
                            break;
                        }
                    }
                    _ => return Err(e),
                },
            }
        }
        cumulative.result()
    }
}
