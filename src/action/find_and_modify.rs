use std::{borrow::Borrow, marker::PhantomData, time::Duration};

use bson::{Bson, Document};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    coll::options::{
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        Hint,
        ReturnDocument,
        UpdateModifications,
    },
    collation::Collation,
    error::Result,
    operation::{
        find_and_modify::options::{FindAndModifyOptions, Modification},
        FindAndModify as Op,
        UpdateOrReplace,
    },
    options::WriteConcern,
    ClientSession,
    Collection,
};

use super::{action_impl, option_setters};

impl<T: DeserializeOwned + Send + Sync> Collection<T> {
    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return `Result<Option<T>>`.
    pub fn find_one_and_delete(&self, filter: Document) -> FindAndModify<'_, T, Delete> {
        FindAndModify {
            coll: self,
            filter,
            modification: Ok(Modification::Delete),
            options: None,
            session: None,
            _mode: PhantomData,
        }
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return `Result<Option<T>>`.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
    ) -> FindAndModify<'_, T, Update> {
        let update = update.into();
        FindAndModify {
            coll: self,
            filter,
            modification: Ok(Modification::Update(update.into())),
            options: None,
            session: None,
            _mode: PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync> Collection<T> {
    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: impl Borrow<T>,
    ) -> FindAndModify<'_, T, Replace> {
        let human_readable_serialization = self.human_readable_serialization();
        FindAndModify {
            coll: self,
            filter,
            modification: UpdateOrReplace::replacement(
                replacement.borrow(),
                human_readable_serialization,
            )
            .map(Modification::Update),
            options: None,
            session: None,
            _mode: PhantomData,
        }
    }
}

#[cfg(feature = "sync")]
impl<T: DeserializeOwned + Send + Sync> crate::sync::Collection<T> {
    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](FindAndModify::run) will return `Result<Option<T>>`.
    pub fn find_one_and_delete(&self, filter: Document) -> FindAndModify<'_, T, Delete> {
        self.async_collection.find_one_and_delete(filter)
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](FindAndModify::run) will return `Result<Option<T>>`.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
    ) -> FindAndModify<'_, T, Update> {
        self.async_collection.find_one_and_update(filter, update)
    }
}

#[cfg(feature = "sync")]
impl<T: Serialize + DeserializeOwned + Send + Sync> crate::sync::Collection<T> {
    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: impl Borrow<T>,
    ) -> FindAndModify<'_, T, Replace> {
        self.async_collection
            .find_one_and_replace(filter, replacement)
    }
}

/// Atomically find up to one document in the collection matching a filter and modify it.  Construct
/// with [`Collection::find_one_and_delete`].
#[must_use]
pub struct FindAndModify<'a, T: Send + Sync, Mode> {
    coll: &'a Collection<T>,
    filter: Document,
    modification: Result<Modification>,
    options: Option<FindAndModifyOptions>,
    session: Option<&'a mut ClientSession>,
    _mode: PhantomData<Mode>,
}

pub struct Delete;
pub struct Update;
pub struct Replace;

impl<'a, T: Send + Sync, Mode> FindAndModify<'a, T, Mode> {
    fn options(&mut self) -> &mut FindAndModifyOptions {
        self.options
            .get_or_insert_with(<FindAndModifyOptions>::default)
    }

    /// Runs the operation using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

impl<'a, T: Send + Sync> FindAndModify<'a, T, Delete> {
    /// Set all options.  Note that this will replace all previous values set.
    pub fn with_options(mut self, value: impl Into<Option<FindOneAndDeleteOptions>>) -> Self {
        self.options = value.into().map(FindAndModifyOptions::from);
        self
    }

    option_setters! { FindOneAndDeleteOptions;
        max_time: Duration,
        projection: Document,
        sort: Document,
        write_concern: WriteConcern,
        collation: Collation,
        hint: Hint,
        let_vars: Document,
        comment: Bson,
    }
}

impl<'a, T: Send + Sync> FindAndModify<'a, T, Update> {
    /// Set all options.  Note that this will replace all previous values set.
    pub fn with_options(mut self, value: impl Into<Option<FindOneAndUpdateOptions>>) -> Self {
        self.options = value.into().map(FindAndModifyOptions::from);
        self
    }

    option_setters! { FindOneAndUpdateOptions;
        array_filters: Vec<Document>,
        bypass_document_validation: bool,
        max_time: Duration,
        projection: Document,
        sort: Document,
        upsert: bool,
        write_concern: WriteConcern,
        collation: Collation,
        hint: Hint,
        let_vars: Document,
        comment: Bson,
    }

    /// Set the [`FindOneAndUpdateOptions::return_document`] option.
    pub fn return_document(mut self, value: ReturnDocument) -> Self {
        self.options().new = Some(value.as_bool());
        self
    }
}

impl<'a, T: Send + Sync> FindAndModify<'a, T, Replace> {
    /// Set all options.  Note that this will replace all previous values set.
    pub fn with_options(mut self, value: impl Into<Option<FindOneAndReplaceOptions>>) -> Self {
        self.options = value.into().map(FindAndModifyOptions::from);
        self
    }

    option_setters! { FindOneAndReplaceOptions;
        bypass_document_validation: bool,
        max_time: Duration,
        projection: Document,
        sort: Document,
        upsert: bool,
        write_concern: WriteConcern,
        collation: Collation,
        hint: Hint,
        let_vars: Document,
        comment: Bson,
    }

    /// Set the [`FindOneAndReplaceOptions::return_document`] option.
    pub fn return_document(mut self, value: ReturnDocument) -> Self {
        self.options().new = Some(value.as_bool());
        self
    }
}

action_impl! {
    impl<'a, T: DeserializeOwned + Send + Sync, Mode> Action for FindAndModify<'a, T, Mode> {
        type Future = FindAndDeleteFuture<'a, T: DeserializeOwned + Send + Sync>;

        async fn execute(mut self) -> Result<Option<T>> {
            resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;

            let op = Op::<T>::with_modification(
                self.coll.namespace(),
                self.filter,
                self.modification?,
                self.options,
            )?;
            self.coll.client().execute_operation(op, self.session).await
        }
    }
}
