use std::{marker::PhantomData, time::Duration};

use bson::{Bson, Document};
use serde::de::DeserializeOwned;

use crate::{coll::options::{FindOneAndDeleteOptions, Hint}, collation::Collation, operation::find_and_modify::options::{FindAndModifyOptions, Modification}, options::WriteConcern, ClientSession, Collection};
use crate::error::Result;
use crate::operation::FindAndModify as Op;

use super::{action_impl, option_setters};

impl<T: DeserializeOwned + Send> Collection<T> {
    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return `Result<Option<T>>`.
    pub fn find_one_and_delete_2(
        &self,
        filter: Document,
    ) -> FindAndModify<'_, T, Delete> {
        FindAndModify {
            coll: self,
            filter,
            modification: Modification::Delete,
            options: None,
            session: None,
            _mode: PhantomData,
        }
    }
}

#[must_use]
pub struct FindAndModify<'a, T, Mode> {
    coll: &'a Collection<T>,
    filter: Document,
    modification: Modification,
    options: Option<FindAndModifyOptions>,
    session: Option<&'a mut ClientSession>,
    _mode: PhantomData<Mode>,
}

pub struct Delete;

impl<'a, T, Mode> FindAndModify<'a, T, Mode> {
    fn options(&mut self) -> &mut FindAndModifyOptions {
        self.options.get_or_insert_with(<FindAndModifyOptions>::default)
    }

    /// Runs the operation using the provided session.
    pub fn session(
        mut self,
        value: impl Into<&'a mut ClientSession>,
    ) -> Self {
        self.session = Some(value.into());
        self
    }
}

impl<'a, T> FindAndModify<'a, T, Delete> {
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

action_impl! {
    impl<'a, T: DeserializeOwned + Send> Action for FindAndModify<'a, T, Delete> {
        type Future = FindAndDeleteFuture;

        async fn execute(mut self) -> Result<Option<T>> {
            resolve_write_concern_with_session!(self.coll, self.options, self.session.as_ref())?;
            
            let op = Op::<T>::with_modification(
                self.coll.namespace(),
                self.filter,
                self.modification,
                self.options,
            );
            self.coll.client().execute_operation(op, self.session).await
        }
    }
}