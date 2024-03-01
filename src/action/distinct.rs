use std::time::Duration;

use bson::{Bson, Document};

use crate::{
    coll::options::DistinctOptions,
    collation::Collation,
    error::Result,
    operation::Distinct as Op,
    options::ReadConcern,
    selection_criteria::SelectionCriteria,
    ClientSession,
    Collection,
};

use super::{action_impl, option_setters, CollRef};

impl<T> Collection<T> {
    /// Finds the distinct values of the field specified by `field_name` across the collection.
    ///
    /// `await` will return `Result<Vec<Bson>>`.
    pub fn distinct(&self, field_name: impl AsRef<str>, filter: Document) -> Distinct {
        Distinct {
            coll: CollRef::new(self),
            field_name: field_name.as_ref().to_string(),
            filter,
            options: None,
            session: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<T> crate::sync::Collection<T> {
    /// Finds the distinct values of the field specified by `field_name` across the collection.
    ///
    /// [`run`](Distinct::run) will return `Result<Vec<Bson>>`.
    pub fn distinct(&self, field_name: impl AsRef<str>, filter: Document) -> Distinct {
        self.async_collection.distinct(field_name, filter)
    }
}

/// Finds the distinct values of a field.  Construct with [`Collection::distinct`].
#[must_use]
pub struct Distinct<'a> {
    coll: CollRef<'a>,
    field_name: String,
    filter: Document,
    options: Option<DistinctOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> Distinct<'a> {
    option_setters!(options: DistinctOptions;
        max_time: Duration,
        selection_criteria: SelectionCriteria,
        read_concern: ReadConcern,
        collation: Collation,
        comment: Bson,
    );

    /// Runs the operation using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for Distinct<'a> {
        type Future = DistinctFuture;

        async fn execute(mut self) -> Result<Vec<Bson>> {
            resolve_read_concern_with_session!(self.coll, self.options, self.session.as_ref())?;
            resolve_selection_criteria_with_session!(self.coll, self.options, self.session.as_ref())?;

            let op = Op::new(
                self.coll.namespace(),
                self.field_name,
                self.filter,
                self.options,
            );
            self.coll.client().execute_operation(op, self.session).await
        }
    }
}
