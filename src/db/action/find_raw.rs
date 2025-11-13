use crate::{
    action::{action_impl, FindRawBatches},
    error::Result,
    Namespace,
};

#[action_impl]
impl<'a> Action for FindRawBatches<'a, crate::action::ImplicitSession> {
    type Future = FindRawBatchesFuture;

    async fn execute(mut self) -> Result<crate::cursor::raw_batch::RawBatchCursor> {
        resolve_options!(self.db, self.options, [read_concern, selection_criteria]);

        let ns = Namespace {
            db: self.db.name().to_string(),
            coll: self.collection,
        };

        let op = crate::operation::find_raw::FindRaw::new(ns, self.filter, self.options);
        self.db
            .client()
            .execute_raw_batch_cursor_operation(op)
            .await
    }
}

#[action_impl]
impl<'a> Action for FindRawBatches<'a, crate::action::ExplicitSession<'a>> {
    type Future = FindRawBatchesSessionFuture;

    async fn execute(mut self) -> Result<crate::cursor::raw_batch::SessionRawBatchCursor> {
        resolve_read_concern_with_session!(self.db, self.options, Some(&mut *self.session.0))?;
        resolve_selection_criteria_with_session!(
            self.db,
            self.options,
            Some(&mut *self.session.0)
        )?;

        let ns = Namespace {
            db: self.db.name().to_string(),
            coll: self.collection,
        };

        let op = crate::operation::find_raw::FindRaw::new(ns, self.filter, self.options);
        self.db
            .client()
            .execute_session_raw_batch_cursor_operation(op, self.session.0)
            .await
    }
}
