use bson::Document;

use crate::{
    bson::doc,
    client::session::TransactionPin,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::Retryability,
    options::WriteConcern,
    selection_criteria::SelectionCriteria,
};

use super::{OperationWithDefaults, WriteConcernOnlyBody};

pub(crate) struct AbortTransaction {
    write_concern: Option<WriteConcern>,
    pinned: Option<TransactionPin>,
}

impl AbortTransaction {
    pub(crate) fn new(write_concern: Option<WriteConcern>, pinned: Option<TransactionPin>) -> Self {
        Self {
            write_concern,
            pinned,
        }
    }
}

impl OperationWithDefaults for AbortTransaction {
    type O = ();
    type Command = Document;

    const NAME: &'static str = "abortTransaction";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };
        if let Some(ref write_concern) = self.write_concern() {
            if !write_concern.is_empty() {
                body.insert("writeConcern", bson::to_bson(write_concern)?);
            }
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            "admin".to_string(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        match &self.pinned {
            Some(TransactionPin::Mongos(s)) => Some(s),
            _ => None,
        }
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        match &self.pinned {
            Some(TransactionPin::Connection(h)) => Some(h),
            _ => None,
        }
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern.as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }

    fn update_for_retry(&mut self) {
        // The session must be "unpinned" before server selection for a retry.
        self.pinned = None;
    }
}
