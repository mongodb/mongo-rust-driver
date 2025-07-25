use crate::bson::rawdoc;

use crate::{
    bson_compat::{cstr, CStr},
    bson_util::append_ser,
    client::session::TransactionPin,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::Retryability,
    options::WriteConcern,
    selection_criteria::SelectionCriteria,
};

use super::{ExecutionContext, OperationWithDefaults, WriteConcernOnlyBody};

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

    const NAME: &'static CStr = cstr!("abortTransaction");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
        };
        if let Some(ref write_concern) = self.write_concern() {
            if !write_concern.is_empty() {
                append_ser(&mut body, cstr!("writeConcern"), write_concern)?;
            }
        }

        Ok(Command::new(Self::NAME, "admin", body))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
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
