#![allow(unused_variables, dead_code)]

use crate::{
    bson::RawDocumentBuf,
    client::bulk_write::{BulkWriteModel, BulkWriteOptions, BulkWriteResult},
    operation::OperationWithDefaults,
};

pub(crate) struct BulkWrite {
    models: Vec<BulkWriteModel>,
    options: Option<BulkWriteOptions>,
}

impl OperationWithDefaults for BulkWrite {
    type O = BulkWriteResult;

    type Command = RawDocumentBuf;

    const NAME: &'static str = "bulkWrite";

    fn build(
        &mut self,
        description: &crate::cmap::StreamDescription,
    ) -> crate::error::Result<crate::cmap::Command<Self::Command>> {
        todo!()
    }

    fn serialize_command(
        &mut self,
        cmd: crate::cmap::Command<Self::Command>,
    ) -> crate::error::Result<Vec<u8>> {
        todo!()
    }

    fn extract_at_cluster_time(
        &self,
        _response: &bson::RawDocument,
    ) -> crate::error::Result<Option<bson::Timestamp>> {
        todo!()
    }

    fn handle_response(
        &self,
        response: crate::cmap::RawCommandResponse,
        description: &crate::cmap::StreamDescription,
    ) -> crate::error::Result<Self::O> {
        todo!()
    }

    fn handle_error(&self, error: crate::error::Error) -> crate::error::Result<Self::O> {
        todo!()
    }

    fn selection_criteria(&self) -> Option<&crate::selection_criteria::SelectionCriteria> {
        todo!()
    }

    fn is_acknowledged(&self) -> bool {
        todo!()
    }

    fn write_concern(&self) -> Option<&crate::options::WriteConcern> {
        todo!()
    }

    fn supports_read_concern(&self, _description: &crate::cmap::StreamDescription) -> bool {
        todo!()
    }

    fn supports_sessions(&self) -> bool {
        todo!()
    }

    fn retryability(&self) -> super::Retryability {
        todo!()
    }

    fn update_for_retry(&mut self) {
        todo!()
    }

    fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }
}
