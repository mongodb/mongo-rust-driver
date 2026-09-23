use serde::Deserialize;

use crate::{
    bson::{doc, rawdoc, Document, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    bson_util::to_raw_bson_array_ser,
    cmap::{Command, RawCommandResponse},
    error::Result,
    operation::{
        default_impl,
        ExecutionContext,
        Feature,
        Operation,
        OperationDetails,
        ResponseHandlingKind,
        Retryability,
    },
    options::ClientOptions,
    Collection,
    SearchIndexModel,
};

#[derive(Debug)]
pub(crate) struct CreateSearchIndexes {
    target: Collection<Document>,
    indexes: Vec<SearchIndexModel>,
}

impl CreateSearchIndexes {
    pub(crate) fn new(target: Collection<Document>, indexes: Vec<SearchIndexModel>) -> Self {
        Self { target, indexes }
    }
}

impl Operation for CreateSearchIndexes {
    type O = Vec<String>;
    const NAME: &'static CStr = cstr!("createSearchIndexes");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, _options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: Feature::NotSupported,
            read_concern: Feature::NotSupported,
            write_concern: Feature::NotSupported,
            supports_sessions: false,
            retryability: Retryability::None,
            is_backpressure_retryable: false,
            override_criteria: None,
            target: (&self.target).into(),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &crate::cmap::StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            rawdoc! {
                Self::NAME: self.target.name(),
                "indexes": to_raw_bson_array_ser(&self.indexes)?,
            },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            indexes_created: Vec<CreatedIndex>,
        }

        #[derive(Debug, Deserialize)]
        struct CreatedIndex {
            #[allow(unused)]
            id: String,
            name: String,
        }

        let response: Response = response.body()?;
        Ok(response
            .indexes_created
            .into_iter()
            .map(|ci| ci.name)
            .collect())
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for CreateSearchIndexes {}

#[derive(Debug)]
pub(crate) struct UpdateSearchIndex {
    target: Collection<Document>,
    name: String,
    definition: Document,
}

impl UpdateSearchIndex {
    pub(crate) fn new(target: Collection<Document>, name: String, definition: Document) -> Self {
        Self {
            target,
            name,
            definition,
        }
    }
}

impl Operation for UpdateSearchIndex {
    type O = ();
    const NAME: &'static CStr = cstr!("updateSearchIndex");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, _options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: Feature::NotSupported,
            read_concern: Feature::NotSupported,
            write_concern: Feature::NotSupported,
            supports_sessions: false,
            retryability: Retryability::None,
            is_backpressure_retryable: false,
            override_criteria: None,
            target: (&self.target).into(),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &crate::cmap::StreamDescription,
        op_details: &OperationDetails,
    ) -> crate::error::Result<crate::cmap::Command> {
        let raw_def: RawDocumentBuf = (&self.definition).try_into()?;
        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            rawdoc! {
                Self::NAME: self.target.name(),
                "name": self.name.as_str(),
                "definition": raw_def,
            },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for UpdateSearchIndex {}

#[derive(Debug)]
pub(crate) struct DropSearchIndex {
    target: Collection<Document>,
    name: String,
}

impl DropSearchIndex {
    pub(crate) fn new(target: Collection<Document>, name: String) -> Self {
        Self { target, name }
    }
}

impl Operation for DropSearchIndex {
    type O = ();
    const NAME: &'static CStr = cstr!("dropSearchIndex");

    default_impl!(
        name,
        extract_at_cluster_time,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, _options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: Feature::NotSupported,
            read_concern: Feature::NotSupported,
            write_concern: Feature::NotSupported,
            supports_sessions: false,
            retryability: Retryability::None,
            is_backpressure_retryable: false,
            override_criteria: None,
            target: (&self.target).into(),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &crate::cmap::StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            rawdoc! {
                Self::NAME: self.target.name(),
                "name": self.name.as_str(),
            },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
    }

    fn handle_error(&self, error: crate::error::Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(())
        } else {
            Err(error)
        }
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for DropSearchIndex {}
