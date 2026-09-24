use crate::{
    bson::rawdoc,
    bson_compat::{cstr, CStr},
    client::{session::TransactionPin, Retry},
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{
        default_impl,
        ExecutionContext,
        Feature,
        Operation,
        OperationDetails,
        OperationTarget,
        ResponseHandlingKind,
        Retryability,
    },
    options::{ClientOptions, WriteConcern},
    Client,
};

pub(crate) struct AbortTransaction {
    write_concern: Option<WriteConcern>,
    pinned: Option<TransactionPin>,
    target: Client,
}

impl AbortTransaction {
    pub(crate) fn new(
        client: &Client,
        write_concern: Option<WriteConcern>,
        pinned: Option<TransactionPin>,
    ) -> Self {
        Self {
            write_concern,
            pinned,
            target: client.clone(),
        }
    }
}

impl Operation for AbortTransaction {
    type O = ();

    const NAME: &'static CStr = cstr!("abortTransaction");

    default_impl!(name, extract_at_cluster_time, handle_error);

    fn details(&self, _options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: match &self.pinned {
                Some(TransactionPin::Mongos(s)) => Feature::Set(s.clone()),
                _ => Feature::NotSupported,
            },
            read_concern: Feature::NotSupported,
            write_concern: self.write_concern.clone().into(),
            supports_sessions: true,
            // abortTransaction is retryable regardless of the value of retryWrites
            retryability: Retryability::Write,
            is_backpressure_retryable: true,
            override_criteria: None,
            target: OperationTarget::admin(&self.target),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let body = rawdoc! {
            Self::NAME: 1,
        };

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        response.validate_single_write()
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        match &self.pinned {
            Some(TransactionPin::Connection(h)) => Some(h),
            _ => None,
        }
    }

    fn update_for_retry(&mut self, _retry: Option<&Retry>) {
        // The session must be "unpinned" before server selection for a retry.
        self.pinned = None;
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for AbortTransaction {}
