use std::time::Duration;

use crate::{
    bson::{rawdoc, Bson, RawBson},
    bson_compat::{cstr, CStr},
    checked::Checked,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    cursor::common::{CursorInformation, CursorReply},
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
    options::{ClientOptions, SelectionCriteria},
    results::GetMoreResult,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct GetMore<'conn> {
    ns: Namespace,
    cursor_id: i64,
    selection_criteria: SelectionCriteria,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
    comment: Option<Bson>,
}

impl<'conn> GetMore<'conn> {
    pub(crate) fn new(
        info: CursorInformation,
        pinned: Option<&'conn PinnedConnectionHandle>,
    ) -> Self {
        Self {
            ns: info.ns,
            cursor_id: info.id,
            selection_criteria: SelectionCriteria::from_address(info.address),
            batch_size: info.batch_size,
            max_time: info.max_time,
            pinned_connection: pinned,
            comment: info.comment,
        }
    }
}

impl Operation for GetMore<'_> {
    type O = GetMoreResult;

    const NAME: &'static CStr = cstr!("getMore");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry
    );

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Owned,
            selection_criteria: Feature::Set(self.selection_criteria.clone()),
            read_concern: Feature::NotSupported,
            write_concern: Feature::NotSupported,
            supports_sessions: true,
            retryability: Retryability::None,
            is_backpressure_retryable: options.retry_reads != Some(false),
            override_criteria: None,
            target: OperationTarget::Namespace(self.ns.clone()),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.cursor_id,
            "collection": self.ns.coll.clone(),
        };

        if let Some(batch_size) = self.batch_size {
            let batch_size = Checked::from(batch_size).try_into::<i32>()?;
            if batch_size != 0 {
                body.append(cstr!("batchSize"), batch_size);
            }
        }

        if let Some(ref max_time) = self.max_time {
            body.append(
                cstr!("maxTimeMS"),
                max_time.as_millis().try_into().unwrap_or(i32::MAX),
            );
        }

        if let Some(comment) = &self.comment {
            let raw_comment: RawBson = comment.clone().try_into()?;
            body.append(cstr!("comment"), raw_comment);
        }

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            body,
        ))
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Extract minimal fields directly from the raw reply to avoid walking the batch via serde.
        let root = response.raw_body();
        let cursor = root.get_document("cursor")?;
        let CursorReply {
            id,
            ns,
            post_batch_resume_token,
        } = CursorReply::parse(cursor)?;

        // Take ownership of the raw bytes without copying.
        let raw = response.into_raw_document_buf();

        Ok(GetMoreResult {
            raw_reply: raw,
            exhausted: id == 0,
            post_batch_resume_token,
            ns,
            id,
        })
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for GetMore<'_> {
    #[cfg(feature = "opentelemetry")]
    fn cursor_id(&self) -> Option<i64> {
        Some(self.cursor_id)
    }
}

/*
#[derive(Debug, Deserialize)]
pub(crate) struct GetMoreResponseBody {
    cursor: NextBatchBody,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NextBatchBody {
    id: i64,
    next_batch: VecDeque<RawDocumentBuf>,
    post_batch_resume_token: Option<RawDocumentBuf>,
    ns: String,
}
*/
