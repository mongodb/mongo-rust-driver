use crate::bson::RawDocumentBuf;

use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::{raw_batch::RawBatchCursorSpecification, CursorInformation},
    error::{Error, Result},
    operation::{
        append_options_to_raw_document, CursorBody, OperationWithDefaults,
        SERVER_4_4_0_WIRE_VERSION,
    },
    options::{CursorType, FindOptions, SelectionCriteria},
    Namespace,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct FindRaw {
    ns: Namespace,
    filter: Document,
    options: Option<Box<FindOptions>>,
}

impl FindRaw {
    pub(crate) fn new(ns: Namespace, filter: Document, options: Option<FindOptions>) -> Self {
        Self {
            ns,
            filter,
            options: options.map(Box::new),
        }
    }
}

impl OperationWithDefaults for FindRaw {
    type O = RawBatchCursorSpecification;
    const NAME: &'static CStr = cstr!("find");

    fn wants_owned_response(&self) -> bool {
        true
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Parse minimal fields via raw to avoid per-doc copies.
        let raw_root = response.raw_body();
        let cursor_doc = raw_root
            .get("cursor")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .ok_or_else(|| Error::invalid_response("missing cursor in response"))?;

        let id = cursor_doc
            .get("id")?
            .and_then(crate::bson::RawBsonRef::as_i64)
            .ok_or_else(|| Error::invalid_response("missing cursor id"))?;

        let ns_str = cursor_doc
            .get("ns")?
            .and_then(crate::bson::RawBsonRef::as_str)
            .ok_or_else(|| Error::invalid_response("missing cursor ns"))?;
        let ns = Namespace::from_str(ns_str)
            .ok_or_else(|| Error::invalid_response("invalid cursor ns"))?;

        let post_token_raw = cursor_doc
            .get("postBatchResumeToken")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .map(|d| RawDocumentBuf::from_bytes(d.as_bytes().to_vec()))
            .transpose()?;
        let post_batch_resume_token =
            crate::change_stream::event::ResumeToken::from_raw(post_token_raw);

        let description = context.connection.stream_description()?;
        let comment = if description.max_wire_version.unwrap_or(0) < SERVER_4_4_0_WIRE_VERSION {
            None
        } else {
            self.options.as_ref().and_then(|opts| opts.comment.clone())
        };

        let info = CursorInformation {
            ns,
            id,
            address: description.server_address.clone(),
            batch_size: self.options.as_ref().and_then(|opts| opts.batch_size),
            max_time: self.options.as_ref().and_then(|opts| opts.max_await_time),
            comment,
        };

        // Take ownership of the raw reply with zero copies.
        let raw = response.into_raw_document_buf();

        Ok(RawBatchCursorSpecification {
            info,
            initial_reply: raw,
            post_batch_resume_token,
        })
    }

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        if let Some(ref mut options) = self.options {
            if options.limit.map(|limit| limit < 0) == Some(true) {
                body.append(cstr!("singleBatch"), true);
            }

            if let Some(ref mut batch_size) = options.batch_size {
                if i32::try_from(*batch_size).is_err() {
                    return Err(Error::invalid_argument(
                        "the batch size must fit into a signed 32-bit integer",
                    ));
                }
                if let Some(limit) = options.limit.and_then(|limit| u32::try_from(limit).ok()) {
                    if *batch_size == limit {
                        *batch_size += 1;
                    }
                }
            }

            match options.cursor_type {
                Some(CursorType::Tailable) => {
                    body.append(cstr!("tailable"), true);
                }
                Some(CursorType::TailableAwait) => {
                    body.append(cstr!("tailable"), true);
                    body.append(cstr!("awaitData"), true);
                }
                _ => {}
            };
        }

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        let raw_filter: RawDocumentBuf = (&self.filter).try_into()?;
        body.append(cstr!("filter"), raw_filter);

        Ok(Command::new_read(
            Self::NAME,
            &self.ns.db,
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        CursorBody::extract_at_cluster_time(response)
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Build initial spec using minimal parsing and copy of raw reply.
        let raw = RawDocumentBuf::from_bytes(response.as_bytes().to_vec())?;

        // Parse minimal fields via raw to avoid per-doc copies.
        let raw_root = response.raw_body();
        let cursor_doc = raw_root
            .get("cursor")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .ok_or_else(|| Error::invalid_response("missing cursor in response"))?;

        let id = cursor_doc
            .get("id")?
            .and_then(crate::bson::RawBsonRef::as_i64)
            .ok_or_else(|| Error::invalid_response("missing cursor id"))?;

        let ns_str = cursor_doc
            .get("ns")?
            .and_then(crate::bson::RawBsonRef::as_str)
            .ok_or_else(|| Error::invalid_response("missing cursor ns"))?;
        let ns = Namespace::from_str(ns_str)
            .ok_or_else(|| Error::invalid_response("invalid cursor ns"))?;

        let post_token_raw = cursor_doc
            .get("postBatchResumeToken")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .map(|d| RawDocumentBuf::from_bytes(d.as_bytes().to_vec()))
            .transpose()?;
        let post_batch_resume_token =
            crate::change_stream::event::ResumeToken::from_raw(post_token_raw);

        let description = context.connection.stream_description()?;
        let comment = if description.max_wire_version.unwrap_or(0) < SERVER_4_4_0_WIRE_VERSION {
            None
        } else {
            self.options.as_ref().and_then(|opts| opts.comment.clone())
        };

        let info = CursorInformation {
            ns,
            id,
            address: description.server_address.clone(),
            batch_size: self.options.as_ref().and_then(|opts| opts.batch_size),
            max_time: self.options.as_ref().and_then(|opts| opts.max_await_time),
            comment,
        };

        Ok(RawBatchCursorSpecification {
            info,
            initial_reply: raw,
            post_batch_resume_token,
        })
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        true
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|opts| opts.selection_criteria.as_ref())
    }
}
