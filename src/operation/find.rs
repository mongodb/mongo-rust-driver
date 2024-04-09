use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::{ErrorKind, Result},
    operation::{
        append_options,
        CursorBody,
        OperationWithDefaults,
        Retryability,
        SERVER_4_4_0_WIRE_VERSION,
    },
    options::{CursorType, FindOptions, SelectionCriteria},
    BoxFuture,
    Namespace,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct Find {
    ns: Namespace,
    filter: Document,
    options: Option<Box<FindOptions>>,
}

impl Find {
    pub(crate) fn new(ns: Namespace, filter: Document, mut options: Option<FindOptions>) -> Self {
        if let Some(ref mut options) = options {
            if let Some(ref comment) = options.comment {
                if options.comment_bson.is_none() {
                    options.comment_bson = Some(comment.clone().into());
                }
            }
        }

        Self {
            ns,
            filter,
            options: options.map(Box::new),
        }
    }
}

impl OperationWithDefaults for Find {
    type O = CursorSpecification;
    type Command = Document;
    const NAME: &'static str = "find";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        if let Some(ref options) = self.options {
            // negative limits should be interpreted as request for single batch as per crud spec.
            if options.limit.map(|limit| limit < 0) == Some(true) {
                body.insert("singleBatch", true);
            }

            if options
                .batch_size
                .map(|batch_size| batch_size > std::i32::MAX as u32)
                == Some(true)
            {
                return Err(ErrorKind::InvalidArgument {
                    message: "The batch size must fit into a signed 32-bit integer".to_string(),
                }
                .into());
            }

            match options.cursor_type {
                Some(CursorType::Tailable) => {
                    body.insert("tailable", true);
                }
                Some(CursorType::TailableAwait) => {
                    body.insert("tailable", true);
                    body.insert("awaitData", true);
                }
                _ => {}
            };
        }

        append_options(&mut body, self.options.as_ref())?;

        body.insert("filter", self.filter.clone());

        Ok(Command::new_read(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        CursorBody::extract_at_cluster_time(response)
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let response: CursorBody = response.body()?;

            let description = context.stream_description()?;

            // The comment should only be propagated to getMore calls on 4.4+.
            let comment = if description.max_wire_version.unwrap_or(0) < SERVER_4_4_0_WIRE_VERSION {
                None
            } else {
                self.options
                    .as_ref()
                    .and_then(|opts| opts.comment_bson.clone())
            };

            Ok(CursorSpecification::new(
                response.cursor,
                description.server_address.clone(),
                self.options.as_ref().and_then(|opts| opts.batch_size),
                self.options.as_ref().and_then(|opts| opts.max_await_time),
                comment,
            ))
        }
        .boxed()
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        true
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|opts| opts.selection_criteria.as_ref())
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
