#[cfg(test)]
mod test;

use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    cursor::CursorSpecification,
    error::{ErrorKind, Result},
    operation::{append_options, CursorBody, Operation, Retryability},
    options::{CursorType, FindOptions, SelectionCriteria},
    Namespace,
};

use super::{CursorResponse, ReadConcernSupport};

#[derive(Debug)]
pub(crate) struct Find<T> {
    ns: Namespace,
    filter: Option<Document>,
    options: Option<Box<FindOptions>>,
    _phantom: PhantomData<T>,
}

impl<T> Find<T> {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            None,
            None,
        )
    }

    pub(crate) fn new(
        ns: Namespace,
        filter: Option<Document>,
        options: Option<FindOptions>,
    ) -> Self {
        Self {
            ns,
            filter,
            options: options.map(Box::new),
            _phantom: Default::default(),
        }
    }
}

impl<T: DeserializeOwned> Operation for Find<T> {
    type O = CursorSpecification<T>;
    type Command = Document;
    type Response = CursorResponse<T>;
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

        if let Some(ref filter) = self.filter {
            body.insert("filter", filter.clone());
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: CursorBody<T>,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(CursorSpecification::new(
            response.cursor,
            description.server_address.clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            self.options.as_ref().and_then(|opts| opts.max_await_time),
        ))
    }

    fn read_concern_support(&self) -> super::ReadConcernSupport<'_> {
        ReadConcernSupport::Supported(
            self.options
                .as_ref()
                .and_then(|opts| opts.read_concern.as_ref()),
        )
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
