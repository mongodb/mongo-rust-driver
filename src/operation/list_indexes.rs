use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, OperationWithDefaults},
    options::ListIndexesOptions,
    selection_criteria::{ReadPreference, SelectionCriteria},
    BoxFuture,
    Namespace,
};

use super::{CursorBody, ExecutionContext, Retryability};

pub(crate) struct ListIndexes {
    ns: Namespace,
    options: Option<ListIndexesOptions>,
}

impl ListIndexes {
    pub(crate) fn new(ns: Namespace, options: Option<ListIndexesOptions>) -> Self {
        ListIndexes { ns, options }
    }
}

impl OperationWithDefaults for ListIndexes {
    type O = CursorSpecification;
    type Command = Document;

    const NAME: &'static str = "listIndexes";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            "listIndexes": self.ns.coll.clone(),
        };
        if let Some(size) = self.options.as_ref().and_then(|o| o.batch_size) {
            body.insert("cursor", doc! { "batchSize": size });
        }
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let response: CursorBody = response.body()?;
            Ok(CursorSpecification::new(
                response.cursor,
                context
                    .connection
                    .stream_description()?
                    .server_address
                    .clone(),
                self.options.as_ref().and_then(|o| o.batch_size),
                self.options.as_ref().and_then(|o| o.max_time),
                None,
            ))
        }
        .boxed()
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
