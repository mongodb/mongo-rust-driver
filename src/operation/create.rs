use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{
        append_options,
        remove_empty_write_concern,
        OperationWithDefaults,
        WriteConcernOnlyBody,
    },
    options::{CreateCollectionOptions, WriteConcern},
    BoxFuture,
    ClientSession,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Create {
    ns: Namespace,
    options: Option<CreateCollectionOptions>,
}

impl Create {
    pub(crate) fn new(ns: Namespace, options: Option<CreateCollectionOptions>) -> Self {
        Self { ns, options }
    }
}

impl OperationWithDefaults for Create {
    type O = ();
    type Command = Document;

    const NAME: &'static str = "create";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> BoxFuture<'static, Result<Self::O>> {
        async move {
            let response: WriteConcernOnlyBody = response.body()?;
            response.validate()
        }
        .boxed()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
