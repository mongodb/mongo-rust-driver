use bson::Document;

use crate::{
    bson::doc,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::{
        append_options,
        remove_empty_write_concern,
        OperationWithDefaults,
        WriteConcernOnlyBody,
    },
    options::{DropCollectionOptions, WriteConcern},
    ClientSession,
    Namespace,
};

use super::{handle_response_sync, OperationResponse};

#[derive(Debug)]
pub(crate) struct DropCollection {
    ns: Namespace,
    options: Option<DropCollectionOptions>,
}

impl DropCollection {
    pub(crate) fn new(ns: Namespace, options: Option<DropCollectionOptions>) -> Self {
        DropCollection { ns, options }
    }
}

impl OperationWithDefaults for DropCollection {
    type O = ();
    type Command = Document;

    const NAME: &'static str = "drop";

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
    ) -> OperationResponse<'static, Self::O> {
        handle_response_sync! {{
            let response: WriteConcernOnlyBody = response.body()?;
            response.validate()
        }}
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
