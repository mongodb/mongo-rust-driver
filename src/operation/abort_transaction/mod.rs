use crate::{
    bson::doc,
    cmap::{Command, StreamDescription},
    error::Result,
    operation::{Operation, Retryability},
    options::WriteConcern,
};

use super::{CommandResponse, Response, WriteConcernOnlyBody};

pub(crate) struct AbortTransaction {
    write_concern: Option<WriteConcern>,
}

impl AbortTransaction {
    pub(crate) fn new(write_concern: Option<WriteConcern>) -> Self {
        Self { write_concern }
    }
}

impl Operation for AbortTransaction {
    type O = ();
    type Response = CommandResponse<WriteConcernOnlyBody>;

    const NAME: &'static str = "abortTransaction";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };
        if let Some(ref write_concern) = self.write_concern {
            body.insert("writeConcern", bson::to_bson(write_concern)?);
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            "admin".to_string(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: <Self::Response as Response>::Body,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        response.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern.as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }
}
