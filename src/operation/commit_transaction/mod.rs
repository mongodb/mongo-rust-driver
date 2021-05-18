use std::time::Duration;

use bson::doc;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, Operation, Retryability},
    options::{Acknowledgment, TransactionOptions, WriteConcern},
};

use super::WriteConcernOnlyBody;

pub(crate) struct CommitTransaction {
    options: Option<TransactionOptions>,
}

impl CommitTransaction {
    pub(crate) fn new(options: Option<TransactionOptions>) -> Self {
        Self { options }
    }
}

impl Operation for CommitTransaction {
    type O = ();
    const NAME: &'static str = "commitTransaction";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            "admin".to_string(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: CommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        response.body::<WriteConcernOnlyBody>()?.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        Retryability::Write
    }

    // Updates the write concern to use w: majority and a w_timeout of 10000 if w_timeout is not
    // already set. The write concern on a commitTransaction command should be updated if a
    // commit is being retried internally or by the user.
    fn update_for_retry(&mut self) {
        let options = self
            .options
            .get_or_insert_with(|| TransactionOptions::builder().build());
        match &mut options.write_concern {
            Some(write_concern) => {
                write_concern.w = Some(Acknowledgment::Majority);
                if write_concern.w_timeout.is_none() {
                    write_concern.w_timeout = Some(Duration::from_millis(10000));
                }
            }
            None => {
                options.write_concern = Some(
                    WriteConcern::builder()
                        .w(Acknowledgment::Majority)
                        .w_timeout(Duration::from_millis(10000))
                        .build(),
                );
            }
        }
    }
}
