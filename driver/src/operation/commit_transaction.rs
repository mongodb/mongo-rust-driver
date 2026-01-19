use std::time::Duration;

use crate::{bson::rawdoc, Client};

use crate::{
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{append_options_to_raw_document, OperationWithDefaults, Retryability},
    options::{Acknowledgment, TransactionOptions, WriteConcern},
};

use super::{ExecutionContext, WriteConcernOnlyBody};

pub(crate) struct CommitTransaction {
    options: Option<TransactionOptions>,
    target: Client,
}

impl CommitTransaction {
    pub(crate) fn new(client: &Client, options: Option<TransactionOptions>) -> Self {
        Self {
            options,
            target: client.clone(),
        }
    }
}

impl OperationWithDefaults for CommitTransaction {
    type O = ();

    const NAME: &'static CStr = cstr!("commitTransaction");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, "admin", body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()
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

    fn target(&self) -> super::OperationTarget {
        super::OperationTarget::admin(&self.target)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for CommitTransaction {}
