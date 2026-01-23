use crate::{
    bson::{doc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    collation::Collation,
    error::{convert_insert_many_error, Result},
    operation::{append_options, OperationWithDefaults, Retryability, WriteResponseBody},
    options::{DeleteOptions, Hint, WriteConcern},
    results::DeleteResult,
    Collection,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct Delete {
    target: Collection<Document>,
    filter: Document,
    limit: u32,
    options: Option<DeleteOptions>,
    collation: Option<Collation>,
    hint: Option<Hint>,
}

impl Delete {
    pub(crate) fn new(
        target: Collection<Document>,
        filter: Document,
        limit: Option<u32>,
        mut options: Option<DeleteOptions>,
    ) -> Self {
        Self {
            target,
            filter,
            limit: limit.unwrap_or(0), // 0 = no limit
            collation: options.as_mut().and_then(|opts| opts.collation.take()),
            hint: options.as_mut().and_then(|opts| opts.hint.take()),
            options,
        }
    }
}

impl OperationWithDefaults for Delete {
    type O = DeleteResult;

    const NAME: &'static CStr = cstr!("delete");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut delete = doc! {
            "q": self.filter.clone(),
            "limit": self.limit,
        };

        if let Some(ref collation) = self.collation {
            delete.insert(
                "collation",
                crate::bson_compat::serialize_to_bson(&collation)?,
            );
        }

        if let Some(ref hint) = self.hint {
            delete.insert("hint", crate::bson_compat::serialize_to_bson(&hint)?);
        }

        let mut body = doc! {
            crate::bson_compat::cstr_to_str(Self::NAME): self.target.name(),
            "deletes": [delete],
            "ordered": true, // command monitoring tests expect this (SPEC-1130)
        };

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::from_operation(self, (&body).try_into()?))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: WriteResponseBody = response.body()?;
        response.validate().map_err(convert_insert_many_error)?;

        Ok(DeleteResult {
            deleted_count: response.n,
        })
    }

    fn write_concern(&self) -> super::Feature<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
            .into()
    }

    fn set_write_concern(&mut self, wc: WriteConcern) {
        self.options.get_or_insert_default().write_concern = Some(wc);
    }

    fn retryability(&self) -> Retryability {
        if self.limit == 1 {
            Retryability::Write
        } else {
            Retryability::None
        }
    }

    fn target(&self) -> super::OperationTarget {
        (&self.target).into()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for Delete {}
