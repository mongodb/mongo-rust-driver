use crate::{
    bson::{doc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    collation::Collation,
    error::Result,
    operation::{
        append_options,
        default_impl,
        to_feature,
        Operation,
        OperationDetails,
        ResponseContext,
        Retryability,
    },
    options::{ClientOptions, DeleteOptions, Hint},
    results::DeleteResult,
    Collection,
};

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

impl Operation for Delete {
    type O = DeleteResult;

    const NAME: &'static CStr = cstr!("delete");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );

    fn retryability(&self, options: &ClientOptions) -> Retryability {
        if self.limit == 1 {
            Retryability::write(options)
        } else {
            Retryability::None
        }
    }

    fn is_backpressure_retryable(&self, options: &ClientOptions) -> bool {
        options.retry_writes != Some(false)
    }

    fn details(&self) -> OperationDetails {
        OperationDetails::for_write(&self.target, to_feature!(self.options, write_concern))
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
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

        Ok(Command::from_op_details(
            op_details,
            self.name(),
            (&body).try_into()?,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ResponseContext<'a>,
    ) -> Result<Self::O> {
        response.validate_single_write()?;
        Ok(DeleteResult {
            deleted_count: response.extract_n()?,
        })
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for Delete {}
