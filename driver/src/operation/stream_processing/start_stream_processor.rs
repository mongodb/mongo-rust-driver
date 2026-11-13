use crate::{
    bson::{doc, Bson, Document},
    bson_compat::{cstr, cstr_to_str, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::{ExecutionContext, OperationWithDefaults},
    stream_processing::options::StartStreamProcessorOptions,
    Client,
};

const VALID_FAILOVER_MODES: &[&str] = &["GRACEFUL", "FORCED"];

#[derive(Debug)]
pub(crate) struct StartStreamProcessor {
    client: Client,
    name: String,
    options: Option<StartStreamProcessorOptions>,
}

impl StartStreamProcessor {
    pub(crate) fn new(
        client: Client,
        name: String,
        options: Option<StartStreamProcessorOptions>,
    ) -> Self {
        Self {
            client,
            name,
            options,
        }
    }
}

impl OperationWithDefaults for StartStreamProcessor {
    type O = ();

    const NAME: &'static CStr = cstr!("startStreamProcessor");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! { cstr_to_str(Self::NAME): self.name.as_str() };

        if let Some(opts) = self.options.as_ref() {
            if let Some(workers) = opts.workers {
                body.insert("workers", workers);
            }

            // Spec puts these option fields under a nested `options` document.
            let mut sub = Document::new();
            if let Some(clear) = opts.clear_checkpoints {
                sub.insert("clearCheckpoints", clear);
            }
            if let Some(ts) = opts.start_at_operation_time {
                sub.insert("startAtOperationTime", Bson::Timestamp(ts));
            }
            if let Some(tier) = opts.tier.as_deref() {
                sub.insert("tier", tier);
            }
            if let Some(auto_scaling) = opts.enable_auto_scaling {
                sub.insert("enableAutoScaling", auto_scaling);
            }
            if !sub.is_empty() {
                body.insert("options", sub);
            }

            if let Some(failover) = opts.failover.as_ref() {
                if let Some(mode) = failover.mode.as_deref() {
                    if !VALID_FAILOVER_MODES.contains(&mode) {
                        return Err(Error::invalid_argument(format!(
                            "invalid failover.mode \"{mode}\"; expected one of: {}",
                            VALID_FAILOVER_MODES.join(", "),
                        )));
                    }
                }
                let mut f = doc! { "region": failover.region.as_str() };
                if let Some(mode) = failover.mode.as_deref() {
                    f.insert("mode", mode);
                }
                if let Some(dry) = failover.dry_run {
                    f.insert("dryRun", dry);
                }
                body.insert("failover", f);
            }
        }

        Ok(Command::from_operation(self, (&body).try_into()?))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
    }

    fn target(&self) -> super::super::OperationTarget {
        super::super::OperationTarget::admin(&self.client)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for StartStreamProcessor {}
