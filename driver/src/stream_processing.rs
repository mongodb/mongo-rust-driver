//! Atlas Stream Processing client and helpers.
//!
//! Atlas Stream Processing (ASP) workspaces share the `mongodb://` URI scheme
//! with standard MongoDB clusters but use a distinct hostname pattern:
//!
//! ```text
//! mongodb://atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net/
//! ```
//!
//! Atlas staging endpoints use `.a.query.mongodb-stage.net` instead; both are
//! accepted.
//!
//! Per the ASP spec, TLS is required and `authSource` defaults to `"admin"`.

pub mod options;
pub mod types;

pub use options::*;
pub use types::*;

use crate::{
    bson::Document,
    error::{Error, Result},
    operation::stream_processing::{
        create_stream_processor::CreateStreamProcessor,
        drop_stream_processor::DropStreamProcessor,
        get_more_sample_stream_processor::GetMoreSampleStreamProcessor,
        get_stream_processor::GetStreamProcessor,
        get_stream_processor_stats::GetStreamProcessorStats,
        start_sample_stream_processor::StartSampleStreamProcessor,
        start_stream_processor::StartStreamProcessor,
        stop_stream_processor::StopStreamProcessor,
    },
    options::{ClientOptions, Tls},
    Client,
};

/// Client for an Atlas Stream Processing workspace.
///
/// Distinct from [`Client`] so that connection intent is explicit and ASP
/// commands cannot be accidentally routed to a standard `mongod`. Drivers MAY
/// still use [`Client`] directly with `run_command` for any commands not yet
/// modeled here.
#[derive(Clone, Debug)]
pub struct StreamProcessingClient {
    client: Client,
}

impl StreamProcessingClient {
    /// Creates a new [`StreamProcessingClient`] connected to the workspace
    /// specified by `uri`. The URI must point at a workspace endpoint —
    /// see [`is_workspace_uri`](Self::is_workspace_uri).
    pub async fn with_uri_str(uri: impl AsRef<str>) -> Result<Self> {
        let uri = uri.as_ref();

        if !Self::is_workspace_uri(uri) {
            return Err(Error::invalid_argument(
                "StreamProcessingClient requires a workspace endpoint URI \
                 (atlas-stream-*.a.query.mongodb.net or .mongodb-<env>.net). For standard MongoDB \
                 clusters, use mongodb::Client instead.",
            ));
        }

        if uri.to_ascii_lowercase().starts_with("mongodb+srv://") {
            return Err(Error::invalid_argument(
                "mongodb+srv:// is not supported for workspace endpoints; use mongodb://",
            ));
        }

        let mut options = ClientOptions::parse(uri).await?;
        Self::apply_workspace_defaults(&mut options)?;

        Ok(Self {
            client: Client::with_options(options)?,
        })
    }

    /// Creates a new [`StreamProcessingClient`] from pre-parsed options.
    ///
    /// Workspace defaults — TLS enabled, `authSource=admin` — are applied if
    /// not already set. Returns an error if TLS has been explicitly disabled.
    pub fn with_options(mut options: ClientOptions) -> Result<Self> {
        Self::apply_workspace_defaults(&mut options)?;
        Ok(Self {
            client: Client::with_options(options)?,
        })
    }

    /// Returns the underlying [`Client`] for advanced uses such as
    /// [`Client::database`] followed by `run_command`.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns a handle for managing stream processors in this workspace.
    pub fn stream_processors(&self) -> StreamProcessors {
        StreamProcessors {
            client: self.client.clone(),
        }
    }

    /// Returns `true` when the supplied URI targets an Atlas Stream Processing
    /// workspace endpoint.
    ///
    /// Matches hostnames that begin with `atlas-stream-` and end with
    /// `.a.query.mongodb.net` (production) or `.a.query.mongodb-<env>.net`
    /// (e.g. `mongodb-stage.net` for Atlas staging).
    pub fn is_workspace_uri(uri: &str) -> bool {
        // Lower-case host comparison; URI scheme is case-insensitive.
        let lower = uri.to_ascii_lowercase();
        if !lower.starts_with("mongodb://") {
            return false;
        }
        // Strip scheme + optional userinfo.
        let after_scheme = &lower["mongodb://".len()..];
        let after_userinfo = match after_scheme.rfind('@') {
            Some(idx) if idx < after_scheme.find(['/', '?']).unwrap_or(after_scheme.len()) => {
                &after_scheme[idx + 1..]
            }
            _ => after_scheme,
        };
        // Strip path/query/port — we only need the host.
        let end = after_userinfo
            .find(['/', '?', ':'])
            .unwrap_or(after_userinfo.len());
        let host = &after_userinfo[..end];

        if !host.starts_with("atlas-stream-") {
            return false;
        }

        // Accept .a.query.mongodb.net or .a.query.mongodb-<env>.net
        if host.ends_with(".a.query.mongodb.net") {
            return true;
        }
        if let Some(rest) = host.strip_suffix(".net") {
            if let Some(prefix) = rest.rsplit_once(".a.query.mongodb-") {
                // prefix.1 must be a non-empty environment suffix containing
                // only ascii letters/digits/dashes.
                let env = prefix.1;
                if !env.is_empty() && env.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
                    return true;
                }
            }
        }
        false
    }

    fn apply_workspace_defaults(options: &mut ClientOptions) -> Result<()> {
        match &options.tls {
            Some(Tls::Disabled) => {
                return Err(Error::invalid_argument(
                    "TLS cannot be disabled for an Atlas Stream Processing workspace connection",
                ));
            }
            Some(Tls::Enabled(_)) => {}
            None => {
                options.tls = Some(Tls::Enabled(Default::default()));
            }
        }

        if let Some(cred) = options.credential.as_mut() {
            if cred.source.is_none() {
                cred.source = Some("admin".into());
            }
        }

        Ok(())
    }
}

/// Handle for managing stream processors in a workspace.
///
/// Obtained from [`StreamProcessingClient::stream_processors`].
#[derive(Clone, Debug)]
pub struct StreamProcessors {
    client: Client,
}

impl StreamProcessors {
    /// Creates a new stream processor.
    pub async fn create(
        &self,
        name: impl Into<String>,
        pipeline: Vec<Document>,
        options: impl Into<Option<CreateStreamProcessorOptions>>,
    ) -> Result<()> {
        let op =
            CreateStreamProcessor::new(self.client.clone(), name.into(), pipeline, options.into());
        self.client.execute_operation(op, None).await
    }

    /// Returns a handle for the named processor. Does not imply that the
    /// processor currently exists on the server.
    pub fn get(&self, name: impl Into<String>) -> StreamProcessor {
        StreamProcessor {
            client: self.client.clone(),
            name: name.into(),
        }
    }

    /// Returns information about a single stream processor.
    pub async fn get_info(&self, name: impl Into<String>) -> Result<StreamProcessorInfo> {
        let op = GetStreamProcessor::new(self.client.clone(), name.into());
        self.client.execute_operation(op, None).await
    }
}

/// Handle for a specific named stream processor.
///
/// Holding a handle does not imply the processor currently exists on the
/// server.
#[derive(Clone, Debug)]
pub struct StreamProcessor {
    client: Client,
    name: String,
}

impl StreamProcessor {
    /// Returns the processor name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Starts the processor. The processor MUST be in `STOPPED` or `FAILED`;
    /// starting a `STARTED` processor returns an error from the server.
    pub async fn start(
        &self,
        options: impl Into<Option<StartStreamProcessorOptions>>,
    ) -> Result<()> {
        let op = StartStreamProcessor::new(self.client.clone(), self.name.clone(), options.into());
        self.client.execute_operation(op, None).await
    }

    /// Stops the processor. The processor remains in the `STOPPED` state and
    /// can be restarted.
    pub async fn stop(&self) -> Result<()> {
        let op = StopStreamProcessor::new(self.client.clone(), self.name.clone());
        self.client.execute_operation(op, None).await
    }

    /// Drops the processor permanently. A dropped processor cannot be
    /// recovered.
    #[allow(clippy::should_implement_trait)] // `drop` here is the ASP command, not Drop::drop
    pub async fn drop(&self) -> Result<()> {
        let op = DropStreamProcessor::new(self.client.clone(), self.name.clone());
        self.client.execute_operation(op, None).await
    }

    /// Returns runtime statistics for the processor. Returns an error if the
    /// processor is not in the `STARTED` state.
    pub async fn stats(
        &self,
        options: impl Into<Option<GetStreamProcessorStatsOptions>>,
    ) -> Result<Document> {
        let op =
            GetStreamProcessorStats::new(self.client.clone(), self.name.clone(), options.into());
        self.client.execute_operation(op, None).await
    }

    /// Retrieves a batch of sampled documents.
    ///
    /// Routes to `startSampleStreamProcessor` when no `cursor_id` is supplied
    /// (or `cursor_id` is 0); otherwise routes to `getMoreSampleStreamProcessor`
    /// with the supplied `cursor_id`. The caller MUST stop iterating when the
    /// returned [`cursor_id`](StreamProcessorSamples::cursor_id) is 0.
    pub async fn samples(
        &self,
        options: impl Into<Option<GetStreamProcessorSamplesOptions>>,
    ) -> Result<StreamProcessorSamples> {
        let options = options.into().unwrap_or_default();
        let cursor_id = options.cursor_id.unwrap_or(0);

        if cursor_id == 0 {
            let op = StartSampleStreamProcessor::new(
                self.client.clone(),
                self.name.clone(),
                options.limit,
            );
            let cursor_id = self.client.execute_operation(op, None).await?;
            return Ok(StreamProcessorSamples {
                cursor_id,
                documents: Vec::new(),
            });
        }

        let op = GetMoreSampleStreamProcessor::new(
            self.client.clone(),
            self.name.clone(),
            cursor_id,
            options.batch_size,
        );
        self.client.execute_operation(op, None).await
    }
}

#[cfg(test)]
mod tests {
    use super::StreamProcessingClient;

    #[test]
    fn is_workspace_uri_accepts_production_endpoint() {
        assert!(StreamProcessingClient::is_workspace_uri(
            "mongodb://atlas-stream-699c842ef433fe6001480b17-etif1.virginia-usa.a.query.mongodb.\
             net/"
        ));
    }

    #[test]
    fn is_workspace_uri_accepts_with_credentials_and_port() {
        assert!(StreamProcessingClient::is_workspace_uri(
            "mongodb://user:pass@atlas-stream-xyz.us-east-1.a.query.mongodb.net:27017/?\
             retryWrites=true"
        ));
    }

    #[test]
    fn is_workspace_uri_accepts_staging_endpoint() {
        assert!(StreamProcessingClient::is_workspace_uri(
            "mongodb://user:pass@atlas-stream-699c842ef433fe6001480b17-etif1.virginia-usa.a.query.\
             mongodb-stage.net"
        ));
    }

    #[test]
    fn is_workspace_uri_rejects_standard_cluster() {
        assert!(!StreamProcessingClient::is_workspace_uri(
            "mongodb://localhost:27017/"
        ));
    }

    #[test]
    fn is_workspace_uri_rejects_srv_scheme() {
        assert!(!StreamProcessingClient::is_workspace_uri(
            "mongodb+srv://cluster0.example.mongodb.net/"
        ));
    }

    #[test]
    fn is_workspace_uri_rejects_similar_looking_host() {
        assert!(!StreamProcessingClient::is_workspace_uri(
            "mongodb://atlas-stream-x.example.com/"
        ));
    }

    #[test]
    fn is_workspace_uri_rejects_missing_prefix() {
        assert!(!StreamProcessingClient::is_workspace_uri(
            "mongodb://abc.virginia-usa.a.query.mongodb.net/"
        ));
    }

    #[test]
    fn is_workspace_uri_is_case_insensitive_on_scheme() {
        assert!(StreamProcessingClient::is_workspace_uri(
            "MONGODB://atlas-stream-xyz.us-east-1.a.query.mongodb.net/"
        ));
    }

    #[tokio::test]
    async fn with_uri_str_rejects_non_workspace_uri() {
        let err = StreamProcessingClient::with_uri_str("mongodb://localhost:27017/")
            .await
            .expect_err("expected error for non-workspace URI");
        assert!(err.to_string().contains("workspace endpoint URI"));
    }

    #[tokio::test]
    async fn with_uri_str_rejects_srv_scheme() {
        let err = StreamProcessingClient::with_uri_str(
            "mongodb+srv://atlas-stream-x.us-east-1.a.query.mongodb.net/",
        )
        .await
        .expect_err("expected error for SRV URI");
        // Either the workspace check or the srv check should reject; both are valid.
        let msg = err.to_string();
        assert!(
            msg.contains("workspace endpoint URI") || msg.contains("mongodb+srv://"),
            "unexpected error: {msg}"
        );
    }
}
