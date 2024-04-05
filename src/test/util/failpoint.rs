use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Document},
    error::Result,
    selection_criteria::{ReadPreference, SelectionCriteria},
    test::log_uncaptured,
    Client,
};

impl Client {
    /// Configure a fail point on this client. Any test that calls this method must use the
    /// #[tokio::test(flavor = "multi_thread")] test annotation. The guard returned from this
    /// method should remain in scope while the fail point is intended for use. Upon drop, the
    /// guard will disable the fail point on the server.
    pub(crate) async fn enable_fail_point(&self, fail_point: FailPoint) -> Result<FailPointGuard> {
        let command = bson::to_document(&fail_point)?;
        self.database("admin")
            .run_command(command)
            .selection_criteria(fail_point.selection_criteria.clone())
            .await?;

        Ok(FailPointGuard {
            client: self.clone(),
            failure_type: fail_point.failure_type,
            selection_criteria: fail_point.selection_criteria,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FailPoint {
    /// The type of failure to configure. The current valid values are "failCommand" and
    /// "failGetMoreAfterCursorCheckout".
    #[serde(rename = "configureFailPoint")]
    failure_type: String,

    /// The fail point's mode.
    mode: FailPointMode,

    /// The data associated with the fail point. This includes the commands that should fail and
    /// the error information that should be returned.
    #[serde(default)]
    data: Document,

    /// The selection criteria to use when configuring this fail point.
    #[serde(skip, default = "primary_selection_criteria")]
    selection_criteria: SelectionCriteria,
}

fn primary_selection_criteria() -> SelectionCriteria {
    ReadPreference::Primary.into()
}

impl FailPoint {
    /// Creates a new failCommand FailPoint. Call the various builder methods on the returned
    /// FailPoint to configure the type of failure that should occur.
    pub(crate) fn new(command_names: &[&str], mode: FailPointMode) -> Self {
        let data = doc! { "failCommands": command_names };
        Self {
            failure_type: "failCommand".to_string(),
            mode,
            data,
            selection_criteria: ReadPreference::Primary.into(),
        }
    }

    /// The appName that a client must use to hit this fail point.
    pub(crate) fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.data.insert("appName", app_name.into());
        self
    }

    /// How long the server should block the affected commands. Only available on 4.2.9+ servers.
    pub(crate) fn block_connection(mut self, block_connection_duration: Duration) -> Self {
        self.data.insert("blockConnection", true);
        self.data
            .insert("blockTimeMS", block_connection_duration.as_millis() as i64);
        self
    }

    /// Whether the server should close the connection when the client sends an affected command.
    /// Defaults to false.
    pub(crate) fn close_connection(mut self, close_connection: bool) -> Self {
        self.data.insert("closeConnection", close_connection);
        self
    }

    /// The error code to include in the server's reply to an affected command.
    pub(crate) fn error_code(mut self, error_code: i64) -> Self {
        self.data.insert("errorCode", error_code);
        self
    }

    /// The error labels to include in the server's reply to an affected command. Note that the
    /// value passed to this method will completely override the labels that the server would
    /// otherwise return. Only available on 4.4+ servers.
    pub(crate) fn error_labels(
        mut self,
        error_labels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let error_labels: Vec<String> = error_labels.into_iter().map(Into::into).collect();
        self.data.insert("errorLabels", error_labels);
        self
    }

    /// The write concern error to include in the server's reply to an affected command.
    pub(crate) fn write_concern_error(mut self, write_concern_error: Document) -> Self {
        self.data.insert("writeConcernError", write_concern_error);
        self
    }

    /// The selection criteria to use when enabling this fail point. Defaults to a primary read
    /// preference if unspecified.
    pub(crate) fn selection_criteria(mut self, selection_criteria: SelectionCriteria) -> Self {
        self.selection_criteria = selection_criteria;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(unused)]
pub(crate) enum FailPointMode {
    AlwaysOn,
    Times(i32),
    Skip(i32),
    Off,
    ActivationProbability(f32),
}

#[derive(Debug)]
#[must_use]
pub(crate) struct FailPointGuard {
    client: Client,
    failure_type: String,
    selection_criteria: SelectionCriteria,
}

impl Drop for FailPointGuard {
    fn drop(&mut self) {
        let client = self.client.clone();

        // This forces the Tokio runtime to not finish shutdown until this future has completed.
        // Unfortunately, this also means that tests using FailPointGuards have to use the
        // multi-threaded runtime.
        let result = tokio::task::block_in_place(|| {
            futures::executor::block_on(async move {
                client
                    .database("admin")
                    .run_command(
                        doc! { "configureFailPoint": self.failure_type.clone(), "mode": "off" },
                    )
                    .selection_criteria(self.selection_criteria.clone())
                    .await
            })
        });

        if let Err(error) = result {
            log_uncaptured(format!("failed disabling failpoint: {:?}", error));
        }
    }
}
