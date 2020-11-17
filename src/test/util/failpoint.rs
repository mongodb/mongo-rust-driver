use bson::{doc, Document};
use serde::{Deserialize, Serialize, Serializer};
use std::time::Duration;
use typed_builder::TypedBuilder;

use super::TestClient;
use crate::{
    error::Result,
    operation::append_options,
    options::{ReadPreference, SelectionCriteria},
    RUNTIME,
};

#[derive(Clone, Debug, Deserialize)]
pub struct FailPoint {
    #[serde(flatten)]
    command: Document,
}

impl FailPoint {
    fn name(&self) -> &str {
        self.command.get_str("configureFailPoint").unwrap()
    }

    /// Create a failCommand failpoint.
    /// See https://github.com/mongodb/mongo/wiki/The-%22failCommand%22-fail-point for more info.
    pub fn fail_command(
        fail_commands: &[&str],
        mode: FailPointMode,
        options: impl Into<Option<FailCommandOptions>>,
    ) -> FailPoint {
        let options = options.into();
        let mut data = doc! {
            "failCommands": fail_commands.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
        };
        append_options(&mut data, options.as_ref()).unwrap();

        let command = doc! {
            "configureFailPoint": "failCommand",
            "mode": bson::to_bson(&mode).unwrap(),
            "data": data,
        };
        FailPoint { command }
    }

    pub(super) async fn enable(self, client: &TestClient) -> Result<FailPointGuard> {
        let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
        client
            .database("admin")
            .run_command(self.command.clone(), selection_criteria)
            .await?;
        Ok(FailPointGuard {
            failpoint_name: self.name().to_string(),
            client: client.clone(),
        })
    }
}

pub struct FailPointGuard {
    client: TestClient,
    failpoint_name: String,
}

impl Drop for FailPointGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let name = self.failpoint_name.clone();

        let result = RUNTIME.block_on(async move {
            client
                .database("admin")
                .run_command(doc! { "configureFailPoint": name, "mode": "off" }, None)
                .await
        });

        if let Err(e) = result {
            println!("failed disabling failpoint: {:?}", e);
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(unused)]
pub enum FailPointMode {
    AlwaysOn,
    Times(i32),
    Off,
    ActivationProbability(f32),
}

#[derive(Debug, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailCommandOptions {
    /// If non-null, how long the server should block the affected commands.
    /// Only available in 4.2.9+.
    #[serde(serialize_with = "serialize_block_connection")]
    #[serde(flatten)]
    #[builder(default)]
    block_connection: Option<Duration>,

    /// Whether the server should hang up when the client sends an affected command
    #[builder(default)]
    close_connection: Option<bool>,

    /// The error code to include in the server's reply to an affected command.
    #[builder(default)]
    error_code: Option<i64>,

    /// Array of error labels to be included in the server's reply to an affected command. Passing
    /// in an empty array suppresses all error labels that would otherwise be returned by the
    /// server. The existence of the "errorLabels" field in the failCommand failpoint completely
    /// overrides the server's normal error labels adding behaviors for the affected commands.
    /// Only available in 4.4+.
    #[builder(default)]
    error_labels: Option<Vec<String>>,

    /// Document to be returned as a write concern error.
    #[builder(default)]
    write_concern_error: Option<Document>,
}

fn serialize_block_connection<S: Serializer>(
    val: &Option<Duration>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match val {
        Some(duration) => {
            (doc! { "blockConnection": true, "blockTimeMS": duration.as_millis() as i64})
                .serialize(serializer)
        }
        None => serializer.serialize_none(),
    }
}
