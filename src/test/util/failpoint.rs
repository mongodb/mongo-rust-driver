use bson::{doc, Document};
use serde::{Deserialize, Serialize, Serializer};
use std::time::Duration;
use typed_builder::TypedBuilder;

use crate::{
    action::Action,
    error::Result,
    operation::append_options,
    selection_criteria::SelectionCriteria,
    Client,
};

// If you write a tokio test that uses this, make sure to annotate it with
// tokio::test(flavor = "multi_thread").
// TODO RUST-1530 Make the error message here better.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FailPoint {
    #[serde(flatten)]
    command: Document,
}

impl FailPoint {
    fn name(&self) -> &str {
        self.command.get_str("configureFailPoint").unwrap()
    }

    /// Create a failCommand failpoint.
    /// See <https://github.com/mongodb/mongo/wiki/The-%22failCommand%22-fail-point> for more info.
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

    pub async fn enable(
        &self,
        client: &Client,
        criteria: impl Into<Option<SelectionCriteria>>,
    ) -> Result<FailPointGuard> {
        let criteria = criteria.into();
        client
            .database("admin")
            .run_command(self.command.clone())
            .optional(criteria.clone(), |a, c| a.selection_criteria(c))
            .await?;
        Ok(FailPointGuard {
            failpoint_name: self.name().to_string(),
            client: client.clone(),
            criteria,
        })
    }
}

#[derive(Debug)]
pub struct FailPointGuard {
    client: Client,
    failpoint_name: String,
    criteria: Option<SelectionCriteria>,
}

impl Drop for FailPointGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let name = self.failpoint_name.clone();

        // This forces the Tokio runtime to not finish shutdown until this future has completed.
        // Unfortunately, this also means that tests using FailPointGuards have to use the
        // multi-threaded runtime.
        let result = tokio::task::block_in_place(|| {
            futures::executor::block_on(async move {
                client
                    .database("admin")
                    .run_command(doc! { "configureFailPoint": name, "mode": "off" })
                    .optional(self.criteria.clone(), |a, c| a.selection_criteria(c))
                    .await
            })
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
    Skip(i32),
    Off,
    ActivationProbability(f32),
}

#[serde_with::skip_serializing_none]
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
pub struct FailCommandOptions {
    /// The appName that a client must use in order to hit this fail point.
    app_name: Option<String>,

    /// If non-null, how long the server should block the affected commands.
    /// Only available in 4.2.9+.
    #[serde(serialize_with = "serialize_block_connection")]
    #[serde(flatten)]
    block_connection: Option<Duration>,

    /// Whether the server should hang up when the client sends an affected command
    close_connection: Option<bool>,

    /// The error code to include in the server's reply to an affected command.
    error_code: Option<i64>,

    /// Array of error labels to be included in the server's reply to an affected command. Passing
    /// in an empty array suppresses all error labels that would otherwise be returned by the
    /// server. The existence of the "errorLabels" field in the failCommand failpoint completely
    /// overrides the server's normal error labels adding behaviors for the affected commands.
    /// Only available in 4.4+.
    error_labels: Option<Vec<String>>,

    /// Document to be returned as a write concern error.
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
