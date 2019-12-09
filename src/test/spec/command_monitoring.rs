use std::{convert::Into, ops::Deref, time::Duration};

use bson::{Bson, Document};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize,
    Deserializer,
};

use crate::{
    bson_util,
    error::Result,
    options::{FindOptions, Hint, InsertManyOptions, UpdateOptions},
    test::{assert_matches, parse_version, CommandEvent, EventClient, Matchable, CLIENT, LOCK},
    Collection,
};

#[derive(Deserialize)]
struct TestFile {
    data: Vec<Document>,
    collection_name: String,
    database_name: String,
    tests: Vec<TestCase>,
}

#[derive(Deserialize)]
struct TestCase {
    description: String,
    #[serde(rename = "ignore_if_server_version_greater_than", default)]
    max_version: Option<String>,
    #[serde(rename = "ignore_if_server_version_less_than", default)]
    min_version: Option<String>,
    operation: Document,
    expectations: Vec<TestEvent>,
}

trait TestOperation {
    /// The command names to monitor as part of this test.
    fn command_names(&self) -> &[&str];

    fn execute(&self, collection: Collection) -> Result<()>;
}

#[derive(Deserialize)]
struct AnyTestOperation<T: TestOperation> {
    #[serde(rename = "arguments")]
    operation: T,
}

impl<T: TestOperation> Deref for AnyTestOperation<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.operation
    }
}

fn run_command_monitoring_test<T: TestOperation + DeserializeOwned>(test_file_name: &str) {
    let test_file: TestFile = crate::test::spec::load_test(&["command-monitoring", test_file_name]);

    for test_case in test_file.tests {
        if let Some((major, minor)) = test_case.max_version.map(|s| parse_version(s.as_str())) {
            if CLIENT.server_version_gt(major, minor) {
                println!("Skipping {}", test_case.description);
                continue;
            }
        }

        if let Some((major, minor)) = test_case.min_version.map(|s| parse_version(s.as_str())) {
            if CLIENT.server_version_lt(major, minor) {
                println!("Skipping {}", test_case.description);
                continue;
            }
        }

        // We can't pass this test since it relies on old OP_QUERY behavior (SPEC-1519)
        if test_case.description.as_str()
            == "A successful find event with a getmore and the server kills the cursor"
        {
            continue;
        }

        let _guard = LOCK.run_exclusively();

        println!("Running {}", test_case.description);

        let operation: AnyTestOperation<T> =
            bson::from_bson(Bson::Document(test_case.operation)).unwrap();

        CLIENT
            .init_db_and_coll(&test_file.database_name, &test_file.collection_name)
            .insert_many(test_file.data.clone(), None)
            .expect("insert many error");

        let client = EventClient::new();

        let events: Vec<TestEvent> = client
            .run_operation_with_events(
                operation.command_names(),
                &test_file.database_name,
                &test_file.collection_name,
                |collection| {
                    let _ = operation.execute(collection);
                },
            )
            .into_iter()
            .map(Into::into)
            .collect();

        assert_eq!(events.len(), test_case.expectations.len());
        for (actual_event, expected_event) in events.iter().zip(test_case.expectations.iter()) {
            assert_matches(actual_event, expected_event);
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TestEvent {
    CommandStartedEvent {
        command_name: String,
        database_name: String,
        command: Document,
    },

    CommandSucceededEvent {
        command_name: String,
        reply: Document,
    },

    CommandFailedEvent {
        command_name: String,
    },
}

impl Matchable for TestEvent {
    fn content_matches(&self, actual: &TestEvent) -> bool {
        match (self, actual) {
            (
                TestEvent::CommandStartedEvent {
                    command_name: actual_command_name,
                    database_name: actual_database_name,
                    command: actual_command,
                },
                TestEvent::CommandStartedEvent {
                    command_name: expected_command_name,
                    database_name: expected_database_name,
                    command: expected_command,
                },
            ) => {
                actual_command_name == expected_command_name
                    && actual_database_name == expected_database_name
                    && actual_command.matches(expected_command)
            }
            (
                TestEvent::CommandSucceededEvent {
                    command_name: actual_command_name,
                    reply: actual_reply,
                },
                TestEvent::CommandSucceededEvent {
                    command_name: expected_command_name,
                    reply: expected_reply,
                },
            ) => {
                actual_command_name == expected_command_name && actual_reply.matches(expected_reply)
            }
            (
                TestEvent::CommandFailedEvent {
                    command_name: actual_command_name,
                },
                TestEvent::CommandFailedEvent {
                    command_name: expected_command_name,
                },
            ) => actual_command_name == expected_command_name,
            _ => false,
        }
    }
}

impl From<CommandEvent> for TestEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::CommandStartedEvent(event) => TestEvent::CommandStartedEvent {
                command_name: event.command_name,
                database_name: event.db,
                command: event.command,
            },
            CommandEvent::CommandFailedEvent(event) => TestEvent::CommandFailedEvent {
                command_name: event.command_name,
            },
            CommandEvent::CommandSucceededEvent(event) => TestEvent::CommandSucceededEvent {
                command_name: event.command_name,
                reply: event.reply,
            },
        }
    }
}

#[derive(Deserialize)]
struct DeleteMany {
    filter: Document,
}

impl TestOperation for DeleteMany {
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .delete_many(self.filter.clone(), None)
            .map(|_| ())
    }
}

#[test]
fn delete_many() {
    run_command_monitoring_test::<DeleteMany>("deleteMany.json")
}

#[derive(Deserialize)]
struct DeleteOne {
    filter: Document,
}

impl TestOperation for DeleteOne {
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        collection.delete_one(self.filter.clone(), None).map(|_| ())
    }
}

#[test]
fn delete_one() {
    run_command_monitoring_test::<DeleteOne>("deleteOne.json")
}

fn deserialize_i64_from_ext_json<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let document = Option::<Document>::deserialize(deserializer)?;
    match document {
        Some(document) => {
            let number_string = document
                .get("$numberLong")
                .and_then(Bson::as_str)
                .ok_or_else(|| de::Error::custom("missing $numberLong field"))?;
            let parsed = number_string
                .parse::<i64>()
                .map_err(|_| de::Error::custom("failed to parse to i64"))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

// This struct is necessary because the command monitoring tests specify the options in a very old
// way (SPEC-1519).
#[derive(Debug, Deserialize, Default)]
struct FindModifiers {
    #[serde(rename = "$comment", default)]
    comment: Option<String>,
    #[serde(rename = "$hint", default)]
    hint: Option<Hint>,
    #[serde(
        rename = "$maxTimeMS",
        deserialize_with = "bson_util::deserialize_duration_from_u64_millis",
        default
    )]
    max_time: Option<Duration>,
    #[serde(rename = "$min", default)]
    min: Option<Document>,
    #[serde(rename = "$max", default)]
    max: Option<Document>,
    #[serde(rename = "$returnKey", default)]
    return_key: Option<bool>,
    #[serde(rename = "$showDiskLoc", default)]
    show_disk_loc: Option<bool>,
}

impl FindModifiers {
    fn update_options(&self, options: &mut FindOptions) {
        options.comment = self.comment.clone();
        options.hint = self.hint.clone();
        options.max_time = self.max_time;
        options.min = self.min.clone();
        options.max = self.max.clone();
        options.return_key = self.return_key;
        options.show_record_id = self.show_disk_loc;
    }
}

#[derive(Debug, Default, Deserialize)]
struct Find {
    filter: Option<Document>,
    #[serde(default)]
    sort: Option<Document>,
    #[serde(default, deserialize_with = "deserialize_i64_from_ext_json")]
    skip: Option<i64>,
    #[serde(
        default,
        rename = "batchSize",
        deserialize_with = "deserialize_i64_from_ext_json"
    )]
    batch_size: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_i64_from_ext_json")]
    limit: Option<i64>,
    #[serde(default)]
    modifiers: Option<FindModifiers>,
}

impl TestOperation for Find {
    fn command_names(&self) -> &[&str] {
        &["find", "getMore"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        let mut options = FindOptions {
            sort: self.sort.clone(),
            skip: self.skip,
            batch_size: self.batch_size.map(|i| i as u32),
            limit: self.limit,
            ..Default::default()
        };

        if let Some(ref modifiers) = self.modifiers {
            modifiers.update_options(&mut options);
        }

        collection.find(self.filter.clone(), options).map(
            |mut cursor| {
                while cursor.next().is_some() {}
            },
        )
    }
}

#[test]
fn find() {
    run_command_monitoring_test::<Find>("find.json")
}

#[derive(Debug, Deserialize)]
struct InsertMany {
    documents: Vec<Document>,
    #[serde(default)]
    options: Option<InsertManyOptions>,
}

impl TestOperation for InsertMany {
    fn command_names(&self) -> &[&str] {
        &["insert"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .insert_many(self.documents.clone(), self.options.clone())
            .map(|_| ())
    }
}

#[test]
fn insert_many() {
    run_command_monitoring_test::<InsertMany>("insertMany.json")
}

#[derive(Debug, Deserialize)]
struct InsertOne {
    document: Document,
}

impl TestOperation for InsertOne {
    fn command_names(&self) -> &[&str] {
        &["insert"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .insert_one(self.document.clone(), None)
            .map(|_| ())
    }
}

#[test]
fn insert_one() {
    run_command_monitoring_test::<InsertOne>("insertOne.json")
}

#[derive(Debug, Deserialize)]
struct UpdateMany {
    filter: Document,
    update: Document,
}

impl TestOperation for UpdateMany {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .update_many(self.filter.clone(), self.update.clone(), None)
            .map(|_| ())
    }
}

#[test]
fn update_many() {
    run_command_monitoring_test::<UpdateMany>("updateMany.json")
}

#[derive(Debug, Deserialize)]
struct UpdateOne {
    filter: Document,
    update: Document,
    #[serde(default)]
    upsert: Option<bool>,
}

impl TestOperation for UpdateOne {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    fn execute(&self, collection: Collection) -> Result<()> {
        let options = self.upsert.map(|b| UpdateOptions {
            upsert: Some(b),
            ..Default::default()
        });
        collection
            .update_one(self.filter.clone(), self.update.clone(), options)
            .map(|_| ())
    }
}

#[test]
fn update_one() {
    run_command_monitoring_test::<UpdateOne>("updateOne.json")
}
