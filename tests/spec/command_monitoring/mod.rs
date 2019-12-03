use std::time::Duration;

use bson::{bson, doc, Bson, Document};
use serde::Deserialize;

use mongodb::{
    options::{FindOptions, Hint, InsertManyOptions},
    Collection,
};

use crate::{util::EventClient, CLIENT};

#[derive(Debug, Deserialize)]
struct TestFile {
    pub data: Vec<Document>,
    pub collection_name: String,
    pub database_name: String,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestCase {
    pub description: String,
    pub operation: Operation,
    pub expectations: Vec<Expectation>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    name: String,
    arguments: Document,
}

#[derive(Debug, Deserialize)]
struct Expectation {
    command_started_event: Option<Document>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct UpdateCommand {
    update: String,
    ordered: bool,
    updates: Vec<Updates>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct InsertCommand {
    insert: String,
    documents: Vec<Document>,
    ordered: bool,
}

#[derive(Debug, Deserialize, PartialEq)]
struct DeleteCommand {
    delete: String,
    deletes: Vec<Deletes>,
    ordered: bool,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Deletes {
    q: Document,
    limit: i32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Updates {
    q: Document,
    u: Document,
}

#[derive(Debug, Deserialize)]
struct FindArguments {
    filter: Option<Document>,
    sort: Option<Document>,
    skip: Option<NumberLong>,
    #[serde(rename = "batchSize")]
    batch_size: Option<NumberLong>,
    limit: Option<NumberLong>,
    modifiers: Option<Modifiers>,
}

#[derive(Debug, Deserialize)]
struct NumberLong {
    #[serde(rename = "$numberLong")]
    number_long: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Modifiers {
    #[serde(rename = "$comment")]
    comment: Option<String>,
    #[serde(rename = "$hint")]
    hint: Option<Document>,
    #[serde(rename = "$maxTimeMS")]
    max_time_ms: Option<i64>,
    #[serde(rename = "$min")]
    min: Option<Document>,
    #[serde(rename = "$max")]
    max: Option<Document>,
    #[serde(rename = "$returnKey")]
    return_key: Option<bool>,
    #[serde(rename = "$showDiskLoc")]
    show_disk_loc: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct UpdateArguments {
    filter: Document,
    update: Document,
}

fn get_command_name(operation_name: &str) -> &[&str] {
    match &operation_name.to_lowercase()[..] {
        "deletemany" => &["delete"],

        "deleteone" => &["delete"],

        "find" => &["find", "getMore"],

        "insertmany" => &["insert"],

        "insertone" => &["insert"],

        "updatemany" => &["update"],

        "updateone" => &["update"],

        s => panic!("unexpected command! Received {}", s),
    }
}

#[allow(unused_must_use)]
fn run_command(collection: Collection, name: &str, mut arguments: Document) {
    match &name.to_lowercase()[..] {
        "deletemany" => {
            if let Some(filter) = arguments.remove("filter") {
                let filter: Document = bson::from_bson(filter).unwrap();
                collection.delete_many(filter, None);
            }
        }
        "deleteone" => {
            if let Some(filter) = arguments.remove("filter") {
                let filter: Document = bson::from_bson(filter).unwrap();
                collection.delete_one(filter, None);
            }
        }
        "find" => {
            let args: FindArguments = bson::from_bson(Bson::Document(arguments)).unwrap();

            let mut options = FindOptions::builder().sort(args.sort).build();

            if let Some(skip) = args.skip {
                options.skip = if let Some(s) = skip.number_long {
                    Some(s.parse::<i64>().unwrap())
                } else {
                    None
                };
            }

            if let Some(batch_size) = args.batch_size {
                options.batch_size = if let Some(s) = batch_size.number_long {
                    Some(s.parse::<u32>().unwrap())
                } else {
                    None
                };
            }

            if let Some(limit) = args.limit {
                options.limit = if let Some(s) = limit.number_long {
                    Some(s.parse::<i64>().unwrap())
                } else {
                    None
                };
            }

            if let Some(modifiers) = args.modifiers {
                options.comment = modifiers.comment;
                options.hint = if let Some(hint) = modifiers.hint {
                    Some(Hint::Keys(hint))
                } else {
                    None
                };
                options.max_time = if let Some(i) = modifiers.max_time_ms {
                    Some(Duration::from_millis(i as u64))
                } else {
                    None
                };
                options.min = modifiers.min;
                options.max = modifiers.max;
                options.return_key = modifiers.return_key;
                options.show_record_id = modifiers.show_disk_loc;
            }

            let cursor = collection.find(args.filter, Some(options));
            if let Ok(mut cursor) = cursor {
                let mut result = cursor.next();
                while result.is_some() {
                    result = cursor.next();
                }
            }
        }
        "insertmany" => {
            if let Some(docs) = arguments.remove("documents") {
                let docs: Vec<Document> = bson::from_bson(docs).unwrap();
                let mut options: Option<InsertManyOptions> = None;
                if let Some(opts) = arguments.remove("options") {
                    options = bson::from_bson(opts).unwrap();
                }
                collection.insert_many(docs, options);
            }
        }
        "insertone" => {
            if let Some(doc) = arguments.remove("document") {
                let doc: Document = bson::from_bson(doc).unwrap();
                collection.insert_one(doc, None);
            }
        }
        "updatemany" => {
            let args: UpdateArguments = bson::from_bson(Bson::Document(arguments)).unwrap();
            collection.update_many(args.filter, args.update, None);
        }
        "updateone" => {
            let args: UpdateArguments = bson::from_bson(Bson::Document(arguments)).unwrap();
            collection.update_one(args.filter, args.update, None);
        }
        s => {
            panic!("unexpected command! Received {}", s);
        }
    }
}

fn run_test(test_file: TestFile) {
    for test_case in test_file.tests {
        let collection =
            CLIENT.init_db_and_coll(&test_file.database_name, &test_file.collection_name);

        collection
            .insert_many(test_file.data.clone(), None)
            .expect("insert many error");
        // bulk write not implemented and kill cursor not supported
        if test_case.description.contains("bulk write")
            || test_case.description.contains("killcursors")
            || test_case.description.contains("kills the cursor")
        {
            continue;
        }
        let name = test_case.operation.name.as_str();
        // count is deprecated
        if name == "count" {
            continue;
        }

        let arguments = test_case.operation.arguments;
        let client = EventClient::new();

        let command_name = get_command_name(name);

        let events = client.run_operation_with_events(
            command_name,
            &test_file.database_name,
            &test_file.collection_name,
            |collection| {
                run_command(collection, name, arguments);
            },
        );

        let mut i = 0;
        for expectation in test_case.expectations {
            if let Some(expected_command_started_event) = expectation.command_started_event.clone()
            {
                let event = &events[i];

                let expected_command_name: String = bson::from_bson(
                    expected_command_started_event
                        .get("command_name")
                        .unwrap()
                        .clone(),
                )
                .unwrap();

                assert_eq!(event.command_name, expected_command_name);

                let expected_database_name: String = bson::from_bson(
                    expected_command_started_event
                        .get("database_name")
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                assert_eq!(event.db, expected_database_name);

                match expected_command_name.as_str() {
                    "delete" => {
                        let expected_command: DeleteCommand = bson::from_bson(
                            expected_command_started_event
                                .get("command")
                                .unwrap()
                                .clone(),
                        )
                        .expect("expected command won't convert");

                        let mut command = event.command.clone();
                        if !command.contains_key("ordered") {
                            command.insert("ordered", true);
                        }

                        let actual_command: DeleteCommand =
                            bson::from_bson(Bson::Document(command.clone()))
                                .expect("actual command won't convert");

                        assert_eq!(actual_command, expected_command);
                    }
                    "find" => {
                        let mut expected_command: Document = bson::from_bson(
                            expected_command_started_event
                                .get("command")
                                .unwrap()
                                .clone(),
                        )
                        .expect("expected command won't convert");

                        let mut command = event.command.clone();

                        if let Some(skip) = command.remove("skip") {
                            command.insert("skip", doc! {"$numberLong": skip.to_string()});
                        }

                        if let Some(batch_size) = command.remove("batchSize") {
                            command
                                .insert("batchSize", doc! {"$numberLong": batch_size.to_string()});
                        }

                        if let Some(limit) = command.remove("limit") {
                            command.insert("limit", doc! {"$numberLong": limit.to_string()});
                        }

                        crate::sort_document(&mut command);
                        crate::sort_document(&mut expected_command);

                        assert_eq!(command, expected_command);
                    }
                    "insert" => {
                        let expected_command: InsertCommand = bson::from_bson(
                            expected_command_started_event
                                .get("command")
                                .unwrap()
                                .clone(),
                        )
                        .expect("expected command won't convert");

                        let mut command = event.command.clone();
                        if !command.contains_key("ordered") {
                            command.insert("ordered", true);
                        }

                        let actual_command: InsertCommand =
                            bson::from_bson(Bson::Document(command.clone()))
                                .expect("actual command won't convert");

                        assert_eq!(actual_command, expected_command);
                    }
                    "update" => {
                        let expected_command: UpdateCommand = bson::from_bson(
                            expected_command_started_event
                                .get("command")
                                .unwrap()
                                .clone(),
                        )
                        .expect("expected command won't convert");

                        let mut command = event.command.clone();
                        if !command.contains_key("ordered") {
                            command.insert("ordered", true);
                        }

                        let actual_command: UpdateCommand =
                            bson::from_bson(Bson::Document(command.clone()))
                                .expect("actual command won't convert");

                        assert_eq!(actual_command, expected_command);
                    }

                    "getMore" => {
                        let mut expected_command: Document = bson::from_bson(
                            expected_command_started_event
                                .get("command")
                                .unwrap()
                                .clone(),
                        )
                        .expect("expected command won't convert");

                        let mut command = event.command.clone();

                        if let Some(batch_size) = command.remove("batchSize") {
                            command.insert(
                                "batchSize",
                                doc! {"$numberLong":
                                batch_size.to_string()},
                            );
                        }

                        if let Some(doc) = expected_command.remove("getMore") {
                            if doc
                                .as_document()
                                .unwrap()
                                .get("$numberLong")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                == "42"
                            {
                                let id = command.remove("getMore").unwrap().as_i64().unwrap();
                                assert!(id > 0);
                            } else {
                                expected_command.insert("getMore", doc);
                            }
                        }

                        crate::sort_document(&mut command);
                        crate::sort_document(&mut expected_command);

                        assert_eq!(command, expected_command);
                    }
                    s => {
                        panic!("this case should not occur. Received {}", s);
                    }
                }
                i += 1;
            }
        }
    }
}

#[test]
fn run() {
    crate::spec::test(&["command-monitoring"], run_test);
}
