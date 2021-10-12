use bson::Document;

use crate::{
    bson::{doc, spec::ElementType, Bson},
    bson_util::get_int,
    test::{CmapEvent, CommandEvent, Event},
};

use super::{EntityMap, ExpectedCmapEvent, ExpectedCommandEvent, ExpectedEvent};

pub fn results_match(
    actual: Option<&Bson>,
    expected: &Bson,
    returns_root_documents: bool,
    entities: Option<&EntityMap>,
) -> bool {
    results_match_inner(actual, expected, returns_root_documents, true, entities)
}

pub fn events_match(
    actual: &Event,
    expected: &ExpectedEvent,
    entities: Option<&EntityMap>,
) -> bool {
    match (actual, expected) {
        (Event::Command(act), ExpectedEvent::Command(exp)) => {
            command_events_match(act, exp, entities)
        }
        (Event::Cmap(act), ExpectedEvent::Cmap(exp)) => cmap_events_match(act, exp),
        _ => false,
    }
}

fn match_opt<T: PartialEq>(actual: &T, expected: &Option<T>) -> bool {
    expected.is_none() || expected.as_ref() == Some(actual)
}

fn match_results_opt(
    actual: &Document,
    expected: &Option<Document>,
    entities: Option<&EntityMap>,
) -> bool {
    let expected_doc = if let Some(doc) = expected {
        Bson::Document(doc.clone())
    } else {
        return true;
    };
    let actual_doc = Some(Bson::Document(actual.clone()));
    results_match(actual_doc.as_ref(), &expected_doc, false, entities)
}

fn command_events_match(
    actual: &CommandEvent,
    expected: &ExpectedCommandEvent,
    entities: Option<&EntityMap>,
) -> bool {
    match (actual, expected) {
        (
            CommandEvent::Started(actual),
            ExpectedCommandEvent::Started {
                command_name: expected_command_name,
                database_name: expected_database_name,
                command: expected_command,
                has_service_id: expected_has_service_id,
            },
        ) => {
            if !match_opt(&actual.command_name, expected_command_name) {
                return false;
            }
            if !match_opt(&actual.db, expected_database_name) {
                return false;
            }
            if !match_opt(&actual.service_id.is_some(), expected_has_service_id) {
                return false;
            }
            if !match_results_opt(&actual.command, expected_command, entities) {
                return false;
            }
            true
        }
        (
            CommandEvent::Succeeded(actual),
            ExpectedCommandEvent::Succeeded {
                command_name: expected_command_name,
                reply: expected_reply,
                has_service_id: expected_has_service_id,
            },
        ) => {
            if !match_opt(&actual.command_name, expected_command_name) {
                return false;
            }
            if !match_opt(&actual.service_id.is_some(), expected_has_service_id) {
                return false;
            }
            if !match_results_opt(&actual.reply, expected_reply, None) {
                return false;
            }
            true
        }
        (
            CommandEvent::Failed(actual),
            ExpectedCommandEvent::Failed {
                command_name: expected_command_name,
                has_service_id: expected_has_service_id,
            },
        ) => {
            if !match_opt(&actual.service_id.is_some(), expected_has_service_id) {
                return false;
            }
            if !match_opt(&actual.command_name, expected_command_name) {
                return false;
            }
            true
        }
        _ => false,
    }
}

fn cmap_events_match(actual: &CmapEvent, expected: &ExpectedCmapEvent) -> bool {
    match (actual, expected) {
        (CmapEvent::PoolCreated(_), ExpectedCmapEvent::PoolCreated {}) => true,
        (CmapEvent::PoolReady(_), ExpectedCmapEvent::PoolReady {}) => true,
        (
            CmapEvent::PoolCleared(actual),
            ExpectedCmapEvent::PoolCleared {
                has_service_id: expected_has_service_id,
            },
        ) => match_opt(&actual.service_id.is_some(), expected_has_service_id),
        (CmapEvent::PoolClosed(_), ExpectedCmapEvent::PoolClosed {}) => true,
        (CmapEvent::ConnectionCreated(_), ExpectedCmapEvent::ConnectionCreated {}) => true,
        (CmapEvent::ConnectionReady(_), ExpectedCmapEvent::ConnectionReady {}) => true,
        (
            CmapEvent::ConnectionClosed(actual),
            ExpectedCmapEvent::ConnectionClosed {
                reason: expected_reason,
            },
        ) => match_opt(&actual.reason, expected_reason),
        (
            CmapEvent::ConnectionCheckOutStarted(_),
            ExpectedCmapEvent::ConnectionCheckOutStarted {},
        ) => true,
        (
            CmapEvent::ConnectionCheckOutFailed(actual),
            ExpectedCmapEvent::ConnectionCheckOutFailed {
                reason: expected_reason,
            },
        ) => match_opt(&actual.reason, expected_reason),
        (CmapEvent::ConnectionCheckedOut(_), ExpectedCmapEvent::ConnectionCheckedOut {}) => true,
        (CmapEvent::ConnectionCheckedIn(_), ExpectedCmapEvent::ConnectionCheckedIn {}) => true,
        _ => false,
    }
}

fn results_match_inner(
    actual: Option<&Bson>,
    expected: &Bson,
    returns_root_documents: bool,
    root: bool,
    entities: Option<&EntityMap>,
) -> bool {
    match expected {
        Bson::Document(expected_doc) => {
            if let Some((key, value)) = expected_doc.iter().next() {
                if key.starts_with("$$") && expected_doc.len() == 1 {
                    return special_operator_matches((key, value), actual, entities);
                }
            }

            let actual_doc = match actual {
                Some(Bson::Document(actual)) => actual,
                // The only case in which None is an acceptable value is if the expected document
                // is a special operator; otherwise, the two documents do not match.
                _ => return false,
            };

            for (key, value) in expected_doc {
                if key == "upsertedCount" {
                    continue;
                }
                if !results_match_inner(actual_doc.get(key), value, false, false, entities) {
                    return false;
                }
            }

            // Documents that are not the root-level document should not contain extra keys.
            if !root {
                for (key, _) in actual_doc {
                    if !expected_doc.contains_key(key) {
                        return false;
                    }
                }
            }

            true
        }
        Bson::Array(expected_array) => {
            let actual_array = actual.unwrap().as_array().unwrap();
            if expected_array.len() != actual_array.len() {
                return false;
            }

            // Some operations return an array of documents that should be treated as root
            // documents.
            for (actual, expected) in actual_array.iter().zip(expected_array) {
                if !results_match_inner(
                    Some(actual),
                    expected,
                    false,
                    returns_root_documents,
                    entities,
                ) {
                    return false;
                }
            }

            true
        }
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => match actual {
            Some(actual) => numbers_match(actual, expected),
            None => false,
        },
        _ => match actual {
            Some(actual) => actual == expected,
            None => false,
        },
    }
}

fn numbers_match(actual: &Bson, expected: &Bson) -> bool {
    if actual.element_type() == expected.element_type() {
        return actual == expected;
    }

    match (get_int(actual), get_int(expected)) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => false,
    }
}

fn special_operator_matches(
    (key, value): (&String, &Bson),
    actual: Option<&Bson>,
    entities: Option<&EntityMap>,
) -> bool {
    match key.as_ref() {
        "$$exists" => value.as_bool().unwrap() == actual.is_some(),
        "$$type" => type_matches(value, actual.unwrap()),
        "$$unsetOrMatches" => {
            if actual.is_some() {
                results_match_inner(actual, value, false, false, entities)
            } else {
                true
            }
        }
        "$$matchesEntity" => {
            let id = value.as_str().unwrap();
            entity_matches(id, actual, entities.unwrap())
        }
        "$$matchesHexBytes" => panic!("GridFS not implemented"),
        "$$sessionLsid" => match entities {
            Some(entity_map) => {
                let session_id = value.as_str().unwrap();
                let session = entity_map.get(session_id).unwrap().as_session_entity();
                results_match_inner(
                    actual,
                    &Bson::from(session.lsid.clone()),
                    false,
                    false,
                    entities,
                )
            }
            None => panic!("Could not find entity: {}", value),
        },
        other => panic!("unknown special operator: {}", other),
    }
}

fn entity_matches(id: &str, actual: Option<&Bson>, entities: &EntityMap) -> bool {
    let bson = entities.get(id).unwrap().as_bson();
    results_match_inner(actual, bson, false, false, Some(entities))
}

fn type_matches(types: &Bson, actual: &Bson) -> bool {
    match types {
        Bson::Array(types) => types.iter().any(|t| type_matches(t, actual)),
        Bson::String(str) => match str.as_ref() {
            "double" => actual.element_type() == ElementType::Double,
            "string" => actual.element_type() == ElementType::String,
            "object" => actual.element_type() == ElementType::EmbeddedDocument,
            "array" => actual.element_type() == ElementType::Array,
            "binData" => actual.element_type() == ElementType::Binary,
            "undefined" => actual.element_type() == ElementType::Undefined,
            "objectId" => actual.element_type() == ElementType::ObjectId,
            "bool" => actual.element_type() == ElementType::Boolean,
            "date" => actual.element_type() == ElementType::DateTime,
            "null" => actual.element_type() == ElementType::Null,
            "regex" => actual.element_type() == ElementType::RegularExpression,
            "dbPointer" => actual.element_type() == ElementType::DbPointer,
            "javascript" => actual.element_type() == ElementType::JavaScriptCode,
            "symbol" => actual.element_type() == ElementType::Symbol,
            "javascriptWithScope" => actual.element_type() == ElementType::JavaScriptCodeWithScope,
            "int" => actual.element_type() == ElementType::Int32,
            "timestamp" => actual.element_type() == ElementType::Timestamp,
            "long" => actual.element_type() == ElementType::Int64,
            "decimal" => actual.element_type() == ElementType::Decimal128,
            "minKey" => actual.element_type() == ElementType::MinKey,
            "maxKey" => actual.element_type() == ElementType::MaxKey,
            other => panic!("unrecognized type: {}", other),
        },
        other => panic!("unrecognized type: {}", other),
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn basic_matching() {
    let actual = doc! { "x": 1, "y": 1 };
    let expected = doc! { "x": 1 };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": 1, "y": 1 };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn array_matching() {
    let mut actual: Vec<Bson> = Vec::new();
    for i in 1..4 {
        actual.push(Bson::Int32(i));
    }
    let mut expected: Vec<Bson> = Vec::new();
    for i in 1..3 {
        expected.push(Bson::Int32(i));
    }
    assert!(!results_match(
        Some(&Bson::Array(actual)),
        &Bson::Array(expected),
        false,
        None,
    ));

    let actual = vec![
        Bson::Document(doc! { "x": 1, "y": 1 }),
        Bson::Document(doc! { "x": 2, "y": 2 }),
    ];
    let expected = vec![
        Bson::Document(doc! { "x": 1 }),
        Bson::Document(doc! { "x": 2 }),
    ];
    assert!(!results_match(
        Some(&Bson::Array(actual)),
        &Bson::Array(expected),
        false,
        None,
    ));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn special_operators() {
    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$exists": true } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$exists": false } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "y": { "$$exists": false } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "y": { "$$exists": true } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$type": [ "int", "long" ] } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! {};
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "x": 2 };
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let expected = doc! { "x": { "y": { "$$exists": false } } };
    let actual = doc! { "x": {} };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn extra_fields() {
    let actual = doc! { "x": 1, "y": 2 };
    let expected = doc! { "x": 1 };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));

    let actual = doc! { "doc": { "x": 1, "y": 2 } };
    let expected = doc! { "doc": { "x": 1 } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    ));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn numbers() {
    let actual = Bson::Int32(2);
    let expected = Bson::Int64(2);
    assert!(results_match(Some(&actual), &expected, false, None));

    let actual = Bson::Double(2.5);
    let expected = Bson::Int32(2);
    assert!(!results_match(Some(&actual), &expected, false, None));

    let actual = Bson::Double(2.0);
    let expected = Bson::Int64(2);
    assert!(results_match(Some(&actual), &expected, false, None));
}
