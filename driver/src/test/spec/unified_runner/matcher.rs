use std::fmt::Debug;

use crate::{
    bson::{doc, spec::ElementType, Bson, Document},
    bson_util::{get_double, get_int},
    event::{
        cmap::CmapEvent,
        command::CommandEvent,
        sdam::{SdamEvent, ServerDescription},
    },
    test::Event,
};

use super::{
    test_event::{ExpectedSdamEvent, TestServerDescription},
    EntityMap,
    ExpectedCmapEvent,
    ExpectedCommandEvent,
    ExpectedEvent,
};

#[cfg(feature = "tracing-unstable")]
use super::test_file::ExpectedMessage;
#[cfg(feature = "tracing-unstable")]
use crate::test::util::{TracingEvent, TracingEventValue};

#[cfg(feature = "tracing-unstable")]
fn mismatch_message(kind: &str, actual: impl Debug, expected: impl Debug) -> String {
    format!("{kind} do not match. Actual:\n{actual:?}\nExpected:\n{expected:?}")
}

use std::convert::TryInto;

pub(crate) fn results_match(
    actual: Option<&Bson>,
    expected: &Bson,
    returns_root_documents: bool,
    entities: Option<&EntityMap>,
) -> Result<(), String> {
    results_match_inner(actual, expected, returns_root_documents, true, entities)
}

pub(crate) fn events_match(
    actual: &Event,
    expected: &ExpectedEvent,
    entities: Option<&EntityMap>,
) -> Result<(), String> {
    match (actual, expected) {
        (Event::Command(act), ExpectedEvent::Command(exp)) => {
            command_events_match(act, exp, entities)
        }
        (Event::Cmap(act), ExpectedEvent::Cmap(exp)) => cmap_events_match(act, exp),
        (Event::Sdam(act), ExpectedEvent::Sdam(exp)) => sdam_events_match(act, exp),
        _ => expected_err(actual, expected),
    }
}

#[cfg(feature = "tracing-unstable")]
pub(crate) fn tracing_events_match(
    actual: &TracingEvent,
    expected: &ExpectedMessage,
) -> Result<(), String> {
    if let Some(ref target) = expected.target {
        if &actual.target != target {
            return Err(mismatch_message(
                "tracing event components",
                &actual.target,
                target,
            ));
        }
    }
    if let Some(level) = expected.level {
        if actual.level != level {
            return Err(mismatch_message(
                "tracing event levels",
                actual.level,
                level,
            ));
        }
    }

    use regex::Regex;
    use std::sync::LazyLock;

    if let Some(failure_should_be_redacted) = expected.failure_is_redacted {
        match actual.fields.get("failure") {
            Some(failure) => {
                match failure {
                    TracingEventValue::String(failure_str) => {
                        // `Lazy` saves us having to recompile this regex every time this
                        // function is called.

                        static COMMAND_FAILED_REGEX: LazyLock<Regex> = LazyLock::new(|| {
                            Regex::new(
                                r"^Kind: Command failed: Error code (?P<code>\d+) \((?P<codename>.+)\): (?P<message>.+)+, labels: (?P<labels>.+)$"
                            ).unwrap()
                        });

                        static IO_ERROR_REGEX: LazyLock<Regex> = LazyLock::new(|| {
                            Regex::new(
                                r"^Kind: I/O error: (?P<message>.+), labels: (?P<labels>.+)$",
                            )
                            .unwrap()
                        });

                        // We redact all server-returned errors, however at this time the only types
                        // of errors that show up in tracing redaction tests
                        // are command errors (which should be redacted) and
                        // I/O errors (which should not redacted). redaction of other errors is
                        // unit-tested in src/test/spec/trace.rs.
                        if let Some(captures) = COMMAND_FAILED_REGEX.captures(failure_str) {
                            let code = captures.name("code").unwrap().as_str();
                            // code should never be redacted (here we consider "present and
                            // non-zero" to mean unredacted)
                            match code.parse::<u32>() {
                                Ok(code) => {
                                    if code == 0 {
                                        return Err(format! {
                                            "Expected a non-zero error code, but got {code}",
                                        });
                                    }
                                }
                                Err(err) => {
                                    return Err(format! {
                                        "Expected error code {code} to be parseable to a u32 but was not; got error {err:?}",
                                    })
                                }
                            }

                            let codename = captures.name("codename").unwrap().as_str();
                            // codename should never be redacted (here we consider "non-empty and
                            // does not contain `REDACTED`" to mean
                            // unredacted)
                            if codename.is_empty() {
                                return Err(format! {
                                    "Expected a non-empty error codename, but got {codename}",
                                });
                            }

                            if codename.contains("REDACTED") {
                                return Err(format! {
                                    "Expected error codename to not be redacted, but got {codename}",
                                });
                            }

                            // note: we can't really assert on error labels being redacted here
                            // because they are not always present and
                            // so a non-redacted error can have empty
                            // error labels.

                            let errmsg = captures.name("message").unwrap().as_str();

                            if failure_should_be_redacted && errmsg != "REDACTED" {
                                return Err(format! {
                                    "Expected command error message to be redacted, but was not; got message {errmsg}",
                                });
                            } else if !failure_should_be_redacted && errmsg.contains("REDACTED") {
                                return Err(format! {
                                    "Expected command error message to not be redacted, but was; got message {errmsg}",
                                });
                            }
                        } else if let Some(captures) = IO_ERROR_REGEX.captures(failure_str) {
                            let message = captures.name("message").unwrap().as_str();
                            if message.is_empty() || message.contains("REDACTED") {
                                return Err(format! {
                                    "Expected I/O error message to not be redacted, but was; got message {message}",
                                });
                            }
                        }
                    }
                    _ => {
                        return Err(format!(
                            "Expected failure to be a string, but was not; got {failure:?}"
                        ))
                    }
                };
            }
            None => {
                return Err("Expected event to contain a failure, but did not find one".to_string());
            }
        };
    }

    let serialized_fields = crate::bson_compat::serialize_to_document(&actual.fields)
        .map_err(|e| format!("Failed to serialize tracing fields to document: {e}"))?;

    results_match(
        Some(&Bson::Document(serialized_fields)),
        &Bson::Document(expected.data.clone()),
        false,
        None,
    )
}

fn match_opt<T: PartialEq + std::fmt::Debug>(
    actual: &T,
    expected: &Option<T>,
) -> Result<(), String> {
    match expected.as_ref() {
        None => Ok(()),
        Some(exp) => match_eq(actual, exp),
    }
}

fn match_results_opt(
    actual: &Document,
    expected: &Option<Document>,
    entities: Option<&EntityMap>,
) -> Result<(), String> {
    let expected_doc = if let Some(doc) = expected {
        Bson::Document(doc.clone())
    } else {
        return Ok(());
    };
    let actual_doc = Some(Bson::Document(actual.clone()));
    results_match(actual_doc.as_ref(), &expected_doc, false, entities)
}

fn command_events_match(
    actual: &CommandEvent,
    expected: &ExpectedCommandEvent,
    entities: Option<&EntityMap>,
) -> Result<(), String> {
    match (actual, expected) {
        (
            CommandEvent::Started(actual),
            ExpectedCommandEvent::Started {
                command_name: expected_command_name,
                database_name: expected_database_name,
                command: expected_command,
                has_service_id: expected_has_service_id,
                has_server_connection_id: expected_has_server_connection_id,
            },
        ) => {
            match_opt(&actual.command_name, expected_command_name)?;
            match_opt(&actual.db, expected_database_name)?;
            match_opt(&actual.service_id.is_some(), expected_has_service_id)?;
            match_opt(
                &actual.connection.server_id.is_some(),
                expected_has_server_connection_id,
            )?;
            match_results_opt(&actual.command, expected_command, entities)?;
            Ok(())
        }
        (
            CommandEvent::Succeeded(actual),
            ExpectedCommandEvent::Succeeded {
                command_name: expected_command_name,
                reply: expected_reply,
                has_service_id: expected_has_service_id,
                has_server_connection_id: expected_has_server_connection_id,
            },
        ) => {
            match_opt(&actual.command_name, expected_command_name)?;
            match_opt(&actual.service_id.is_some(), expected_has_service_id)?;
            match_opt(
                &actual.connection.server_id.is_some(),
                expected_has_server_connection_id,
            )?;
            match_results_opt(&actual.reply, expected_reply, None)?;
            Ok(())
        }
        (
            CommandEvent::Failed(actual),
            ExpectedCommandEvent::Failed {
                command_name: expected_command_name,
                has_service_id: expected_has_service_id,
                has_server_connection_id: expected_has_server_connection_id,
            },
        ) => {
            match_opt(&actual.service_id.is_some(), expected_has_service_id)?;
            match_opt(
                &actual.connection.server_id.is_some(),
                expected_has_server_connection_id,
            )?;
            match_opt(&actual.command_name, expected_command_name)?;
            Ok(())
        }
        _ => expected_err(actual, expected),
    }
}

fn cmap_events_match(actual: &CmapEvent, expected: &ExpectedCmapEvent) -> Result<(), String> {
    match (actual, expected) {
        (CmapEvent::PoolCreated(_), ExpectedCmapEvent::PoolCreated {}) => Ok(()),
        (CmapEvent::PoolReady(_), ExpectedCmapEvent::PoolReady {}) => Ok(()),
        (
            CmapEvent::PoolCleared(actual),
            ExpectedCmapEvent::PoolCleared {
                has_service_id: expected_has_service_id,
            },
        ) => match_opt(&actual.service_id.is_some(), expected_has_service_id),
        (CmapEvent::PoolClosed(_), ExpectedCmapEvent::PoolClosed {}) => Ok(()),
        (CmapEvent::ConnectionCreated(_), ExpectedCmapEvent::ConnectionCreated {}) => Ok(()),
        (CmapEvent::ConnectionReady(_), ExpectedCmapEvent::ConnectionReady {}) => Ok(()),
        (
            CmapEvent::ConnectionClosed(actual),
            ExpectedCmapEvent::ConnectionClosed {
                reason: expected_reason,
            },
        ) => match_opt(&actual.reason, expected_reason),
        (
            CmapEvent::ConnectionCheckoutStarted(_),
            ExpectedCmapEvent::ConnectionCheckOutStarted {},
        ) => Ok(()),
        (
            CmapEvent::ConnectionCheckoutFailed(actual),
            ExpectedCmapEvent::ConnectionCheckOutFailed {
                reason: expected_reason,
            },
        ) => match_opt(&actual.reason, expected_reason),
        (CmapEvent::ConnectionCheckedOut(_), ExpectedCmapEvent::ConnectionCheckedOut {}) => Ok(()),
        (CmapEvent::ConnectionCheckedIn(_), ExpectedCmapEvent::ConnectionCheckedIn {}) => Ok(()),
        _ => expected_err(actual, expected),
    }
}

fn sdam_events_match(actual: &SdamEvent, expected: &ExpectedSdamEvent) -> Result<(), String> {
    match (actual, expected) {
        (
            SdamEvent::ServerDescriptionChanged(actual),
            ExpectedSdamEvent::ServerDescriptionChanged {
                previous_description,
                new_description,
            },
        ) => {
            let match_sd = |actual: &ServerDescription,
                            expected: &TestServerDescription|
             -> std::result::Result<(), String> {
                match_opt(&actual.server_type(), &expected.server_type)?;
                Ok(())
            };

            if let Some(expected_previous_description) = previous_description {
                match_sd(&actual.previous_description, expected_previous_description)?;
            }
            if let Some(expected_new_description) = new_description {
                match_sd(&actual.new_description, expected_new_description)?;
            }
            Ok(())
        }
        (SdamEvent::TopologyOpening(_), ExpectedSdamEvent::TopologyOpening {}) => Ok(()),
        (SdamEvent::TopologyClosed(_), ExpectedSdamEvent::TopologyClosed {}) => Ok(()),
        (
            SdamEvent::TopologyDescriptionChanged(actual),
            ExpectedSdamEvent::TopologyDescriptionChanged {
                previous_description,
                new_description,
            },
        ) => {
            if let Some(expected_prev) = previous_description {
                match_opt(
                    &actual.previous_description.topology_type(),
                    &expected_prev.topology_type,
                )?;
            }
            if let Some(expected_new) = new_description {
                match_opt(
                    &actual.new_description.topology_type(),
                    &expected_new.topology_type,
                )?;
            }
            Ok(())
        }
        (
            SdamEvent::ServerHeartbeatStarted(actual),
            ExpectedSdamEvent::ServerHeartbeatStarted { awaited },
        ) => {
            match_opt(&actual.awaited, awaited)?;
            Ok(())
        }
        (
            SdamEvent::ServerHeartbeatSucceeded(actual),
            ExpectedSdamEvent::ServerHeartbeatSucceeded { awaited },
        ) => {
            match_opt(&actual.awaited, awaited)?;
            Ok(())
        }
        (
            SdamEvent::ServerHeartbeatFailed(actual),
            ExpectedSdamEvent::ServerHeartbeatFailed { awaited },
        ) => {
            match_opt(&actual.awaited, awaited)?;
            Ok(())
        }
        _ => expected_err(actual, expected),
    }
}

fn results_match_inner(
    actual: Option<&Bson>,
    expected: &Bson,
    returns_root_documents: bool,
    root: bool,
    entities: Option<&EntityMap>,
) -> Result<(), String> {
    match expected {
        Bson::Document(expected_doc) => {
            if let Some((key, value)) = expected_doc.iter().next() {
                if key.starts_with("$$") && expected_doc.len() == 1 {
                    return special_operator_matches((key, value), actual, entities)
                        .map_err(|e| format!("{key}: {e}"));
                }
            }

            let actual_doc = match actual {
                Some(Bson::Document(actual)) => actual,
                // The only case in which None is an acceptable value is if the expected document
                // is a special operator; otherwise, the two documents do not match.
                _ => return Err(format!("expected document, found {actual:?}")),
            };

            for (key, value) in expected_doc {
                if key == "upsertedCount" {
                    continue;
                }
                results_match_inner(actual_doc.get(key), value, false, false, entities)
                    .map_err(|e| format!("{key:?}: {e}"))?;
            }

            // Documents that are not the root-level document should not contain extra keys.
            if !root {
                for (key, _) in actual_doc {
                    if !expected_doc.contains_key(key) {
                        return Err(format!("extra key {key:?} found"));
                    }
                }
            }

            Ok(())
        }
        Bson::Array(expected_array) => {
            let actual_array = match actual {
                Some(Bson::Array(arr)) => arr,
                _ => return Err(format!("expected array, got {actual:?}")),
            };
            if expected_array.len() != actual_array.len() {
                return Err(format!(
                    "expected array len = {}, got len = {}",
                    expected_array.len(),
                    actual_array.len()
                ));
            }

            // Some operations return an array of documents that should be treated as root
            // documents.
            for (actual, expected) in actual_array.iter().zip(expected_array) {
                results_match_inner(
                    Some(actual),
                    expected,
                    false,
                    returns_root_documents,
                    entities,
                )?;
            }

            Ok(())
        }
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => match actual {
            Some(actual) => numbers_match(actual, expected),
            None => Err("expected number, got None".to_string()),
        },
        _ => match actual {
            Some(actual) => match_eq(actual, expected),
            None if *expected == Bson::Null => Ok(()),
            None => Err(format!("expected {expected:?}, got None")),
        },
    }
}

fn expected_err<A: Debug, B: Debug>(actual: &A, expected: &B) -> Result<(), String> {
    Err(format!("expected {expected:?}, got {actual:?}"))
}

fn match_eq<V: PartialEq + std::fmt::Debug>(actual: &V, expected: &V) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        expected_err(actual, expected)
    }
}

fn numbers_match(actual: &Bson, expected: &Bson) -> Result<(), String> {
    if actual.element_type() == expected.element_type() {
        return match_eq(actual, expected);
    }

    match (get_int(actual), get_int(expected)) {
        (Some(actual), Some(expected)) => match_eq(&actual, &expected),
        _ => expected_err(actual, expected),
    }
}

fn special_operator_matches(
    (key, value): (&String, &Bson),
    actual: Option<&Bson>,
    entities: Option<&EntityMap>,
) -> Result<(), String> {
    match key.as_ref() {
        "$$exists" => {
            let expected = value.as_bool().unwrap();
            // Allow `Some(Bson::Null)` to count as not existing
            if !expected && actual == Some(&Bson::Null) {
                return Ok(());
            }
            match_eq(&actual.is_some(), &expected)
        }
        "$$type" => {
            if let Some(actual) = actual {
                type_matches(value, actual)
            } else {
                Err(format!(
                    "Expected value to have type {value:?} but got None"
                ))
            }
        }
        "$$unsetOrMatches" => {
            if actual.is_some() {
                results_match_inner(actual, value, false, false, entities)
            } else {
                Ok(())
            }
        }
        "$$matchesEntity" => {
            let id = value.as_str().unwrap();
            entity_matches(id, actual, entities.unwrap())
        }
        "$$matchesHexBytes" => match (actual, value) {
            (Some(Bson::String(actual)), Bson::String(expected)) => {
                if actual.to_lowercase() == expected.to_lowercase() {
                    Ok(())
                } else {
                    Err(format!(
                        "hex bytes do not match: expected {actual:?} but got {expected:?}"
                    ))
                }
            }
            (actual, expected) => Err(format!(
                "actual and expected should both be BSON strings but got: actual {actual:?}, \
                 expected {expected:?}"
            )),
        },
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
            None => panic!("Could not find entity: {value}"),
        },
        "$$matchAsDocument" => {
            let str = match actual {
                Some(Bson::String(str)) => str,
                _ => return Err(format!("expected value to be a string, got {actual:?}")),
            };
            let json: serde_json::Value = serde_json::from_str(str)
                .map_err(|e| format!("Failed to convert string to JSON: {e}"))?;
            let doc = json
                .try_into()
                .map_err(|e| format!("Failed to convert JSON to BSON: {e}"))?;
            results_match_inner(Some(&doc), value, false, false, entities)
        }
        "$$matchAsRoot" => results_match_inner(actual, value, false, true, entities),
        "$$lte" => {
            let Some(expected) = get_double(value) else {
                return Err(format!("expected number for comparison, got {value}"));
            };
            let Some(actual) = actual.and_then(get_double) else {
                return Err(format!("expected actual to be a number, got {actual:?}"));
            };
            if actual > expected {
                return Err(format!("expected actual to be <= {expected}, got {actual}"));
            }
            Ok(())
        }
        other => panic!("unknown special operator: {other}"),
    }
}

fn entity_matches(id: &str, actual: Option<&Bson>, entities: &EntityMap) -> Result<(), String> {
    let bson = entities.get(id).unwrap().as_bson();
    results_match_inner(actual, bson, false, false, Some(entities))
}

fn type_matches(types: &Bson, actual: &Bson) -> Result<(), String> {
    match types {
        Bson::Array(types) => {
            if types.iter().any(|t| type_matches(t, actual).is_ok()) {
                Ok(())
            } else {
                Err(format!("expected any of {types:?}, got {actual:?}"))
            }
        }
        Bson::String(str) => {
            if str == "number" {
                let number_types: Bson = vec!["int", "long", "double", "decimal"].into();
                return type_matches(&number_types, actual);
            }
            let expected = match str.as_ref() {
                "double" => ElementType::Double,
                "string" => ElementType::String,
                "object" => ElementType::EmbeddedDocument,
                "array" => ElementType::Array,
                "binData" => ElementType::Binary,
                "undefined" => ElementType::Undefined,
                "objectId" => ElementType::ObjectId,
                "bool" => ElementType::Boolean,
                "date" => ElementType::DateTime,
                "null" => ElementType::Null,
                "regex" => ElementType::RegularExpression,
                "dbPointer" => ElementType::DbPointer,
                "javascript" => ElementType::JavaScriptCode,
                "symbol" => ElementType::Symbol,
                "javascriptWithScope" => ElementType::JavaScriptCodeWithScope,
                "int" => ElementType::Int32,
                "timestamp" => ElementType::Timestamp,
                "long" => ElementType::Int64,
                "decimal" => ElementType::Decimal128,
                "minKey" => ElementType::MinKey,
                "maxKey" => ElementType::MaxKey,
                other => panic!("unrecognized type: {other}"),
            };
            match_eq(&actual.element_type(), &expected)
        }
        other => panic!("unrecognized type: {other}"),
    }
}

#[test]
fn basic_matching() {
    let actual = doc! { "x": 1, "y": 1 };
    let expected = doc! { "x": 1 };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": 1, "y": 1 };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_err());
}

#[test]
fn array_matching() {
    let mut actual: Vec<Bson> = Vec::new();
    for i in 1..4 {
        actual.push(Bson::Int32(i));
    }
    let mut expected: Vec<Bson> = Vec::new();
    for i in 1..3 {
        expected.push(Bson::Int32(i));
    }
    assert!(results_match(
        Some(&Bson::Array(actual)),
        &Bson::Array(expected),
        false,
        None,
    )
    .is_err());

    let actual = vec![
        Bson::Document(doc! { "x": 1, "y": 1 }),
        Bson::Document(doc! { "x": 2, "y": 2 }),
    ];
    let expected = vec![
        Bson::Document(doc! { "x": 1 }),
        Bson::Document(doc! { "x": 2 }),
    ];
    assert!(results_match(
        Some(&Bson::Array(actual)),
        &Bson::Array(expected),
        false,
        None,
    )
    .is_err());
}

#[test]
fn special_operators() {
    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$exists": true } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$exists": false } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_err());

    let actual = doc! { "x": 1 };
    let expected = doc! { "y": { "$$exists": false } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! { "x": 1 };
    let expected = doc! { "y": { "$$exists": true } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_err());

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$type": [ "int", "long" ] } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! {};
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! { "x": 2 };
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_err());

    let expected = doc! { "x": { "y": { "$$exists": false } } };
    let actual = doc! { "x": {} };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());
}

#[test]
fn extra_fields() {
    let actual = doc! { "x": 1, "y": 2 };
    let expected = doc! { "x": 1 };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_ok());

    let actual = doc! { "doc": { "x": 1, "y": 2 } };
    let expected = doc! { "doc": { "x": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected),
        false,
        None,
    )
    .is_err());
}

#[test]
fn numbers() {
    let actual = Bson::Int32(2);
    let expected = Bson::Int64(2);
    assert!(results_match(Some(&actual), &expected, false, None).is_ok());

    let actual = Bson::Double(2.5);
    let expected = Bson::Int32(2);
    assert!(results_match(Some(&actual), &expected, false, None).is_err());

    let actual = Bson::Double(2.0);
    let expected = Bson::Int64(2);
    assert!(results_match(Some(&actual), &expected, false, None).is_ok());
}
