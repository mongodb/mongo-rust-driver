use crate::bson::{doc, spec::ElementType, Bson};

pub fn results_match(actual: Option<&Bson>, expected: &Bson) -> bool {
    match expected {
        Bson::Document(expected_doc) => {
            if let Some(special_op) = expected_doc.iter().next() {
                if special_op.0.starts_with("$$") {
                    return special_operator_matches(special_op, actual);
                }
            }
            let actual = match actual {
                Some(actual) => actual,
                // The only case in which None is an acceptable value is if the expected document
                // is a special operator; otherwise, the two documents do not match.
                None => return false,
            };
            let actual_doc = actual.as_document().unwrap();
            for (key, value) in expected_doc {
                if !results_match(actual_doc.get(key), value) {
                    return false;
                }
            }
            true
        }
        Bson::Array(expected_array) => {
            let actual_array = actual.unwrap().as_array().unwrap();
            if expected_array.len() != actual_array.len() {
                return false;
            }

            for (actual, expected) in actual_array.iter().zip(expected_array) {
                if !results_match(Some(actual), expected) {
                    return false;
                }
            }

            true
        }
        _ => match actual {
            Some(actual) => actual == expected,
            None => false,
        },
    }
}

fn special_operator_matches(special_op: (&String, &Bson), actual: Option<&Bson>) -> bool {
    match special_op.0.as_ref() {
        "$$exists" => special_op.1.as_bool().unwrap() == actual.is_some(),
        "$$type" => type_matches(special_op.1, actual.unwrap()),
        "$$unsetOrMatches" => {
            if let Some(bson) = actual {
                results_match(Some(special_op.1), bson)
            } else {
                true
            }
        }
        "$$sessionLsid" => panic!("Explicit sessions not implemented"),
        other => panic!("unknown special operator: {}", other),
    }
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
        Bson::Int32(n) => match n {
            1 => actual.element_type() == ElementType::Double,
            2 => actual.element_type() == ElementType::String,
            3 => actual.element_type() == ElementType::EmbeddedDocument,
            4 => actual.element_type() == ElementType::Array,
            5 => actual.element_type() == ElementType::Binary,
            6 => actual.element_type() == ElementType::Undefined,
            7 => actual.element_type() == ElementType::ObjectId,
            8 => actual.element_type() == ElementType::Boolean,
            9 => actual.element_type() == ElementType::DateTime,
            10 => actual.element_type() == ElementType::Null,
            11 => actual.element_type() == ElementType::RegularExpression,
            12 => actual.element_type() == ElementType::DbPointer,
            13 => actual.element_type() == ElementType::JavaScriptCode,
            14 => actual.element_type() == ElementType::Symbol,
            15 => actual.element_type() == ElementType::JavaScriptCodeWithScope,
            16 => actual.element_type() == ElementType::Int32,
            17 => actual.element_type() == ElementType::Timestamp,
            18 => actual.element_type() == ElementType::Int64,
            19 => actual.element_type() == ElementType::Decimal128,
            -1 => actual.element_type() == ElementType::MinKey,
            127 => actual.element_type() == ElementType::MaxKey,
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
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": 1, "y": 1 };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
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
        &Bson::Array(expected)
    ));

    let mut actual: Vec<Bson> = Vec::new();
    actual.push(Bson::Document(doc! { "x": 1, "y": 1 }));
    actual.push(Bson::Document(doc! { "x": 2, "y": 2 }));
    let mut expected: Vec<Bson> = Vec::new();
    expected.push(Bson::Document(doc! { "x": 1 }));
    expected.push(Bson::Document(doc! { "x": 2 }));
    assert!(results_match(
        Some(&Bson::Array(actual)),
        &Bson::Array(expected)
    ));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn special_operators() {
    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$exists": true } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$exists": false } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "y": { "$$exists": false } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "y": { "$$exists": true } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$type": [ "int", "long" ] } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$type": 16 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": "one" };
    let expected = doc! { "x": { "$$type": 16 } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! {};
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 1 };
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));

    let actual = doc! { "x": 2 };
    let expected = doc! { "x": { "$$unsetOrMatches": 1 } };
    assert!(!results_match(
        Some(&Bson::Document(actual)),
        &Bson::Document(expected)
    ));
}
