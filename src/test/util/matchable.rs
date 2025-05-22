use std::{any::Any, fmt::Debug, time::Duration};

use crate::bson::spec::ElementType;

use crate::{
    bson::{Bson, Document},
    bson_util,
    options::{AuthMechanism, Credential},
};

pub trait Matchable: Sized + 'static {
    fn is_placeholder(&self) -> bool {
        false
    }

    fn content_matches(&self, expected: &Self) -> Result<(), String>;

    fn matches<T: Matchable + Any>(&self, expected: &T) -> Result<(), String> {
        if expected.is_placeholder() {
            return Ok(());
        }
        if let Some(expected) = <dyn Any>::downcast_ref::<Self>(expected) {
            self.content_matches(expected)
        } else {
            Err(format!(
                "Couldn't down downcast expected ({:?}) to self ({:?})",
                expected.type_id(),
                self.type_id()
            ))
        }
    }
}

pub trait MatchErrExt {
    fn prefix(self, name: &str) -> Self;
}

impl MatchErrExt for Result<(), String> {
    fn prefix(self, name: &str) -> Self {
        self.map_err(|s| format!("{}: {}", name, s))
    }
}

pub fn eq_matches<T: PartialEq + Debug>(
    name: &str,
    actual: &T,
    expected: &T,
) -> Result<(), String> {
    if actual != expected {
        return Err(format!(
            "expected {} {:?}, got {:?}",
            name, expected, actual
        ));
    }
    Ok(())
}

pub(crate) fn is_expected_type(expected: &Bson) -> Option<Vec<ElementType>> {
    let d = expected.as_document()?;
    if d.len() != 1 {
        return None;
    }
    match d.get("$$type")? {
        Bson::String(s) => Some(vec![type_from_name(s)]),
        Bson::Array(arr) => Some(
            arr.iter()
                .filter_map(|bs| bs.as_str().map(type_from_name))
                .collect(),
        ),
        _ => None,
    }
}

fn type_from_name(name: &str) -> ElementType {
    match name {
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
        _ => panic!("invalid type name {:?}", name),
    }
}

impl Matchable for Bson {
    fn is_placeholder(&self) -> bool {
        if let Bson::String(string) = self {
            string.as_str() == "42" || string.as_str() == ""
        } else {
            get_int(self) == Some(42)
        }
    }

    fn content_matches(&self, expected: &Bson) -> Result<(), String> {
        if let Some(types) = is_expected_type(expected) {
            if types.contains(&self.element_type()) {
                return Ok(());
            } else {
                return Err(format!(
                    "expected type {:?}, actual value {:?}",
                    types, self
                ));
            }
        }
        match (self, expected) {
            (Bson::Document(actual_doc), Bson::Document(expected_doc)) => {
                actual_doc.matches(expected_doc)
            }
            (Bson::Array(actual_array), Bson::Array(expected_array)) => {
                if actual_array.len() < expected_array.len() {
                    return Err(format!(
                        "expected {} array elements, got {}",
                        expected_array.len(),
                        actual_array.len()
                    ));
                }
                for (actual, expected) in actual_array.iter().zip(expected_array.iter()) {
                    actual.matches(expected)?;
                }
                Ok(())
            }
            _ => {
                match (bson_util::get_int(self), get_int(expected)) {
                    (Some(actual_int), Some(expected_int)) => {
                        eq_matches("int", &actual_int, &expected_int)?
                    }
                    (None, Some(expected_int)) => {
                        return Err(format!("expected int {}, got none", expected_int))
                    }
                    _ => eq_matches("bson", self, expected)?,
                }
                Ok(())
            }
        }
    }
}

impl Matchable for Document {
    fn content_matches(&self, expected: &Document) -> Result<(), String> {
        for (k, v) in expected.iter() {
            if k == "upsertedCount" {
                continue;
            }
            if k == "recoveryToken" && v.is_placeholder() && self.get_document(k).is_ok() {
                continue;
            }
            match self.get(k) {
                Some(actual_v) => actual_v.matches(v).prefix(k)?,
                None => {
                    if v != &Bson::Null {
                        return Err(format!("{:?}: expected value {:?}, got null", k, v));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Matchable for Credential {
    fn content_matches(&self, expected: &Credential) -> Result<(), String> {
        self.username
            .content_matches(&expected.username)
            .prefix("username")?;
        self.source
            .content_matches(&expected.source)
            .prefix("source")?;
        self.password
            .content_matches(&expected.password)
            .prefix("password")?;
        self.mechanism
            .content_matches(&expected.mechanism)
            .prefix("mechanism")?;
        self.mechanism_properties
            .content_matches(&expected.mechanism_properties)
            .prefix("mechanism_properties")?;
        Ok(())
    }
}

impl Matchable for AuthMechanism {
    fn content_matches(&self, expected: &AuthMechanism) -> Result<(), String> {
        eq_matches("AuthMechanism", self, expected)
    }
}

impl Matchable for bool {
    fn content_matches(&self, expected: &bool) -> Result<(), String> {
        eq_matches("bool", self, expected)
    }
}

impl Matchable for u32 {
    fn is_placeholder(&self) -> bool {
        self == &42
    }

    fn content_matches(&self, expected: &u32) -> Result<(), String> {
        eq_matches("u32", self, expected)
    }
}

impl Matchable for String {
    fn is_placeholder(&self) -> bool {
        self.as_str() == "42"
    }

    fn content_matches(&self, expected: &String) -> Result<(), String> {
        eq_matches("String", self, expected)
    }
}

impl Matchable for Duration {
    fn content_matches(&self, expected: &Duration) -> Result<(), String> {
        eq_matches("Duration", self, expected)
    }
}

impl<T: Matchable> Matchable for Option<T> {
    fn is_placeholder(&self) -> bool {
        match self {
            Some(ref v) => v.is_placeholder(),
            None => true,
        }
    }

    fn content_matches(&self, expected: &Option<T>) -> Result<(), String> {
        // this if should always succeed given that "None" counts as a placeholder value.
        if let Some(expected_value) = expected {
            return match self {
                Some(actual_value) => actual_value.content_matches(expected_value),
                None => Err("expected Some(_), got None".to_string()),
            };
        }
        Ok(())
    }
}

pub fn assert_matches<A: Matchable + Debug, E: Matchable + Debug>(
    actual: &A,
    expected: &E,
    description: Option<&str>,
) {
    let result = actual.matches(expected);
    assert!(
        result.is_ok(),
        "[{}] actual\n{:#?}\n did not MATCH expected\n{:#?}\n MATCH failure: {}",
        description.unwrap_or(""),
        actual,
        expected,
        result.unwrap_err(),
    );
}

fn parse_i64_ext_json(doc: &Document) -> Option<i64> {
    let number_string = doc.get("$numberLong").and_then(Bson::as_str)?;
    number_string.parse::<i64>().ok()
}

fn get_int(value: &Bson) -> Option<i64> {
    bson_util::get_int(value).or_else(|| value.as_document().and_then(parse_i64_ext_json))
}
