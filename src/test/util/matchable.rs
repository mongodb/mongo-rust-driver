use std::{any::Any, fmt::Debug, time::Duration};

use crate::{
    bson::{Bson, Document},
    bson_util,
    options::{AuthMechanism, Credential},
};

pub trait Matchable: Sized + 'static {
    fn is_placeholder(&self) -> bool {
        false
    }

    fn content_matches(&self, expected: &Self) -> bool;

    fn matches<T: Matchable + Any>(&self, expected: &T) -> bool {
        if expected.is_placeholder() {
            return true;
        }
        if let Some(expected) = Any::downcast_ref::<Self>(expected) {
            self.content_matches(expected)
        } else {
            false
        }
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

    fn content_matches(&self, expected: &Bson) -> bool {
        match (self, expected) {
            (Bson::Document(actual_doc), Bson::Document(expected_doc)) => {
                actual_doc.matches(expected_doc)
            }
            (Bson::Array(actual_array), Bson::Array(expected_array)) => {
                for (i, expected_element) in expected_array.iter().enumerate() {
                    if actual_array.len() <= i || !actual_array[i].matches(expected_element) {
                        return false;
                    }
                }
                true
            }
            _ => match (bson_util::get_int(self), get_int(expected)) {
                (Some(actual_int), Some(expected_int)) => actual_int == expected_int,
                (None, Some(_)) => false,
                _ => self == expected,
            },
        }
    }
}

impl Matchable for Document {
    fn content_matches(&self, expected: &Document) -> bool {
        for (k, v) in expected.iter() {
            if k == "upsertedCount" {
                continue;
            }
            match self.get(k) {
                Some(actual_v) => {
                    if !actual_v.matches(v) {
                        return false;
                    }
                }
                None => {
                    if v != &Bson::Null {
                        return false;
                    }
                }
            }
        }
        true
    }
}

impl Matchable for Credential {
    fn content_matches(&self, expected: &Credential) -> bool {
        self.username.content_matches(&expected.username)
            && self.source.content_matches(&expected.source)
            && self.password.content_matches(&expected.password)
            && self.mechanism.content_matches(&expected.mechanism)
            && self
                .mechanism_properties
                .content_matches(&expected.mechanism_properties)
    }
}

impl Matchable for AuthMechanism {
    fn content_matches(&self, expected: &AuthMechanism) -> bool {
        self == expected
    }
}

impl Matchable for bool {
    fn content_matches(&self, expected: &bool) -> bool {
        self == expected
    }
}

impl Matchable for u32 {
    fn is_placeholder(&self) -> bool {
        self == &42
    }

    fn content_matches(&self, expected: &u32) -> bool {
        self == expected
    }
}

impl Matchable for String {
    fn is_placeholder(&self) -> bool {
        self.as_str() == "42"
    }

    fn content_matches(&self, expected: &String) -> bool {
        self == expected
    }
}

impl Matchable for Duration {
    fn content_matches(&self, expected: &Duration) -> bool {
        self == expected
    }
}

impl<T: Matchable> Matchable for Option<T> {
    fn is_placeholder(&self) -> bool {
        match self {
            Some(ref v) => v.is_placeholder(),
            None => true,
        }
    }

    fn content_matches(&self, expected: &Option<T>) -> bool {
        // this if should always succeed given that "None" counts as a placeholder value.
        if let Some(expected_value) = expected {
            return match self {
                Some(actual_value) => actual_value.content_matches(expected_value),
                None => false,
            };
        }
        true
    }
}

pub fn assert_matches<A: Matchable + Debug, E: Matchable + Debug>(
    actual: &A,
    expected: &E,
    description: Option<&str>,
) {
    assert!(
        actual.matches(expected),
        "{}\n{:?}\n did not MATCH \n{:?}",
        description.unwrap_or(""),
        actual,
        expected
    );
}

fn parse_i64_ext_json(doc: &Document) -> Option<i64> {
    let number_string = doc.get("$numberLong").and_then(Bson::as_str)?;
    number_string.parse::<i64>().ok()
}

fn get_int(value: &Bson) -> Option<i64> {
    bson_util::get_int(value).or_else(|| value.as_document().and_then(parse_i64_ext_json))
}
