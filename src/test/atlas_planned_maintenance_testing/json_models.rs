use serde::Serialize;
use serde_json::Number;

use crate::bson::Bson;

#[derive(Default, Serialize)]
pub(crate) struct Events {
    pub(crate) errors: Vec<Bson>,
    pub(crate) failures: Vec<Bson>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Results {
    pub(crate) num_errors: Number,
    pub(crate) num_failures: Number,
    pub(crate) num_successes: Number,
    pub(crate) num_iterations: Number,
}

impl Results {
    pub(crate) fn new_empty() -> Self {
        Self {
            num_errors: (-1i8).into(),
            num_failures: (-1i8).into(),
            num_successes: (-1i8).into(),
            num_iterations: (-1i8).into(),
        }
    }
}
