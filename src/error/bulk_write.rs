#![allow(missing_docs)]

use std::collections::HashMap;

use crate::{
    error::{WriteConcernError, WriteError},
    results::BulkWriteResult,
};

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct BulkWriteError {
    pub write_concern_errors: Vec<WriteConcernError>,
    pub write_errors: HashMap<usize, WriteError>,
    pub partial_result: Option<BulkWriteResult>,
}

impl BulkWriteError {
    pub(crate) fn merge(&mut self, other: BulkWriteError) {
        self.write_concern_errors.extend(other.write_concern_errors);
        self.write_errors.extend(other.write_errors);
        if let Some(other_partial_result) = other.partial_result {
            self.merge_partial_results(other_partial_result);
        }
    }

    pub(crate) fn merge_partial_results(&mut self, other_partial_result: BulkWriteResult) {
        if let Some(ref mut partial_result) = self.partial_result {
            partial_result.merge(other_partial_result);
        } else {
            self.partial_result = Some(other_partial_result);
        }
    }
}
