#![allow(missing_docs)]

use std::collections::HashMap;

use crate::{
    error::{WriteConcernError, WriteError},
    results::{BulkWriteResult, SummaryBulkWriteResult, VerboseBulkWriteResult},
};

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct BulkWriteError {
    pub write_concern_errors: Vec<WriteConcernError>,
    pub write_errors: HashMap<usize, WriteError>,
    pub partial_result: Option<PartialBulkWriteResult>,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
#[cfg_attr(test, serde(untagged))]
pub enum PartialBulkWriteResult {
    Summary(SummaryBulkWriteResult),
    Verbose(VerboseBulkWriteResult),
}

impl PartialBulkWriteResult {
    pub(crate) fn merge(&mut self, other: Self) {
        match (self, other) {
            (Self::Summary(this), Self::Summary(other)) => this.merge(other),
            (Self::Verbose(this), Self::Verbose(other)) => this.merge(other),
            // The operation execution path makes this an unreachable state
            _ => unreachable!(),
        }
    }
}

impl BulkWriteError {
    pub(crate) fn merge(&mut self, other: Self) {
        self.write_concern_errors.extend(other.write_concern_errors);
        self.write_errors.extend(other.write_errors);
        if let Some(other_partial_result) = other.partial_result {
            self.merge_partial_results(other_partial_result);
        }
    }

    pub(crate) fn merge_partial_results(&mut self, other_partial_result: PartialBulkWriteResult) {
        if let Some(ref mut partial_result) = self.partial_result {
            partial_result.merge(other_partial_result);
        } else {
            self.partial_result = Some(other_partial_result);
        }
    }
}
