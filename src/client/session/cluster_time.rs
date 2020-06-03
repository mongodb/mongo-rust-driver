use derivative::Derivative;
use serde::{Deserialize, Serialize};

use crate::bson::{Document, Timestamp};

/// Struct modeling a cluster time reported by the server.
///
/// See [the MongoDB documentation](https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/)
/// for more information.
#[derive(Debug, Deserialize, Clone, Serialize, Derivative)]
#[derivative(PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterTime {
    cluster_time: Timestamp,

    #[derivative(PartialEq = "ignore")]
    signature: Document,
}

impl std::cmp::Ord for ClusterTime {
    // TODO: RUST-390 use Timestamp's Ord impl.
    fn cmp(&self, other: &ClusterTime) -> std::cmp::Ordering {
        let lhs = (self.cluster_time.time, self.cluster_time.increment);
        let rhs = (other.cluster_time.time, other.cluster_time.increment);
        lhs.cmp(&rhs)
    }
}

impl std::cmp::PartialOrd for ClusterTime {
    fn partial_cmp(&self, other: &ClusterTime) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
