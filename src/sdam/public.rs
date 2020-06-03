use std::{fmt, time::Duration};

pub use crate::sdam::description::server::ServerType;
use crate::{
    bson::DateTime,
    is_master::IsMasterCommandResponse,
    options::StreamAddress,
    sdam::description::server::ServerDescription,
    selection_criteria::TagSet,
};

/// Provides information about a given server in the cluster that a client is connected to. This is
/// used when providing a predicate as a `SelectionCriteria`.
pub struct ServerInfo<'a> {
    description: &'a ServerDescription,
}

impl<'a> ServerInfo<'a> {
    pub(crate) fn new(description: &'a ServerDescription) -> Self {
        Self { description }
    }

    fn command_response_getter<T>(
        &'a self,
        f: impl Fn(&'a IsMasterCommandResponse) -> Option<T>,
    ) -> Option<T> {
        self.description
            .reply
            .as_ref()
            .ok()
            .and_then(|reply| reply.as_ref().and_then(|r| f(&r.command_response)))
    }

    /// Gets the address of the server.
    pub fn address(&self) -> &StreamAddress {
        &self.description.address
    }

    /// Gets the weighted average of the time it has taken for an isMaster command to round-trip
    /// from the driver to the server.
    ///
    /// This is the value that the driver uses internally to determine the latency window as part of
    /// server selection.
    pub fn average_round_trip_time(&self) -> Option<Duration> {
        self.description.average_round_trip_time
    }

    /// Gets the last time that the driver's monitoring thread for the server updated the internal
    /// information about the server.
    pub fn last_update_time(&self) -> Option<DateTime> {
        self.description.last_update_time
    }

    /// Gets the maximum wire version that the server supports.
    pub fn max_wire_version(&self) -> Option<i32> {
        self.command_response_getter(|r| r.max_wire_version)
    }

    /// Gets the minimum wire version that the server supports.
    pub fn min_wire_version(&self) -> Option<i32> {
        self.command_response_getter(|r| r.min_wire_version)
    }

    /// Gets the name of the replica set that the server is part of.
    pub fn replica_set_name(&self) -> Option<&str> {
        self.command_response_getter(|r| r.set_name.as_deref())
    }

    /// Gets the version of the replica set that the server is part of.
    pub fn replica_set_version(&self) -> Option<i32> {
        self.command_response_getter(|r| r.set_version)
    }

    /// Get the type of the server.
    pub fn server_type(&self) -> ServerType {
        self.description.server_type
    }

    /// Gets the tags associated with the server.
    pub fn tags(&self) -> Option<&TagSet> {
        self.command_response_getter(|r| r.tags.as_ref())
    }
}

impl<'a> fmt::Display for ServerInfo<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        write!(
            f,
            "{{ Address: {}, Type: {:?}",
            self.address(),
            self.server_type()
        )?;

        match self.description.reply {
            Ok(_) => {
                if let Some(avg_rtt) = self.average_round_trip_time() {
                    write!(f, ", Average RTT: {:?}", avg_rtt)?;
                }

                if let Some(last_update_time) = self.last_update_time() {
                    write!(f, ", Last Update Time: {:?}", last_update_time)?;
                }

                if let Some(max_wire_version) = self.max_wire_version() {
                    write!(f, ", Max Wire Version: {}", max_wire_version)?;
                }

                if let Some(min_wire_version) = self.min_wire_version() {
                    write!(f, ", Min Wire Version: {}", min_wire_version)?;
                }

                if let Some(rs_name) = self.replica_set_name() {
                    write!(f, ", Replica Set Name: {}", rs_name)?;
                }

                if let Some(rs_version) = self.replica_set_version() {
                    write!(f, ", Replica Set Version: {}", rs_version)?;
                }

                if let Some(tags) = self.tags() {
                    write!(f, ", Tags: {:?}", tags)?;
                }

                if let Some(compatibility_error) = self.description.compatibility_error_message() {
                    write!(f, ", Compatiblity Error: {}", compatibility_error)?;
                }
            }
            Err(ref e) => {
                write!(f, ", Error: {}", e)?;
            }
        }

        write!(f, " }}")
    }
}
