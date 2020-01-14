use std::{fmt, time::Duration};

use bson::{oid::ObjectId, UtcDateTime};
use chrono::offset::Utc;

use crate::{
    error::Result,
    is_master::IsMasterReply,
    options::StreamAddress,
    selection_criteria::TagSet,
};

const DRIVER_MIN_DB_VERSION: &str = "3.6";
const DRIVER_MIN_WIRE_VERSION: i32 = 6;
const DRIVER_MAX_WIRE_VERSION: i32 = 7;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ServerType {
    Standalone,
    Mongos,
    RSPrimary,
    RSSecondary,
    RSArbiter,
    RSOther,
    RSGhost,
    Unknown,
}

impl ServerType {
    pub(crate) fn can_auth(self) -> bool {
        match self {
            ServerType::Standalone
            | ServerType::RSPrimary
            | ServerType::RSSecondary
            | ServerType::Mongos => true,
            _ => false,
        }
    }
}

impl Default for ServerType {
    fn default() -> Self {
        ServerType::Unknown
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ServerDescription {
    pub(crate) address: StreamAddress,
    pub(crate) server_type: ServerType,
    pub(crate) last_update_time: Option<UtcDateTime>,
    pub(crate) average_round_trip_time: Option<Duration>,

    // The SDAM spec indicates that a ServerDescription needs to contain an error message if an
    // error occurred when trying to send an isMaster for the server's heartbeat. Additionally,
    // we need to be able to create a server description that doesn't contain either an isMaster
    // reply or an error, since there's a gap between when a server is newly added to the topology
    // and when the first heartbeat occurs.
    //
    // In order to represent all these states, we store a Result directly in the ServerDescription,
    // which either contains the aforementioned error message or an Option<IsMasterResult>. This
    // allows us to ensure that only valid states are possible (e.g. preventing that both an error
    // and a reply are present) while still making it easy to define helper methods on
    // ServerDescription for information we need from the isMaster reply by propagating with `?`.
    pub(crate) reply: Result<Option<IsMasterReply>>,
}

impl PartialEq for ServerDescription {
    fn eq(&self, other: &Self) -> bool {
        if self.address == other.address && self.server_type == other.server_type {
            return false;
        }

        match (self.reply.as_ref(), other.reply.as_ref()) {
            (Ok(self_reply), Ok(other_reply)) => {
                let self_response = self_reply.as_ref().map(|r| &r.command_response);
                let other_response = other_reply.as_ref().map(|r| &r.command_response);

                self_response == other_response
            }
            _ => false,
        }
    }
}

impl fmt::Display for ServerDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ Address: {}, Type: {:?}, Average RTT: {:?}, LastError: ",
            self.address, self.server_type, self.average_round_trip_time
        )?;
        match self.reply.as_ref().err() {
            Some(e) => write!(f, "{}", e)?,
            None => write!(f, "None")?,
        }
        write!(f, " }}")
    }
}

impl ServerDescription {
    pub(crate) fn new(
        mut address: StreamAddress,
        is_master_reply: Option<Result<IsMasterReply>>,
    ) -> Self {
        address.hostname = address.hostname.to_lowercase();

        let mut description = Self {
            address,
            server_type: Default::default(),
            last_update_time: None,
            reply: is_master_reply.transpose(),
            average_round_trip_time: None,
        };

        // We want to set last_update_time if we got any sort of response from the server.
        match description.reply {
            Ok(None) => {}
            _ => description.last_update_time = Some(Utc::now().into()),
        };

        if let Ok(Some(ref mut reply)) = description.reply {
            // Infer the server type from the isMaster response.
            description.server_type = reply.command_response.server_type();

            // Initialize the average round trip time. If a previous value is present for the
            // server, this will be updated before the server description is added to the topology
            // description.
            description.average_round_trip_time = reply.round_trip_time;

            // If the server type is unknown, we don't want to take into account the round trip time
            // when checking the latency during server selection.
            if let ServerType::Unknown = description.server_type {
                reply.round_trip_time.take();
            }

            // Normalize all instances of hostnames to lowercase.
            if let Some(ref mut hosts) = reply.command_response.hosts {
                let normalized_hostnames = hosts
                    .drain(..)
                    .map(|hostname| hostname.to_lowercase())
                    .collect();

                std::mem::replace(hosts, normalized_hostnames);
            }

            if let Some(ref mut passives) = reply.command_response.passives {
                let normalized_hostnames = passives
                    .drain(..)
                    .map(|hostname| hostname.to_lowercase())
                    .collect();

                std::mem::replace(passives, normalized_hostnames);
            }

            if let Some(ref mut arbiters) = reply.command_response.arbiters {
                let normalized_hostnames = arbiters
                    .drain(..)
                    .map(|hostname| hostname.to_lowercase())
                    .collect();

                std::mem::replace(arbiters, normalized_hostnames);
            }

            if let Some(ref mut me) = reply.command_response.me {
                std::mem::replace(me, me.to_lowercase());
            }
        }

        description
    }

    /// Whether this server is "available" as per the definition in the server selection spec.
    pub(crate) fn is_available(&self) -> bool {
        match self.server_type {
            ServerType::Unknown => false,
            _ => true,
        }
    }

    pub(crate) fn compatibility_error_message(&self) -> Option<String> {
        if let Ok(Some(ref reply)) = self.reply {
            let is_master_min_wire_version = reply.command_response.min_wire_version.unwrap_or(0);

            if is_master_min_wire_version > DRIVER_MAX_WIRE_VERSION {
                return Some(format!(
                    "Server at {} requires wire version {}, but this version of the MongoDB Rust \
                     driver only supports up to {}",
                    self.address, is_master_min_wire_version, DRIVER_MAX_WIRE_VERSION,
                ));
            }

            let is_master_max_wire_version = reply.command_response.max_wire_version.unwrap_or(0);

            if is_master_max_wire_version < DRIVER_MIN_WIRE_VERSION {
                return Some(format!(
                    "Server at {} reports wire version {}, but this version of the MongoDB Rust \
                     driver requires at least {} (MongoDB {}).",
                    self.address,
                    is_master_max_wire_version,
                    DRIVER_MIN_WIRE_VERSION,
                    DRIVER_MIN_DB_VERSION
                ));
            }
        }

        None
    }

    pub(crate) fn set_name(&self) -> Result<Option<String>> {
        let set_name = self
            .reply
            .as_ref()
            .map_err(Clone::clone)?
            .as_ref()
            .and_then(|reply| reply.command_response.set_name.clone());
        Ok(set_name)
    }

    pub(crate) fn known_hosts(&self) -> Result<impl Iterator<Item = &String>> {
        let known_hosts = self
            .reply
            .as_ref()
            .map_err(Clone::clone)?
            .as_ref()
            .map(|reply| {
                let hosts = reply.command_response.hosts.as_ref();
                let passives = reply.command_response.passives.as_ref();
                let arbiters = reply.command_response.arbiters.as_ref();

                hosts
                    .into_iter()
                    .flatten()
                    .chain(passives.into_iter().flatten())
                    .chain(arbiters.into_iter().flatten())
            });

        Ok(known_hosts.into_iter().flatten())
    }

    pub(crate) fn invalid_me(&self) -> Result<bool> {
        if let Some(ref reply) = self.reply.as_ref().map_err(Clone::clone)? {
            if let Some(ref me) = reply.command_response.me {
                return Ok(&self.address.to_string() != me);
            }
        }

        Ok(false)
    }

    pub(crate) fn set_version(&self) -> Result<Option<i32>> {
        let me = self
            .reply
            .as_ref()
            .map_err(Clone::clone)?
            .as_ref()
            .and_then(|reply| reply.command_response.set_version);
        Ok(me)
    }

    pub(crate) fn election_id(&self) -> Result<Option<ObjectId>> {
        let me = self
            .reply
            .as_ref()
            .map_err(Clone::clone)?
            .as_ref()
            .and_then(|reply| reply.command_response.election_id.clone());
        Ok(me)
    }

    #[cfg(test)]
    pub(crate) fn min_wire_version(&self) -> Result<Option<i32>> {
        let me = self
            .reply
            .as_ref()
            .map_err(Clone::clone)?
            .as_ref()
            .and_then(|reply| reply.command_response.min_wire_version);
        Ok(me)
    }

    #[cfg(test)]
    pub(crate) fn max_wire_version(&self) -> Result<Option<i32>> {
        let me = self
            .reply
            .as_ref()
            .map_err(Clone::clone)?
            .as_ref()
            .and_then(|reply| reply.command_response.max_wire_version);
        Ok(me)
    }

    pub(crate) fn last_write_date(&self) -> Result<Option<UtcDateTime>> {
        match self.reply {
            Ok(None) => Ok(None),
            Ok(Some(ref reply)) => Ok(reply
                .command_response
                .last_write
                .as_ref()
                .map(|write| write.last_write_date)),
            Err(ref e) => Err(e.clone()),
        }
    }

    pub(crate) fn matches_tag_set(&self, tag_set: &TagSet) -> bool {
        let reply = match self.reply.as_ref() {
            Ok(Some(ref reply)) => reply,
            _ => return false,
        };

        let server_tags = match reply.command_response.tags {
            Some(ref tags) => tags,
            None => return false,
        };

        tag_set
            .iter()
            .all(|(key, val)| server_tags.get(key) == Some(val))
    }
}
