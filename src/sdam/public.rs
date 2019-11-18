use std::time::Duration;

use bson::UtcDateTime;

pub use crate::sdam::description::server::ServerType;
use crate::{
    is_master::IsMasterCommandResponse, options::StreamAddress,
    sdam::description::server::ServerDescription, selection_criteria::TagSet,
};

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

    pub fn address(&self) -> &StreamAddress {
        &self.description.address
    }

    pub fn average_round_trip_time(&self) -> Option<Duration> {
        self.description.average_round_trip_time
    }

    pub fn last_update_time(&self) -> Option<UtcDateTime> {
        self.description.last_update_time
    }

    pub fn max_wire_version(&self) -> Option<i32> {
        self.command_response_getter(|r| r.max_wire_version)
    }

    pub fn min_wire_version(&self) -> Option<i32> {
        self.command_response_getter(|r| r.min_wire_version)
    }

    pub fn server_type(&self) -> ServerType {
        self.description.server_type
    }

    pub fn set_version(&self) -> Option<i32> {
        self.command_response_getter(|r| r.set_version)
    }

    pub fn set_name(&self) -> Option<&str> {
        self.command_response_getter(|r| r.set_name.as_ref().map(String::as_str))
    }

    pub fn tags(&self) -> Option<&TagSet> {
        self.command_response_getter(|r| r.tags.as_ref())
    }
}
