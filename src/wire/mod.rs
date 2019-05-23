mod doc_reader;
//#[cfg_attr(feature = "cargo-clippy", allow(redundant_field_names))]
mod flags;
mod header;
mod query;
mod reply;

pub use self::{
    flags::QueryFlags,
    header::{new_request_id, Header, OpCode},
    query::Query,
    reply::Reply,
};
