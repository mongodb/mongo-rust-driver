use std::net::TcpStream;

use bson::{bson, doc, Bson};

use super::message::{Message, MessageFlags, MessageSection};
use crate::{
    options::StreamAddress,
    test::{CLIENT_OPTIONS, LOCK},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn basic() {
    if CLIENT_OPTIONS.tls_options().is_some() {
        return;
    }

    let _guard = LOCK.run_concurrently();

    let message = Message {
        response_to: 0,
        flags: MessageFlags::empty(),
        sections: vec![MessageSection::Document(
            doc! { "isMaster": 1, "$db": "admin" },
        )],
        checksum: None,
        request_id: None,
    };

    let StreamAddress { ref hostname, port } = CLIENT_OPTIONS.hosts[0];

    let mut stream = TcpStream::connect((&hostname[..], port.unwrap_or(27017))).unwrap();
    message.write_to(&mut stream).unwrap();

    let reply = Message::read_from(&mut stream).unwrap();

    let response_doc = match reply.sections.into_iter().next().unwrap() {
        MessageSection::Document(doc) => doc,
        MessageSection::Sequence { documents, .. } => documents.into_iter().next().unwrap(),
    };

    assert_eq!(response_doc.get("ok"), Some(&Bson::FloatingPoint(1.0)));
}
