use tokio::sync::RwLockReadGuard;

use super::message::{Message, MessageFlags, MessageSection};
use crate::{
    bson::{doc, Bson},
    cmap::options::StreamOptions,
    runtime::AsyncStream,
    test::{CLIENT_OPTIONS, LOCK},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn basic() {
    if CLIENT_OPTIONS.tls_options().is_some() {
        return;
    }

    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let message = Message {
        response_to: 0,
        flags: MessageFlags::empty(),
        sections: vec![MessageSection::Document(
            doc! { "isMaster": 1, "$db": "admin", "apiVersion": "1" },
        )],
        checksum: None,
        request_id: None,
    };

    let options = StreamOptions {
        address: CLIENT_OPTIONS.hosts[0].clone(),
        connect_timeout: None,
        tls_options: None,
    };

    let mut stream = AsyncStream::connect(options).await.unwrap();
    message.write_to(&mut stream).await.unwrap();

    let reply = Message::read_from(&mut stream).await.unwrap();

    let response_doc = match reply.sections.into_iter().next().unwrap() {
        MessageSection::Document(doc) => doc,
        MessageSection::Sequence { documents, .. } => documents.into_iter().next().unwrap(),
    };

    assert_eq!(response_doc.get("ok"), Some(&Bson::Double(1.0)));
}
