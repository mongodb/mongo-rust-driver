use std::time::Instant;

use bson::oid::ObjectId;

use crate::{
    cmap::{
        conn::PendingConnection,
        establish::{ConnectionEstablisher, EstablisherOptions},
    },
    event::cmap::CmapEventEmitter,
    test::get_client_options,
};

// Prose test 1: Test that the driver accepts an arbitrary auth mechanism
#[tokio::test]
async fn arbitrary_auth_mechanism() {
    let options = get_client_options().await.clone();
    let establisher =
        ConnectionEstablisher::new(EstablisherOptions::from_client_options(&options)).unwrap();
    let pending = PendingConnection {
        id: 0,
        address: options.hosts[0].clone(),
        generation: crate::cmap::PoolGeneration::normal(),
        event_emitter: CmapEventEmitter::new(None, ObjectId::new()),
        time_created: Instant::now(),
    };
    let _ = establisher
        .establish_connection(pending, None)
        .await
        .unwrap();
}
