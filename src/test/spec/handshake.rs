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
    let client_options = get_client_options().await;
    let mut options = EstablisherOptions::from_client_options(client_options);
    options.test_patch_reply = Some(|reply| {
        reply
            .as_mut()
            .unwrap()
            .command_response
            .sasl_supported_mechs
            .get_or_insert_with(Vec::new)
            .push("ArBiTrArY!".to_string());
    });
    let establisher = ConnectionEstablisher::new(options).unwrap();
    let pending = PendingConnection {
        id: 0,
        address: client_options.hosts[0].clone(),
        generation: crate::cmap::PoolGeneration::normal(),
        event_emitter: CmapEventEmitter::new(None, ObjectId::new()),
        time_created: Instant::now(),
        cancellation_receiver: None,
    };
    establisher
        .establish_connection(pending, None)
        .await
        .unwrap();
}
