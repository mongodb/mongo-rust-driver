use std::{future::Future, time::Duration};

use futures::stream::StreamExt;
use tokio::sync::RwLockWriteGuard;

use crate::{
    bson::doc,
    error::{CommandError, ErrorKind},
    options::{
        Acknowledgment,
        ClientOptions,
        CreateCollectionOptions,
        DropCollectionOptions,
        FindOptions,
        InsertManyOptions,
        WriteConcern,
    },
    test::{util::EventClient, LOCK},
    Collection,
    Database,
    RUNTIME,
};

async fn run_test<F: Future>(name: &str, test: impl Fn(EventClient, Database, Collection) -> F) {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    let options = ClientOptions::builder().retry_writes(false).build();
    let client = EventClient::with_additional_options(Some(options), None, None, None).await;

    if !client.is_replica_set() {
        return;
    }

    let name = format!("step-down-{}", name);

    let db = client.database(&name);
    let coll = db.collection(&name);

    let wc_majority = WriteConcern::builder().w(Acknowledgment::Majority).build();

    let _: Result<_, _> = coll
        .drop(Some(
            DropCollectionOptions::builder()
                .write_concern(wc_majority.clone())
                .build(),
        ))
        .await;

    db.create_collection(
        &name,
        Some(
            CreateCollectionOptions::builder()
                .write_concern(wc_majority)
                .build(),
        ),
    )
    .await
    .unwrap();

    test(client, db, coll).await;
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn get_more() {
    async fn get_more_test(client: EventClient, _db: Database, coll: Collection) {
        // This test requires server version 4.2 or higher.
        if client.server_version_lt(4, 2) {
            return;
        }

        let docs = vec![doc! { "x": 1 }; 5];
        coll.insert_many(
            docs,
            Some(
                InsertManyOptions::builder()
                    .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                    .build(),
            ),
        )
        .await
        .unwrap();

        let mut cursor = coll
            .find(None, Some(FindOptions::builder().batch_size(2).build()))
            .await
            .unwrap();

        client
            .database("admin")
            .run_command(doc! { "replSetStepDown": 5, "force": true }, None)
            .await
            .expect("stepdown should have succeeded");

        for _ in 0..5 {
            cursor
                .next()
                .await
                .unwrap()
                .expect("cursor iteration should have succeeded");
        }

        RUNTIME.delay_for(Duration::from_millis(250)).await;
        assert!(client.get_pool_cleared_events().is_empty());
    }

    run_test(function_name!(), get_more_test).await;
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn not_master_keep_pool() {
    async fn not_master_keep_pool_test(client: EventClient, _db: Database, coll: Collection) {
        // This test requires server version 4.2 or higher.
        if client.server_version_lt(4, 2) {
            return;
        }

        client
            .database("admin")
            .run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 10107
                    }
                },
                None,
            )
            .await
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None).await;
        assert!(
            matches!(
                result.as_ref().map_err(|e| &e.kind),
                Err(ErrorKind::CommandError(CommandError { code: 10107, .. }))
            ),
            "insert should have failed"
        );

        coll.insert_one(doc! { "test": 1 }, None)
            .await
            .expect("insert should have succeeded");

        RUNTIME.delay_for(Duration::from_millis(250)).await;
        assert!(client.get_pool_cleared_events().is_empty());
    }

    run_test(function_name!(), not_master_keep_pool_test).await;
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn not_master_reset_pool() {
    async fn not_master_reset_pool_test(client: EventClient, _db: Database, coll: Collection) {
        // This test must only run on 4.0 servers.
        if !client.server_version_eq(4, 0) {
            return;
        }

        client
            .database("admin")
            .run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 10107
                    }
                },
                None,
            )
            .await
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None).await;
        assert!(
            matches!(
                result.as_ref().map_err(|e| &e.kind),
                Err(ErrorKind::CommandError(CommandError { code: 10107, .. }))
            ),
            "insert should have failed"
        );

        RUNTIME.delay_for(Duration::from_millis(250)).await;
        assert!(client.get_pool_cleared_events().len() == 1);

        coll.insert_one(doc! { "test": 1 }, None)
            .await
            .expect("insert should have succeeded");
    }

    run_test(function_name!(), not_master_reset_pool_test).await;
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn shutdown_in_progress() {
    async fn shutdown_in_progress_test(client: EventClient, _db: Database, coll: Collection) {
        if client.server_version_lt(4, 0) {
            return;
        }

        client
            .database("admin")
            .run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 91
                    }
                },
                None,
            )
            .await
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None).await;
        assert!(
            matches!(
                result.as_ref().map_err(|e| &e.kind),
                Err(ErrorKind::CommandError(CommandError { code: 91, .. }))
            ),
            "insert should have failed"
        );

        RUNTIME.delay_for(Duration::from_millis(250)).await;
        assert!(client.get_pool_cleared_events().len() == 1);

        coll.insert_one(doc! { "test": 1 }, None)
            .await
            .expect("insert should have succeeded");
    }

    run_test(function_name!(), shutdown_in_progress_test).await;
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn interrupted_at_shutdown() {
    async fn interrupted_at_shutdown_test(client: EventClient, _db: Database, coll: Collection) {
        if client.server_version_lt(4, 0) {
            return;
        }

        client
            .database("admin")
            .run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 11600
                    }
                },
                None,
            )
            .await
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None).await;
        assert!(
            matches!(
                result.as_ref().map_err(|e| &e.kind),
                Err(ErrorKind::CommandError(CommandError { code: 11600, .. }))
            ),
            "insert should have failed"
        );

        RUNTIME.delay_for(Duration::from_millis(250)).await;
        assert!(client.get_pool_cleared_events().len() == 1);

        coll.insert_one(doc! { "test": 1 }, None)
            .await
            .expect("insert should have succeeded");

        RUNTIME.delay_for(Duration::from_millis(250)).await;
    }

    run_test(function_name!(), interrupted_at_shutdown_test).await;
}
