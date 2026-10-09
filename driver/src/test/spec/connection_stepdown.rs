use std::{future::Future, time::Duration};

use futures::stream::StreamExt;

use crate::{
    bson::{doc, Document},
    options::{ReadPreference, WriteConcern},
    test::{
        get_client_options,
        log_uncaptured,
        topology_is_replica_set,
        util::fail_point::{FailPoint, FailPointMode},
        EventClient,
    },
    Collection,
    Database,
};

async fn run_test<F: Future>(
    name: &str,
    test: impl Fn(EventClient, Database, Collection<Document>) -> F,
) {
    if !topology_is_replica_set().await {
        log_uncaptured(format!(
            "skipping test {name:?} due to not running on a replica set"
        ));
        return;
    }

    let mut options = get_client_options().await.clone();
    options.retry_writes = Some(false);
    options.heartbeat_freq = Some(Duration::from_millis(500));
    let client = crate::Client::for_test()
        .options(options)
        .use_single_mongos()
        .monitor_events()
        .await;

    let name = format!("step-down-{name}");

    let db = client.database(&name);
    let coll = db.collection(&name);

    let _ = coll.drop().write_concern(WriteConcern::majority()).await;

    db.create_collection(&name)
        .write_concern(WriteConcern::majority())
        .await
        .unwrap();

    test(client, db, coll).await;
}

#[tokio::test]
async fn get_more() {
    async fn get_more_test(client: EventClient, _db: Database, coll: Collection<Document>) {
        let docs = vec![doc! { "x": 1 }; 5];
        coll.insert_many(docs)
            .write_concern(WriteConcern::majority())
            .await
            .unwrap();

        let mut cursor = coll.find(doc! {}).batch_size(2).await.unwrap();

        let db = client.database("admin");

        db.run_command(doc! { "replSetFreeze": 0 })
            .selection_criteria(ReadPreference::secondary(None).into())
            .await
            .expect("replSetFreeze should have succeeded");

        db.run_command(doc! { "replSetStepDown": 30, "force": true })
            .await
            .expect("stepdown should have succeeded");

        while cursor.advance().await.unwrap() {
            cursor
                .next()
                .await
                .unwrap()
                .expect("cursor iteration should have succeeded");
        }

        client.sync_workers().await;
        assert_eq!(client.events.count_pool_cleared_events(), 0);
    }

    run_test("get_more", get_more_test).await;
}

#[tokio::test]
async fn notwritableprimary_keep_pool() {
    async fn notwritableprimary_keep_pool_test(
        client: EventClient,
        _db: Database,
        coll: Collection<Document>,
    ) {
        let error_code = 10107;
        let fail_point =
            FailPoint::fail_command(&["insert"], FailPointMode::Times(1)).error_code(error_code);
        let _guard = client.enable_fail_point(fail_point).await.unwrap();

        let error = coll.insert_one(doc! { "test": 1 }).await.unwrap_err();
        assert_eq!(error.code(), Some(error_code as i32));

        coll.insert_one(doc! { "test": 1 })
            .await
            .expect("insert should have succeeded");

        client.sync_workers().await;
        assert_eq!(client.events.count_pool_cleared_events(), 0);
    }

    run_test(
        "notwritableprimary_keep_pool",
        notwritableprimary_keep_pool_test,
    )
    .await;
}

#[tokio::test]
async fn shutdown_in_progress() {
    async fn shutdown_in_progress_test(
        client: EventClient,
        _db: Database,
        coll: Collection<Document>,
    ) {
        let error_code = 91;
        let fail_point =
            FailPoint::fail_command(&["insert"], FailPointMode::Times(1)).error_code(error_code);
        let _guard = client.enable_fail_point(fail_point).await.unwrap();

        let error = coll.insert_one(doc! { "test": 1 }).await.unwrap_err();
        assert_eq!(error.code(), Some(error_code as i32));

        client.sync_workers().await;
        assert_eq!(client.events.count_pool_cleared_events(), 1);

        coll.insert_one(doc! { "test": 1 })
            .await
            .expect("insert should have succeeded");
    }

    run_test("shutdown_in_progress", shutdown_in_progress_test).await;
}

#[tokio::test]
async fn interrupted_at_shutdown() {
    async fn interrupted_at_shutdown_test(
        client: EventClient,
        _db: Database,
        coll: Collection<Document>,
    ) {
        let error_code = 11600;
        let fail_point =
            FailPoint::fail_command(&["insert"], FailPointMode::Times(1)).error_code(error_code);
        let _guard = client.enable_fail_point(fail_point).await.unwrap();

        let error = coll.insert_one(doc! { "test": 1 }).await.unwrap_err();
        assert_eq!(error.code(), Some(error_code as i32));

        client.sync_workers().await;
        assert_eq!(client.events.count_pool_cleared_events(), 1);

        coll.insert_one(doc! { "test": 1 })
            .await
            .expect("insert should have succeeded");
    }

    run_test("interrupted_at_shutdown", interrupted_at_shutdown_test).await;
}
