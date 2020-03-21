use bson::doc;

use crate::{
    error::{CommandError, ErrorKind},
    options::{
        Acknowledgment,
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

fn run_test(name: &str, test: impl Fn(EventClient, Database, Collection)) {
    // TODO RUST-51: Disable retryable writes once they're implemented.
    let client = RUNTIME.block_on(EventClient::new());

    if client.options.repl_set_name.is_none() {
        return;
    }

    let name = format!("step-down-{}", name);

    let db = client.database(&name);
    let coll = db.collection(&name);

    let wc_majority = WriteConcern::builder().w(Acknowledgment::Majority).build();

    let _ = coll.drop(Some(
        DropCollectionOptions::builder()
            .write_concern(wc_majority.clone())
            .build(),
    ));

    RUNTIME
        .block_on(
            db.create_collection(
                &name,
                Some(
                    CreateCollectionOptions::builder()
                        .write_concern(wc_majority)
                        .build(),
                ),
            ),
        )
        .unwrap();

    test(client, db, coll);
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn get_more() {
    run_test(function_name!(), |client, db, coll| {
        // This test requires server version 4.2 or higher.
        if client.server_version_lt(4, 2) {
            return;
        }

        let _guard = LOCK.run_concurrently();

        let docs = vec![doc! { "x": 1 }; 5];
        coll.insert_many(
            docs,
            Some(
                InsertManyOptions::builder()
                    .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                    .build(),
            ),
        )
        .unwrap();

        let mut cursor = coll
            .find(None, Some(FindOptions::builder().batch_size(2).build()))
            .unwrap();

        RUNTIME
            .block_on(db.run_command(doc! { "replSetStepDown": 5, "force": true }, None))
            .expect("stepdown should have succeeded");

        for _ in 0..5 {
            cursor
                .next()
                .unwrap()
                .expect("cursor iteration should have succeeded");
        }

        assert!(client.pool_cleared_events.read().unwrap().is_empty());
    });
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn not_master_keep_pool() {
    run_test(function_name!(), |client, _, coll| {
        // This test requires server version 4.2 or higher.
        if client.server_version_lt(4, 2) {
            return;
        }

        let _guard = LOCK.run_exclusively();

        RUNTIME
            .block_on(client.database("admin").run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 10107
                    }
                },
                None,
            ))
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None);
        assert!(
            matches!(
                result.as_ref().map_err(|e| e.as_ref()),
                Err(ErrorKind::CommandError(CommandError { code: 10107, .. }))
            ),
            "insert should have failed"
        );

        coll.insert_one(doc! { "test": 1 }, None)
            .expect("insert should have succeeded");

        assert!(client.pool_cleared_events.read().unwrap().is_empty());
    });
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn not_master_reset_pool() {
    run_test(function_name!(), |client, _, coll| {
        // This test must only run on 4.0 servers.
        if !client.server_version_eq(4, 0) {
            return;
        }

        let _guard = LOCK.run_exclusively();

        RUNTIME
            .block_on(client.database("admin").run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 10107
                    }
                },
                None,
            ))
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None);
        assert!(
            matches!(
                result.as_ref().map_err(|e| e.as_ref()),
                Err(ErrorKind::CommandError(CommandError { code: 10107, .. }))
            ),
            "insert should have failed"
        );

        assert!(client.pool_cleared_events.read().unwrap().len() == 1);

        coll.insert_one(doc! { "test": 1 }, None)
            .expect("insert should have succeeded");
    });
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn shutdown_in_progress() {
    run_test(function_name!(), |client, _, coll| {
        if client.server_version_lt(4, 0) {
            return;
        }

        let _guard = LOCK.run_exclusively();

        RUNTIME
            .block_on(client.database("admin").run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 91
                    }
                },
                None,
            ))
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None);
        assert!(
            matches!(
                result.as_ref().map_err(|e| e.as_ref()),
                Err(ErrorKind::CommandError(CommandError { code: 91, .. }))
            ),
            "insert should have failed"
        );

        assert!(client.pool_cleared_events.read().unwrap().len() == 1);

        coll.insert_one(doc! { "test": 1 }, None)
            .expect("insert should have succeeded");
    })
}

#[function_name::named]
#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn interrupted_at_shutdown() {
    run_test(function_name!(), |client, _, coll| {
        if client.server_version_lt(4, 0) {
            return;
        }

        let _guard = LOCK.run_exclusively();

        RUNTIME
            .block_on(client.database("admin").run_command(
                doc! {
                    "configureFailPoint": "failCommand",
                    "mode": { "times": 1 },
                    "data": {
                        "failCommands": ["insert"],
                        "errorCode": 11600
                    }
                },
                None,
            ))
            .unwrap();

        let result = coll.insert_one(doc! { "test": 1 }, None);
        assert!(
            matches!(
                result.as_ref().map_err(|e| e.as_ref()),
                Err(ErrorKind::CommandError(CommandError { code: 11600, .. }))
            ),
            "insert should have failed"
        );

        assert!(client.pool_cleared_events.read().unwrap().len() == 1);

        coll.insert_one(doc! { "test": 1 }, None)
            .expect("insert should have succeeded");
    })
}
