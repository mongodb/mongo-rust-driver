use bson::{doc, Document};
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::RwLockReadGuard;

use crate::{
    client::options::{ClientOptions, SessionOptions},
    coll::options::CollectionOptions,
    error::Result,
    options::ReadConcern,
    test::{CommandEvent, EventClient, LOCK},
    ClientSession,
    Collection,
};

type OperationFn =
    Box<dyn FnOnce(Collection<Document>, &mut ClientSession) -> BoxFuture<Result<()>>>;

struct Operation {
    name: &'static str,
    fut: OperationFn,
    is_read: bool,
}

impl Operation {
    async fn execute(self, coll: Collection<Document>, session: &mut ClientSession) -> Result<()> {
        (self.fut)(coll, session).await
    }
}

fn for_each_op_with_session_gen() -> impl IntoIterator<Item = Operation> {
    let mut ops = vec![];

    let f: OperationFn = Box::new({
        move |c, s| {
            async move {
                c.insert_one_with_session(doc! { "x": 1 }, None, s)
                    .await
                    .map(|_| ())
            }
            .boxed()
        }
    });
    ops.push(Operation {
        fut: f,
        name: "insert",
        is_read: false,
    });

    ops.push(Operation {
        fut: Box::new({
            move |coll, s| {
                async move {
                    coll.find_one_with_session(doc! { "x": 1 }, None, s)
                        .await
                        .map(|_| ())
                }
                .boxed()
            }
        }),
        name: "find",
        is_read: true,
    });

    ops
}

// // collection operations
// let coll = client
//     .database(test_name)
//     .collection::<bson::Document>(test_name);
// let initial_operation_time = session.operation_time;
// coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)
//     .await
//     .unwrap();
// test_func("insert", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.insert_many_with_session(vec![doc! { "x": 1 }], None, &mut session)
//     .await
//     .unwrap();
// test_func("insert", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.replace_one_with_session(doc! { "x": 1 }, doc! { "x": 2 }, None, &mut session)
//     .await
//     .unwrap();
// test_func("update", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.update_one_with_session(doc! {}, doc! { "$inc": {"x": 5 } }, None, &mut session)
//     .await
//     .unwrap();
// test_func("update", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.update_many_with_session(doc! {}, doc! { "$inc": {"x": 5 } }, None, &mut session)
//     .await
//     .unwrap();
// test_func("update", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.delete_one_with_session(doc! { "x": 1 }, None, &mut session)
//     .await
//     .unwrap();
// test_func("delete", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.delete_many_with_session(doc! { "x": 1 }, None, &mut session)
//     .await
//     .unwrap();
// test_func("delete", &client, &session, initial_operation_time, false);

// let initial_operation_time = session.operation_time;
// coll.find_one_and_delete_with_session(doc! { "x": 1 }, None, &mut session)
//     .await
//     .unwrap();
// test_func(
//     "findAndModify",
//     &client,
//     &session,
//     initial_operation_time,
//     false,
// );

// let initial_operation_time = session.operation_time;
// coll.find_one_and_update_with_session(doc! {}, doc! { "$inc": { "x": 1 } }, None, &mut session)
//     .await
//     .unwrap();
// test_func(
//     "findAndModify",
//     &client,
//     &session,
//     initial_operation_time,
//     false,
// );

// let initial_operation_time = session.operation_time;
// coll.find_one_and_replace_with_session(doc! {}, doc! {"x": 1}, None, &mut session)
//     .await
//     .unwrap();
// test_func(
//     "findAndModify",
//     &client,
//     &session,
//     initial_operation_time,
//     false,
// );

// let initial_operation_time = session.operation_time;
// coll.aggregate_with_session(vec![doc! { "$match": { "x": 1 } }], None, &mut session)
//     .await
//     .unwrap();
// test_func(
//     "aggregate",
//     &client,
//     &session,
//     initial_operation_time,
//     false,
// );

// let initial_operation_time = session.operation_time;
// coll.find_with_session(doc! { "x": 1 }, None, &mut session)
//     .await
//     .unwrap();
// test_func("find", &client, &session, initial_operation_time, true);

// let initial_operation_time = session.operation_time;
// coll.find_one_with_session(doc! { "x": 1 }, None, &mut session)
//     .await
//     .unwrap();
// test_func("find", &client, &session, initial_operation_time, true);

// let initial_operation_time = session.operation_time;
// coll.distinct_with_session("x", None, None, &mut session)
//     .await
//     .unwrap();
// test_func("distinct", &client, &session, initial_operation_time, true);

// let initial_operation_time = session.operation_time;
// coll.count_documents_with_session(None, None, &mut session)
//     .await
//     .unwrap();
// test_func("aggregate", &client, &session, initial_operation_time, true);

// async fn for_each_op_with_session<F>(
//     test_name: &str,
//     test_func: F,
//     client: EventClient,
//     mut session: ClientSession,
// ) where
//     F: Fn(&str, &EventClient, &ClientSession, Option<Timestamp>, bool),
// {
//     // collection operations
//     let coll = client
//         .database(test_name)
//         .collection::<bson::Document>(test_name);
//     let initial_operation_time = session.operation_time;
//     coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("insert", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.insert_many_with_session(vec![doc! { "x": 1 }], None, &mut session)
//         .await
//         .unwrap();
//     test_func("insert", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.replace_one_with_session(doc! { "x": 1 }, doc! { "x": 2 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("update", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.update_one_with_session(doc! {}, doc! { "$inc": {"x": 5 } }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("update", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.update_many_with_session(doc! {}, doc! { "$inc": {"x": 5 } }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("update", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.delete_one_with_session(doc! { "x": 1 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("delete", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.delete_many_with_session(doc! { "x": 1 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("delete", &client, &session, initial_operation_time, false);

//     let initial_operation_time = session.operation_time;
//     coll.find_one_and_delete_with_session(doc! { "x": 1 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func(
//         "findAndModify",
//         &client,
//         &session,
//         initial_operation_time,
//         false,
//     );

//     let initial_operation_time = session.operation_time;
//     coll.find_one_and_update_with_session(doc! {}, doc! { "$inc": { "x": 1 } }, None, &mut
// session)         .await
//         .unwrap();
//     test_func(
//         "findAndModify",
//         &client,
//         &session,
//         initial_operation_time,
//         false,
//     );

//     let initial_operation_time = session.operation_time;
//     coll.find_one_and_replace_with_session(doc! {}, doc! {"x": 1}, None, &mut session)
//         .await
//         .unwrap();
//     test_func(
//         "findAndModify",
//         &client,
//         &session,
//         initial_operation_time,
//         false,
//     );

//     let initial_operation_time = session.operation_time;
//     coll.aggregate_with_session(vec![doc! { "$match": { "x": 1 } }], None, &mut session)
//         .await
//         .unwrap();
//     test_func(
//         "aggregate",
//         &client,
//         &session,
//         initial_operation_time,
//         false,
//     );

//     let initial_operation_time = session.operation_time;
//     coll.find_with_session(doc! { "x": 1 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("find", &client, &session, initial_operation_time, true);

//     let initial_operation_time = session.operation_time;
//     coll.find_one_with_session(doc! { "x": 1 }, None, &mut session)
//         .await
//         .unwrap();
//     test_func("find", &client, &session, initial_operation_time, true);

//     let initial_operation_time = session.operation_time;
//     coll.distinct_with_session("x", None, None, &mut session)
//         .await
//         .unwrap();
//     test_func("distinct", &client, &session, initial_operation_time, true);

//     let initial_operation_time = session.operation_time;
//     coll.count_documents_with_session(None, None, &mut session)
//         .await
//         .unwrap();
//     test_func("aggregate", &client, &session, initial_operation_time, true);
// }

/// Test 1 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn new_session_operation_time_null() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let session = client.start_session(None).await.unwrap();
    assert!(session.operation_time().is_none());
}

/// Test 2 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn first_read_no_after_cluser_time() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let mut session = client
        .start_session(Some(
            SessionOptions::builder().causal_consistency(true).build(),
        ))
        .await
        .unwrap();
    assert!(session.operation_time().is_none());

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| o.is_read)
    {
        let name = op.name;
        op.execute(
            client
                .database("causal_consistency_2")
                .collection("causal_consistency_2"),
            &mut session,
        )
        .await
        .unwrap();
        let (started, _) = client.get_successful_command_execution(name);

        // assert that no read concern was set.
        started.command.get_document("readConcern").unwrap_err();
    }
}

/// Test 3 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn first_op_update_op_time() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let mut session = client
        .start_session(Some(
            SessionOptions::builder().causal_consistency(true).build(),
        ))
        .await
        .unwrap();
    assert!(session.operation_time().is_none());

    for op in for_each_op_with_session_gen() {
        let name = op.name;
        op.execute(
            client
                .database("causal_consistency_3")
                .collection("causal_consistency_3"),
            &mut session,
        )
        .await
        .unwrap();

        let event = client
            .get_command_events(&[name])
            .into_iter()
            .find(|e| matches!(e, CommandEvent::Succeeded(_) | CommandEvent::Failed(_)))
            .unwrap();

        match event {
            CommandEvent::Succeeded(s) => {
                let op_time = s.reply.get_timestamp("operationTime").unwrap();
                assert_eq!(session.operation_time().unwrap(), op_time);
            }
            CommandEvent::Failed(_f) => {
                assert!(session.operation_time().is_some());
            }
            _ => panic!("should have been succeeded or failed"),
        }
    }
}

/// Test 4 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn read_includes_after_cluster_time() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let coll = client
        .database("causal_consistency_4")
        .collection::<Document>("causal_consistency_4");

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| o.is_read)
    {
        let command_name = op.name;
        let mut session = client.start_session(None).await.unwrap();
        coll.find_one_with_session(None, None, &mut session)
            .await
            .unwrap();
        let op_time = session.operation_time().unwrap();
        op.execute(coll.clone(), &mut session).await.unwrap();

        let command_started = client
            .get_command_started_events(&[command_name])
            .pop()
            .unwrap();

        assert_eq!(
            command_started
                .command
                .get_document("readConcern")
                .expect("no readConcern field found in command")
                .get_timestamp("afterClusterTime")
                .expect("no readConcern.afterClusterTime field found in command"),
            op_time,
        );
    }
}

/// Test 5 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn find_after_write_includes_after_cluster_time() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let coll = client
        .database("causal_consistency_5")
        .collection::<Document>("causal_consistency_5");

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| !o.is_read)
    {
        let session_options = SessionOptions::builder().causal_consistency(true).build();
        let mut session = client.start_session(Some(session_options)).await.unwrap();
        op.execute(coll.clone(), &mut session).await.unwrap();
        let op_time = session.operation_time().unwrap();
        coll.find_one_with_session(None, None, &mut session)
            .await
            .unwrap();

        let command_started = client.get_command_started_events(&["find"]).pop().unwrap();
        assert_eq!(
            command_started
                .command
                .get_document("readConcern")
                .expect("no readConcern field found in command")
                .get_timestamp("afterClusterTime")
                .expect("no readConcern.afterClusterTime field found in command"),
            op_time,
        );
    }
}

/// Test 6 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn not_causally_consitent_omits_after_cluster_time() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let coll = client
        .database("causal_consistency_6")
        .collection::<Document>("causal_consistency_6");

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| o.is_read)
    {
        let command_name = op.name;

        let session_options = SessionOptions::builder().causal_consistency(false).build();
        let mut session = client.start_session(Some(session_options)).await.unwrap();
        op.execute(coll.clone(), &mut session).await.unwrap();

        let command_started = client
            .get_command_started_events(&[command_name])
            .pop()
            .unwrap();
        command_started
            .command
            .get_document("readConcern")
            .unwrap_err();
    }
}

/// Test 7 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn omit_after_cluster_time_standalone() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if !client.is_standalone() {
        return;
    }

    let coll = client
        .database("causal_consistency_7")
        .collection::<Document>("causal_consistency_7");

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| o.is_read)
    {
        let command_name = op.name;

        let session_options = SessionOptions::builder().causal_consistency(true).build();
        let mut session = client.start_session(Some(session_options)).await.unwrap();
        op.execute(coll.clone(), &mut session).await.unwrap();

        let command_started = client
            .get_command_started_events(&[command_name])
            .pop()
            .unwrap();
        command_started
            .command
            .get_document("readConcern")
            .unwrap_err();
    }
}

/// Test 8 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn omit_default_read_concern_level() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if client.is_standalone() {
        return;
    }

    let coll = client
        .database("causal_consistency_8")
        .collection::<Document>("causal_consistency_8");

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| o.is_read)
    {
        let command_name = op.name;

        let session_options = SessionOptions::builder().causal_consistency(true).build();
        let mut session = client.start_session(Some(session_options)).await.unwrap();
        coll.find_one_with_session(None, None, &mut session)
            .await
            .unwrap();
        let op_time = session.operation_time().unwrap();
        op.execute(coll.clone(), &mut session).await.unwrap();

        let command_started = client
            .get_command_started_events(&[command_name])
            .pop()
            .unwrap();
        let rc_doc = command_started.command.get_document("readConcern").unwrap();

        rc_doc.get_str("level").unwrap_err();
        assert_eq!(rc_doc.get_timestamp("afterClusterTime").unwrap(), op_time);
    }
}

/// Test 9 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_causal_consistency_read_concern_merge() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let options = ClientOptions::builder()
        .read_concern(ReadConcern::majority())
        .build();
    let client = EventClient::with_options(options).await;
    if client.is_standalone() {
        println!(
            "skipping test_causal_consistency_read_concern_merge due to unsupported topology: \
             standalone"
        );
        return;
    }

    let session_options = SessionOptions::builder().causal_consistency(true).build();
    let mut session = client.start_session(Some(session_options)).await.unwrap();

    let coll_options = CollectionOptions::builder()
        .read_concern(ReadConcern::majority())
        .build();
    let coll = client
        .database("causal_consistency_9")
        .collection_with_options("causal_consistency_9", coll_options);

    for op in for_each_op_with_session_gen()
        .into_iter()
        .filter(|o| o.is_read)
    {
        let command_name = op.name;
        coll.find_one_with_session(None, None, &mut session)
            .await
            .unwrap();
        let op_time = session.operation_time().unwrap();
        op.execute(coll.clone(), &mut session).await.unwrap();

        let command_started = client
            .get_command_started_events(&[command_name])
            .pop()
            .unwrap();
        let rc_doc = command_started
            .command
            .get_document("readConcern")
            .unwrap_or_else(|_| panic!("{} did not include read concern", command_name));

        assert_eq!(rc_doc.get_str("level").unwrap(), "majority");
        assert_eq!(rc_doc.get_timestamp("afterClusterTime").unwrap(), op_time);
    }
}

/// Test 11 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn omit_cluster_time_standalone() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    if !client.is_standalone() {
        println!("skipping omit_cluster_time_standalone due to unsupported topology");
        return;
    }

    let coll = client
        .database("causal_consistency_11")
        .collection::<Document>("causal_consistency_11");

    coll.find_one(None, None).await.unwrap();

    let (started, _) = client.get_successful_command_execution("find");
    started.command.get_document("$clusterTime").unwrap_err();
}

/// Test 12 from the causal consistency specification.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn cluster_time_send_in_commands() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    if client.is_standalone() {
        println!("skipping cluster_time_sent_in_commands due to unsupported topology");
        return;
    }

    let coll = client
        .database("causal_consistency_12")
        .collection::<Document>("causal_consistency_12");

    coll.find_one(None, None).await.unwrap();

    let (started, _) = client.get_successful_command_execution("find");
    started.command.get_document("$clusterTime").unwrap();
}
