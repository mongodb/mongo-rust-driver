use bson::{Document, doc, Bson};
use futures_util::StreamExt;
use semver::VersionReq;

use crate::{
    test::{CommandEvent, FailPoint, FailPointMode, FailCommandOptions},
    event::command::{CommandSucceededEvent, CommandStartedEvent},
    change_stream::{event::{ChangeStreamEvent, OperationType}, ChangeStream},
    Collection,
};

use super::{LOCK, EventClient};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn init_stream(coll_name: &str) -> Result<Option<(EventClient, Collection<Document>, ChangeStream<ChangeStreamEvent<Document>>)>> {
    let client = EventClient::new().await;
    if !client.is_replica_set() && !client.is_sharded() {
        println!("skipping change stream test on unsupported topology");
        return Ok(None);
    }
    if !client.supports_fail_command() {
        println!("skipping change stream test on version without fail commands");
        return Ok(None);
    }
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>(coll_name);
    coll.drop(None).await?;
    let stream = coll.watch(None, None).await?;
    Ok(Some((client, coll, stream)))
}

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn tracks_resume_token() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (client, coll, mut stream) = match init_stream("track_resume_token").await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let mut tokens = vec![];
    if let Some(token) = stream.resume_token() {
        tokens.push(token.parsed()?);
    }
    for _ in 0..3 {
        coll.insert_one(doc! {}, None).await?;
        stream.next().await.transpose()?;
        tokens.push(stream.resume_token().unwrap().parsed()?);
    }

    let expected_tokens: Vec<_> = client.get_command_events(&["aggregate", "getMore"])
        .into_iter()
        .filter_map(|ev| {
            match ev {
                CommandEvent::Succeeded(s) => Some(s),
                _ => None,
            }
        })
        .filter_map(expected_token)
        .collect();
    assert_eq!(tokens, expected_tokens);

    Ok(())
}

fn expected_token(ev: CommandSucceededEvent) -> Option<Bson> {
    let cursor = ev.reply.get_document("cursor").unwrap();
    if let Some(token) = cursor.get("postBatchResumeToken") {
        return Some(token.clone())
    }
    if let Ok(next) = cursor.get_array("nextBatch") {
        return next[0]
            .as_document()
            .unwrap()
            .get("_id")
            .cloned()
    }
    None
}

/// Prose test 2: ChangeStream will throw an exception if the server response is missing the resume token
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn errors_on_missing_token() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (_, coll, _) = match init_stream("errors_on_missing_token").await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut stream = coll.watch(vec![
        doc! { "$project": { "_id": 0 } },
    ], None).await?;
    coll.insert_one(doc! {}, None).await?;
    assert!(stream.next().await.transpose().is_err());

    Ok(())
}

/// Prose test 3: After receiving a resumeToken, ChangeStream will automatically resume one time on a resumable error
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resumes_on_error() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) = match init_stream("resumes_on_error").await? {
        Some(t) => t,
        None => return Ok(()),
    };

    coll.insert_one(doc! { "_id": 1 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    Ok(())
}

/// Prose test 4: ChangeStream will not attempt to resume on any error encountered while executing an aggregate command
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn does_not_resume_aggregate() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, _) = match init_stream("does_not_resume_aggregate").await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let _guard = FailPoint::fail_command(
        &["aggregate"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    assert!(coll.watch(None, None).await.is_err());

    Ok(())
}

// Prose test 5: removed from spec.

// TODO aegnor:
// Prose test 6: ChangeStream will perform server selection before attempting to resume, using initial readPreference

/// Prose test 7: A cursor returned from an aggregate command with a cursor id and an initial empty batch is not closed on the driver side
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn empty_batch_not_closed() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (client, coll, mut stream) = match init_stream("empty_batch_not_closed").await? {
        Some(t) => t,
        None => return Ok(()),
    };

    assert!(stream.next_if_any().await?.is_none());

    coll.insert_one(doc! { }, None).await?;
    stream.next().await.transpose()?;

    let events = client.get_command_events(&["aggregate", "getMore"]);
    assert_eq!(events.len(), 4);
    let cursor_id = match &events[1] {
        CommandEvent::Succeeded(CommandSucceededEvent {
            reply, ..
        }) => reply.get_document("cursor")?.get_i64("id")?,
        _ => panic!("unexpected event {:#?}", events[1]),
    };
    let get_more_id = match &events[2] {
        CommandEvent::Started(CommandStartedEvent {
            command, ..
        }) => command.get_i64("getMore")?,
        _ => panic!("unexpected event {:#?}", events[2]),
    };
    assert_eq!(cursor_id, get_more_id);

    Ok(())
}

/// Prose test 8: The killCursors command sent during the "Resume Process" must not be allowed to throw an exception
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_kill_cursor_error_suppressed() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) = match init_stream("resume_kill_cursor_error_suppressed").await? {
        Some(t) => t,
        None => return Ok(()),
    };

    coll.insert_one(doc! { "_id": 1 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let _getmore_guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    let _killcursors_guard = FailPoint::fail_command(
        &["killCursors"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    Ok(())
}

/// Prose test 9: $changeStream stage for ChangeStream against a server >=4.0 and <4.0.7 that has not received any results yet MUST include a startAtOperationTime option when resuming a change stream.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_start_at_operation_time() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) = match init_stream("resume_start_at_operation_time").await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.0, <4.0.7").unwrap().matches(&client.server_version) {
        println!("skipping change stream test due to server version {:?}", client.server_version);
        return Ok(());
    }

    coll.insert_one(doc! { "_id": 1 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    Ok(())
}