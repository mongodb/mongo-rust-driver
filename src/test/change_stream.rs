use bson::{doc, Bson, Document};
use futures_util::StreamExt;
use semver::VersionReq;

use crate::{
    change_stream::{
        event::{ChangeStreamEvent, OperationType},
        options::ChangeStreamOptions,
        ChangeStream,
    },
    event::command::{CommandStartedEvent, CommandSucceededEvent},
    test::{CommandEvent, FailCommandOptions, FailPoint, FailPointMode},
    Collection,
};

use super::{EventClient, CLIENT_OPTIONS, LOCK};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn init_stream(
    coll_name: &str,
    direct_connection: bool,
) -> Result<
    Option<(
        EventClient,
        Collection<Document>,
        ChangeStream<ChangeStreamEvent<Document>>,
    )>,
> {
    let mut options = CLIENT_OPTIONS.clone();
    // Direct connection is needed for reliable behavior with fail points.
    if direct_connection {
        options.direct_connection = Some(true);
        options.hosts.drain(1..);
    }
    let client = EventClient::with_options(options).await;
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

    let (client, coll, mut stream) = match init_stream("track_resume_token", false).await? {
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

    let expected_tokens: Vec<_> = client
        .get_command_events(&["aggregate", "getMore"])
        .into_iter()
        .filter_map(|ev| match ev {
            CommandEvent::Succeeded(s) => Some(s),
            _ => None,
        })
        .filter_map(expected_token)
        .collect();
    assert_eq!(tokens, expected_tokens);

    Ok(())
}

fn expected_token(ev: CommandSucceededEvent) -> Option<Bson> {
    let cursor = ev.reply.get_document("cursor").unwrap();
    if let Some(token) = cursor.get("postBatchResumeToken") {
        return Some(token.clone());
    }
    if let Ok(next) = cursor.get_array("nextBatch") {
        return next[0].as_document().unwrap().get("_id").cloned();
    }
    None
}

/// Prose test 2: ChangeStream will throw an exception if the server response is missing the resume
/// token
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn errors_on_missing_token() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (_, coll, _) = match init_stream("errors_on_missing_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut stream = coll
        .watch(vec![doc! { "$project": { "_id": 0 } }], None)
        .await?;
    coll.insert_one(doc! {}, None).await?;
    assert!(stream.next().await.transpose().is_err());

    Ok(())
}

/// Prose test 3: After receiving a resumeToken, ChangeStream will automatically resume one time on
/// a resumable error
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resumes_on_error() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) = match init_stream("resumes_on_error", true).await? {
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
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;

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

/// Prose test 4: ChangeStream will not attempt to resume on any error encountered while executing
/// an aggregate command
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn does_not_resume_aggregate() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, _) = match init_stream("does_not_resume_aggregate", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let _guard = FailPoint::fail_command(
        &["aggregate"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;

    assert!(coll.watch(None, None).await.is_err());

    Ok(())
}

// Prose test 5: removed from spec.

// Prose test 6: ChangeStream will perform server selection before attempting to resume
// The Rust driver cannot *not* perform server selection when executing an operation, including
// resume.

/// Prose test 7: A cursor returned from an aggregate command with a cursor id and an initial empty
/// batch is not closed on the driver side
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn empty_batch_not_closed() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (client, coll, mut stream) = match init_stream("empty_batch_not_closed", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    assert!(stream.next_if_any().await?.is_none());

    coll.insert_one(doc! {}, None).await?;
    stream.next().await.transpose()?;

    let events = client.get_command_events(&["aggregate", "getMore"]);
    let cursor_id = match &events[1] {
        CommandEvent::Succeeded(CommandSucceededEvent { reply, .. }) => {
            reply.get_document("cursor")?.get_i64("id")?
        }
        _ => panic!("unexpected event {:#?}", events[1]),
    };
    let get_more_id = match &events[2] {
        CommandEvent::Started(CommandStartedEvent { command, .. }) => command.get_i64("getMore")?,
        _ => panic!("unexpected event {:#?}", events[2]),
    };
    assert_eq!(cursor_id, get_more_id);

    Ok(())
}

/// Prose test 8: The killCursors command sent during the "Resume Process" must not be allowed to
/// throw an exception
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_kill_cursor_error_suppressed() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) =
        match init_stream("resume_kill_cursor_error_suppressed", true).await? {
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
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;

    let _killcursors_guard = FailPoint::fail_command(
        &["killCursors"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;

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

/// Prose test 9: $changeStream stage for ChangeStream against a server >=4.0 and <4.0.7 that has
/// not received any results yet MUST include a startAtOperationTime option when resuming a change
/// stream.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_start_at_operation_time() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) =
        match init_stream("resume_start_at_operation_time", true).await? {
            Some(t) => t,
            None => return Ok(()),
        };
    if !VersionReq::parse(">=4.0, <4.0.7")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping change stream test due to server version {:?}",
            client.server_version
        );
        return Ok(());
    }

    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    stream.next().await.transpose()?;

    let events = client.get_command_events(&["aggregate"]);
    assert_eq!(events.len(), 4);

    fn has_saot(command: &Document) -> Result<bool> {
        Ok(command.get_array("pipeline")?[0]
            .as_document()
            .unwrap()
            .get_document("$changeStream")?
            .contains_key("startAtOperationTime"))
    }
    assert!(matches!(&events[2],
        CommandEvent::Started(CommandStartedEvent {
            command,
            ..
        }) if has_saot(command)?
    ));

    Ok(())
}

// Prose test 10: removed.

/// Prose test 11: Running against a server >=4.0.7, resume token at the end of a batch must return
/// the postBatchResumeToken from the current command response
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn batch_end_resume_token() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (client, _, mut stream) = match init_stream("batch_end_resume_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.0.7")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping change stream test due to server version {:?}",
            client.server_version
        );
        return Ok(());
    }

    assert_eq!(stream.next_if_any().await?, None);
    let token = stream.resume_token().unwrap().parsed()?;
    let commands = client.get_command_events(&["aggregate", "getMore"]);
    assert!(matches!(commands.last(), Some(
        CommandEvent::Succeeded(CommandSucceededEvent {
            reply,
            ..
        })
    ) if reply.get_document("cursor")?.get("postBatchResumeToken") == Some(&token)));

    Ok(())
}

/// Prose test 12: Running against a server <4.0.7, end of batch resume token must follow the spec
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn batch_end_resume_token_legacy() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (client, coll, mut stream) =
        match init_stream("batch_end_resume_token_legacy", false).await? {
            Some(t) => t,
            None => return Ok(()),
        };
    if !VersionReq::parse("<4.0.7")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping change stream test due to server version {:?}",
            client.server_version
        );
        return Ok(());
    }

    // Case: empty batch, `resume_after` not specified
    assert_eq!(stream.next_if_any().await?, None);
    assert_eq!(stream.resume_token(), None);

    // Case: end of batch
    coll.insert_one(doc! {}, None).await?;
    let expected_id = stream.next_if_any().await?.unwrap().id;
    assert_eq!(stream.next_if_any().await?, None);
    assert_eq!(stream.resume_token().as_ref(), Some(&expected_id));

    // Case: empty batch, `resume_after` specified
    let mut stream = coll
        .watch(
            None,
            ChangeStreamOptions::builder()
                .resume_after(Some(expected_id.clone()))
                .build(),
        )
        .await?;
    assert_eq!(stream.next_if_any().await?, None);
    assert_eq!(stream.resume_token(), Some(expected_id));

    Ok(())
}

/// Prose test 13: Mid-batch resume token must be `_id` of last document returned.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn batch_mid_resume_token() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (_, coll, mut stream) = match init_stream("batch_mid_resume_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    coll.insert_one(doc! {}, None).await?;
    coll.insert_one(doc! {}, None).await?;

    let mid_id = stream.next().await.transpose()?.unwrap().id;
    assert_eq!(stream.resume_token(), Some(mid_id));

    Ok(())
}

/// Prose test 14: Resume token with a non-empty batch for the initial `aggregate` must follow the
/// spec.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn aggregate_batch() -> Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let (client, coll, mut stream) = match init_stream("aggregate_batch", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if client.is_sharded() {
        println!("skipping change stream test on unsupported topology");
        return Ok(());
    }
    if !VersionReq::parse(">=4.2")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        );
        return Ok(());
    }

    // Synthesize a resume token for the new stream to start at.
    coll.insert_one(doc! {}, None).await?;
    stream.next().await;
    let token = stream.resume_token().unwrap();

    // Populate the initial batch of the new stream.
    coll.insert_one(doc! {}, None).await?;
    coll.insert_one(doc! {}, None).await?;

    // Case: `start_after` is given
    let stream = coll
        .watch(
            None,
            ChangeStreamOptions::builder()
                .start_after(Some(token.clone()))
                .build(),
        )
        .await?;
    assert_eq!(stream.resume_token().as_ref(), Some(&token));

    // Case: `resume_after` is given
    let stream = coll
        .watch(
            None,
            ChangeStreamOptions::builder()
                .resume_after(Some(token.clone()))
                .build(),
        )
        .await?;
    assert_eq!(stream.resume_token().as_ref(), Some(&token));

    Ok(())
}

// Prose test 15: removed
// Prose test 16: removed

/// Prose test 17: Resuming a change stream with no results uses `startAfter`.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_uses_start_after() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) = match init_stream("resume_uses_start_after", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.1.1")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        );
        return Ok(());
    }

    coll.insert_one(doc! {}, None).await?;
    stream.next().await.transpose()?;
    let token = stream.resume_token().unwrap();

    let mut stream = coll
        .watch(
            None,
            ChangeStreamOptions::builder()
                .start_after(Some(token.clone()))
                .build(),
        )
        .await?;

    // Create an event, and synthesize a resumable error when calling `getMore` for that event.
    coll.insert_one(doc! {}, None).await?;
    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;
    stream.next().await.transpose()?;

    let commands = client.get_command_started_events(&["aggregate"]);
    fn has_start_after(command: &Document) -> Result<bool> {
        let stage = command.get_array("pipeline")?[0]
            .as_document()
            .unwrap()
            .get_document("$changeStream")?;
        Ok(stage.contains_key("startAfter") && !stage.contains_key("resumeAfter"))
    }
    let last = commands.last().unwrap();
    assert!(
        matches!(last, CommandStartedEvent {
        command,
        ..
    } if has_start_after(command)?),
        "resume mismatch: {:#?}",
        last,
    );

    Ok(())
}

/// Prose test 18: Resuming a change stream after results uses `resumeAfter`.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_uses_resume_after() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, coll, mut stream) = match init_stream("resume_uses_resume_after", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.1.1")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        );
        return Ok(());
    }

    coll.insert_one(doc! {}, None).await?;
    stream.next().await.transpose()?;
    let token = stream.resume_token().unwrap();

    let mut stream = coll
        .watch(
            None,
            ChangeStreamOptions::builder()
                .start_after(Some(token.clone()))
                .build(),
        )
        .await?;

    // Create an event and read it.
    coll.insert_one(doc! {}, None).await?;
    stream.next().await.transpose()?;

    // Create an event, and synthesize a resumable error when calling `getMore` for that event.
    coll.insert_one(doc! {}, None).await?;
    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(43).build(),
    )
    .enable(&client, None)
    .await?;
    stream.next().await.transpose()?;

    let commands = client.get_command_started_events(&["aggregate"]);
    fn has_resume_after(command: &Document) -> Result<bool> {
        let stage = command.get_array("pipeline")?[0]
            .as_document()
            .unwrap()
            .get_document("$changeStream")?;
        Ok(!stage.contains_key("startAfter") && stage.contains_key("resumeAfter"))
    }
    let last = commands.last().unwrap();
    assert!(
        matches!(last, CommandStartedEvent {
        command,
        ..
    } if has_resume_after(command)?),
        "resume mismatch: {:#?}",
        last,
    );

    Ok(())
}
