use bson::{doc, Bson, Document};
use futures_util::{StreamExt, TryStreamExt};
use semver::VersionReq;

use crate::{
    change_stream::{
        event::{ChangeStreamEvent, OperationType},
        options::FullDocumentBeforeChangeType,
        ChangeStream,
    },
    coll::options::CollectionOptions,
    db::options::ChangeStreamPreAndPostImages,
    event::command::{CommandEvent, CommandStartedEvent, CommandSucceededEvent},
    options::{Acknowledgment, WriteConcern},
    test::util::fail_point::{FailPoint, FailPointMode},
    Client,
    Collection,
};

use super::{get_client_options, log_uncaptured, EventClient, TestClient};

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
    let init_client = TestClient::new().await;
    if !init_client.is_replica_set() && !init_client.is_sharded() {
        log_uncaptured("skipping change stream test on unsupported topology");
        return Ok(None);
    }
    if !init_client.supports_fail_command() {
        log_uncaptured("skipping change stream test on version without fail commands");
        return Ok(None);
    }

    let mut options = get_client_options().await.clone();
    // Direct connection is needed for reliable behavior with fail points.
    if direct_connection && init_client.is_sharded() {
        options.direct_connection = Some(true);
        options.hosts.drain(1..);
    }
    let client = Client::test_builder()
        .options(options)
        .monitor_events()
        .build()
        .await;
    let db = client.database("change_stream_tests");
    let coll = db.collection_with_options::<Document>(
        coll_name,
        CollectionOptions::builder()
            .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
            .build(),
    );
    coll.drop().await?;
    let stream = coll.watch().await?;
    Ok(Some((client, coll, stream)))
}

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[tokio::test]
async fn tracks_resume_token() -> Result<()> {
    let (client, coll, mut stream) = match init_stream("track_resume_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let mut tokens = vec![];
    if let Some(token) = stream.resume_token() {
        tokens.push(token.parsed()?);
    }
    for _ in 0..3 {
        coll.insert_one(doc! {}).await?;
        stream.next().await.transpose()?;
        tokens.push(stream.resume_token().unwrap().parsed()?);
    }

    #[allow(deprecated)]
    let events: Vec<_> = {
        let mut events = client.events.clone();
        events
            .get_command_events(&["aggregate", "getMore"])
            .into_iter()
            .filter_map(|ev| match ev {
                CommandEvent::Succeeded(s) => Some(s),
                _ => None,
            })
            .collect()
    };
    let mut expected = vec![];
    // Token from `aggregate`
    if let Some(initial) = events[0]
        .reply
        .get_document("cursor")?
        .get("postBatchResumeToken")
    {
        expected.push(initial.clone());
    }
    // Tokens from `getMore`s
    expected.extend(expected_tokens(&events[1..])?);
    assert_eq!(tokens, expected);

    Ok(())
}

fn expected_tokens(events: &[CommandSucceededEvent]) -> Result<Vec<Bson>> {
    let mut out = vec![];
    for event in events {
        let cursor = event.reply.get_document("cursor")?;
        if let Ok(next) = cursor.get_array("nextBatch") {
            // Tokens from results within the batch
            if next.len() > 1 {
                for doc in &next[0..next.len() - 1] {
                    out.push(doc.as_document().unwrap().get("_id").unwrap().clone());
                }
            }
            // Final token, if this wasn't an empty `getMore`
            if !next.is_empty() {
                if let Some(last) = cursor
                    .get("postBatchResumeToken")
                    .or_else(|| next.last().unwrap().as_document().unwrap().get("_id"))
                    .cloned()
                {
                    out.push(last);
                }
            }
        }
    }
    Ok(out)
}

/// Prose test 2: ChangeStream will throw an exception if the server response is missing the resume
/// token
#[tokio::test]
async fn errors_on_missing_token() -> Result<()> {
    let (_, coll, _) = match init_stream("errors_on_missing_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut stream = coll
        .watch()
        .pipeline(vec![doc! { "$project": { "_id": 0 } }])
        .await?;
    coll.insert_one(doc! {}).await?;
    assert!(stream.next().await.transpose().is_err());

    Ok(())
}

/// Prose test 3: After receiving a resumeToken, ChangeStream will automatically resume one time on
/// a resumable error
#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn resumes_on_error() -> Result<()> {
    let (client, coll, mut stream) = match init_stream("resumes_on_error", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    coll.insert_one(doc! { "_id": 1 }).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let fail_point = FailPoint::fail_command(&["getMore"], FailPointMode::Times(1)).error_code(43);
    let _guard = client.enable_fail_point(fail_point).await?;

    coll.insert_one(doc! { "_id": 2 }).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    // Assert that two `aggregate`s were issued, i.e. that a resume happened.
    let events = client.events.get_command_started_events(&["aggregate"]);
    assert_eq!(events.len(), 2);

    Ok(())
}

/// Prose test 4: ChangeStream will not attempt to resume on any error encountered while executing
/// an aggregate command
#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn does_not_resume_aggregate() -> Result<()> {
    let (client, coll, _) = match init_stream("does_not_resume_aggregate", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let fail_point =
        FailPoint::fail_command(&["aggregate"], FailPointMode::Times(1)).error_code(43);
    let _guard = client.enable_fail_point(fail_point).await?;

    assert!(coll.watch().await.is_err());

    Ok(())
}

// Prose test 5: removed from spec.

// Prose test 6: ChangeStream will perform server selection before attempting to resume
// The Rust driver cannot *not* perform server selection when executing an operation, including
// resume.

/// Prose test 7: A cursor returned from an aggregate command with a cursor id and an initial empty
/// batch is not closed on the driver side
#[tokio::test]
async fn empty_batch_not_closed() -> Result<()> {
    let (client, coll, mut stream) = match init_stream("empty_batch_not_closed", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    assert!(stream.next_if_any().await?.is_none());

    coll.insert_one(doc! {}).await?;
    stream.next().await.transpose()?;

    #[allow(deprecated)]
    let events = {
        let mut events = client.events.clone();
        events.get_command_events(&["aggregate", "getMore"])
    };
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
#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn resume_kill_cursor_error_suppressed() -> Result<()> {
    let (client, coll, mut stream) =
        match init_stream("resume_kill_cursor_error_suppressed", true).await? {
            Some(t) => t,
            None => return Ok(()),
        };

    coll.insert_one(doc! { "_id": 1 }).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let fail_point = FailPoint::fail_command(&["getMore", "killCursors"], FailPointMode::Times(1))
        .error_code(43);
    let _guard = client.enable_fail_point(fail_point).await?;

    coll.insert_one(doc! { "_id": 2 }).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    // Assert that two `aggregate`s were issued, i.e. that a resume happened.
    let events = client.events.get_command_started_events(&["aggregate"]);
    assert_eq!(events.len(), 2);

    Ok(())
}

/// Prose test 9: $changeStream stage for ChangeStream against a server >=4.0 and <4.0.7 that has
/// not received any results yet MUST include a startAtOperationTime option when resuming a change
/// stream.
#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn resume_start_at_operation_time() -> Result<()> {
    let (client, coll, mut stream) =
        match init_stream("resume_start_at_operation_time", true).await? {
            Some(t) => t,
            None => return Ok(()),
        };
    if !VersionReq::parse(">=4.0, <4.0.7")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test due to server version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    let fail_point = FailPoint::fail_command(&["getMore"], FailPointMode::Times(1)).error_code(43);
    let _guard = client.enable_fail_point(fail_point).await?;

    coll.insert_one(doc! { "_id": 2 }).await?;
    stream.next().await.transpose()?;

    #[allow(deprecated)]
    let events = {
        let mut events = client.events.clone();
        events.get_command_events(&["aggregate"])
    };
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
#[tokio::test]
async fn batch_end_resume_token() -> Result<()> {
    let (client, _, mut stream) = match init_stream("batch_end_resume_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.0.7")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test due to server version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    assert_eq!(stream.next_if_any().await?, None);
    let token = stream.resume_token().unwrap().parsed()?;
    #[allow(deprecated)]
    let commands = {
        let mut events = client.events.clone();
        events.get_command_events(&["aggregate", "getMore"])
    };
    assert!(matches!(commands.last(), Some(
        CommandEvent::Succeeded(CommandSucceededEvent {
            reply,
            ..
        })
    ) if reply.get_document("cursor")?.get("postBatchResumeToken") == Some(&token)));

    Ok(())
}

/// Prose test 12: Running against a server <4.0.7, end of batch resume token must follow the spec
#[tokio::test]
async fn batch_end_resume_token_legacy() -> Result<()> {
    let (client, coll, mut stream) =
        match init_stream("batch_end_resume_token_legacy", false).await? {
            Some(t) => t,
            None => return Ok(()),
        };
    if !VersionReq::parse("<4.0.7")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test due to server version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    // Case: empty batch, `resume_after` not specified
    assert_eq!(stream.next_if_any().await?, None);
    assert_eq!(stream.resume_token(), None);

    // Case: end of batch
    coll.insert_one(doc! {}).await?;
    let expected_id = stream.next_if_any().await?.unwrap().id;
    assert_eq!(stream.next_if_any().await?, None);
    assert_eq!(stream.resume_token().as_ref(), Some(&expected_id));

    // Case: empty batch, `resume_after` specified
    let mut stream = coll.watch().resume_after(expected_id.clone()).await?;
    assert_eq!(stream.next_if_any().await?, None);
    assert_eq!(stream.resume_token(), Some(expected_id));

    Ok(())
}

/// Prose test 13: Mid-batch resume token must be `_id` of last document returned.
#[tokio::test]
async fn batch_mid_resume_token() -> Result<()> {
    let (_, coll, mut stream) = match init_stream("batch_mid_resume_token", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };

    // This loop gets the stream to a point where it has been iterated up to but not including
    // the last event in its batch.
    let mut event_id = None;
    loop {
        match stream.next_if_any().await? {
            Some(event) => {
                event_id = Some(event.id);
            }
            // If we're out of events, make some more.
            None => {
                coll.insert_many((0..3).map(|_| doc! {})).await?;
            }
        };

        // if after iterating the stream last time there's one document left,
        // then we're done here and can continue to the assertions.
        if stream.current_batch().len() == 1 {
            break;
        }
    }

    assert_eq!(stream.resume_token().unwrap(), event_id.unwrap());

    Ok(())
}

/// Prose test 14: Resume token with a non-empty batch for the initial `aggregate` must follow the
/// spec.
#[tokio::test]
async fn aggregate_batch() -> Result<()> {
    let (client, coll, mut stream) = match init_stream("aggregate_batch", false).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if client.is_sharded() {
        log_uncaptured("skipping change stream test on unsupported topology");
        return Ok(());
    }
    if !VersionReq::parse(">=4.2")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    // Synthesize a resume token for the new stream to start at.
    coll.insert_one(doc! {}).await?;
    stream.next().await;
    let token = stream.resume_token().unwrap();

    // Populate the initial batch of the new stream.
    coll.insert_one(doc! {}).await?;
    coll.insert_one(doc! {}).await?;

    // Case: `start_after` is given
    let stream = coll.watch().start_after(token.clone()).await?;
    assert_eq!(stream.resume_token().as_ref(), Some(&token));

    // Case: `resume_after` is given
    let stream = coll.watch().resume_after(token.clone()).await?;
    assert_eq!(stream.resume_token().as_ref(), Some(&token));

    Ok(())
}

// Prose test 15: removed
// Prose test 16: removed

/// Prose test 17: Resuming a change stream with no results uses `startAfter`.
#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn resume_uses_start_after() -> Result<()> {
    let (client, coll, mut stream) = match init_stream("resume_uses_start_after", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.1.1")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    coll.insert_one(doc! {}).await?;
    stream.next().await.transpose()?;
    let token = stream.resume_token().unwrap();

    let mut stream = coll.watch().start_after(token.clone()).await?;

    // Create an event, and synthesize a resumable error when calling `getMore` for that event.
    coll.insert_one(doc! {}).await?;
    let fail_point = FailPoint::fail_command(&["getMore"], FailPointMode::Times(1)).error_code(43);
    let _guard = client.enable_fail_point(fail_point).await?;
    stream.next().await.transpose()?;

    let commands = client.events.get_command_started_events(&["aggregate"]);
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
#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn resume_uses_resume_after() -> Result<()> {
    let (client, coll, mut stream) = match init_stream("resume_uses_resume_after", true).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    if !VersionReq::parse(">=4.1.1")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    coll.insert_one(doc! {}).await?;
    stream.next().await.transpose()?;
    let token = stream.resume_token().unwrap();

    let mut stream = coll.watch().start_after(token.clone()).await?;

    // Create an event and read it.
    coll.insert_one(doc! {}).await?;
    stream.next().await.transpose()?;

    // Create an event, and synthesize a resumable error when calling `getMore` for that event.
    coll.insert_one(doc! {}).await?;
    let fail_point = FailPoint::fail_command(&["getMore"], FailPointMode::Times(1)).error_code(43);
    let _guard = client.enable_fail_point(fail_point).await?;
    stream.next().await.transpose()?;

    let commands = client.events.get_command_started_events(&["aggregate"]);
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

#[tokio::test]
async fn create_coll_pre_post() -> Result<()> {
    let client = TestClient::new().await;
    if !VersionReq::parse(">=6.0")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(format!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        ));
        return Ok(());
    }

    let db = client.database("create_coll_pre_post");
    db.collection::<Document>("test").drop().await?;
    db.create_collection("test")
        .change_stream_pre_and_post_images(ChangeStreamPreAndPostImages { enabled: true })
        .await?;

    Ok(())
}

// Prose test 19: large event splitting
#[tokio::test]
async fn split_large_event() -> Result<()> {
    let client = Client::test_builder().build().await;
    if !(client.server_version_matches(">= 6.0.9, < 6.1")
        || client.server_version_matches(">= 7.0"))
    {
        log_uncaptured(format!(
            "skipping change stream test on unsupported version {:?}",
            client.server_version
        ));
        return Ok(());
    }
    if !client.is_replica_set() && !client.is_sharded() {
        log_uncaptured("skipping change stream test on unsupported topology");
        return Ok(());
    }

    let db = client.database("change_stream_tests");
    db.collection::<Document>("split_large_event")
        .drop()
        .await?;
    db.create_collection("split_large_event")
        .change_stream_pre_and_post_images(ChangeStreamPreAndPostImages { enabled: true })
        .await?;

    let coll = db.collection::<Document>("split_large_event");
    coll.insert_one(doc! { "value": "q".repeat(10 * 1024 * 1024) })
        .await?;
    let stream = coll
        .watch()
        .pipeline([doc! { "$changeStreamSplitLargeEvent": {} }])
        .full_document_before_change(FullDocumentBeforeChangeType::Required)
        .await?
        .with_type::<Document>();
    coll.update_one(
        doc! {},
        doc! { "$set": { "value": "z".repeat(10 * 1024 * 1024) } },
    )
    .await?;

    let events: Vec<_> = stream.take(2).try_collect().await?;
    assert_eq!(2, events.len());
    assert_eq!(
        events[0].get_document("splitEvent")?,
        &doc! { "fragment": 1, "of": 2 },
    );
    assert_eq!(
        events[1].get_document("splitEvent")?,
        &doc! { "fragment": 2, "of": 2 },
    );

    Ok(())
}
