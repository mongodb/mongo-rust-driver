use std::time::Duration;

use bson::doc;

use crate::{
    test::{CLIENT_OPTIONS, LOCK},
    Client,
    RUNTIME,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn future_drop_corrupt_issue() {
    let _guard = LOCK.run_concurrently().await;

    let options = CLIENT_OPTIONS.clone();

    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database("test");

    db.collection("foo")
        .insert_one(doc! { "x": 1 }, None)
        .await
        .unwrap();

    let _: Result<_, _> = tokio::time::timeout(
        Duration::from_millis(50),
        db.run_command(
            doc! { "count": "foo",
                "query": {
                    "$where": "sleep(100) && true"
                }
            },
            None,
        ),
    )
    .await;

    RUNTIME.delay_for(Duration::from_millis(200)).await;

    let is_master_response = db.run_command(doc! { "isMaster": 1 }, None).await;

    // it's going to fail because is_master_response contains response for this count command
    // instead of isMaster
    assert!(is_master_response
        .ok()
        .and_then(|value| dbg!(value)
            .get("ismaster")
            .and_then(|value| value.as_bool()))
        .is_some());
}
