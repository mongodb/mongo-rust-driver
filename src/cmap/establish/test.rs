use tokio::sync::RwLockWriteGuard;

use crate::{
    bson::{doc, Bson},
    cmap::{establish::Handshaker, Command, Connection, ConnectionPoolOptions},
    options::{AuthMechanism, Credential, ReadPreference},
    test::{TestClient, CLIENT_OPTIONS, LOCK},
};

async fn speculative_auth_test(
    client: &TestClient,
    credential: Credential,
    role: impl Into<Bson>,
    authorized_db_name: &str,
) {
    if !client.auth_enabled() {
        return;
    }

    // `createUser` only accepts SCRAM variants in the `mechanisms` array.
    let mechanisms = match credential.mechanism.as_ref() {
        Some(&AuthMechanism::MongoDbX509) | None => Vec::new(),
        Some(other) => vec![other.clone()],
    };

    client
        .drop_and_create_user(
            credential.username.as_deref().unwrap(),
            credential.password.as_deref(),
            &[role.into()],
            &mechanisms,
            credential.source.as_deref(),
        )
        .await
        .unwrap();

    let mut pool_options = ConnectionPoolOptions::builder()
        .credential(credential.clone())
        .build();
    pool_options.tls_options = CLIENT_OPTIONS.tls_options();

    let handshaker = Handshaker::new(Some(pool_options.clone().into()));

    let mut conn = Connection::new_testing(1, Default::default(), 1, Some(pool_options.into()))
        .await
        .unwrap();

    let first_round = handshaker.handshake(&mut conn).await.unwrap().first_round;

    // We expect that the server will return a response with the `speculativeAuthenticate` field if
    // and only if it's new enough to support it.
    assert_eq!(first_round.is_some(), client.server_version_gte(4, 4));

    // Regardless of whether the server supports our speculative authentication attempt, we should
    // be able to successfully authenticate after the handshake.
    credential
        .authenticate_stream(&mut conn, &Default::default(), None, first_round)
        .await
        .unwrap();

    let mut command = Command::new(
        "find".into(),
        authorized_db_name.into(),
        doc! { "find": "foo", "limit": 1  },
    );
    command.set_read_preference(ReadPreference::PrimaryPreferred {
        options: Default::default(),
    });

    let response = conn.send_command(command, None).await.unwrap();

    assert!(response.is_success());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn speculative_auth_default() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;

    let credential = Credential::builder()
        .username(function_name!().to_string())
        .password("12345".to_string())
        .build();

    speculative_auth_test(&client, credential, "root", function_name!()).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn speculative_auth_scram_sha_1() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;

    let credential = Credential::builder()
        .username(function_name!().to_string())
        .password("12345".to_string())
        .mechanism(AuthMechanism::ScramSha1)
        .build();

    speculative_auth_test(&client, credential, "root", function_name!()).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn speculative_auth_scram_sha_256() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;

    if client.server_version_lt(4, 0) {
        return;
    }

    let credential = Credential::builder()
        .username(function_name!().to_string())
        .password("12345".to_string())
        .mechanism(AuthMechanism::ScramSha256)
        .build();

    speculative_auth_test(&client, credential, "root", function_name!()).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn speculative_auth_x509() {
    // Since this test drops and creates the same user as our other X.509 test, we run it
    // exclusively.
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;

    let username = match std::env::var("MONGO_X509_USER") {
        Ok(user) => user,
        Err(_) => return,
    };

    let credential = Credential::builder()
        .username(username)
        .mechanism(AuthMechanism::MongoDbX509)
        .source("$external".to_string())
        .build();

    speculative_auth_test(
        &client,
        credential,
        doc! { "role": "readWrite", "db": function_name!() },
        function_name!(),
    )
    .await;
}
