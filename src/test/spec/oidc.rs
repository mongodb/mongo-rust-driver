macro_rules! get_env_or_skip {
    ( $env_var: literal ) => {
        match std::env::var($env_var) {
            Ok(val) => val,
            Err(_) => {
                use crate::test::log_uncaptured;
                log_uncaptured(&format!("Skipping test, {} not set", $env_var));
                return Ok(());
            }
        }
    };
}

macro_rules! mongodb_uri_admin {
    () => {
        get_env_or_skip!("MONGODB_URI")
    };
}

macro_rules! mongodb_uri_single {
    () => {
        get_env_or_skip!("MONGODB_URI_SINGLE")
    };
}

macro_rules! mongodb_uri_multi {
    () => {
        get_env_or_skip!("MONGODB_URI_MULTI")
    };
}

macro_rules! token_dir {
    ( $user_name: literal ) => {
        // this cannot use get_env_or_skip because it is used in the callback
        format!(
            "{}/{}",
            std::env::var("OIDC_TOKEN_DIR").unwrap_or_else(|_| "/tmp/tokens".to_string()),
            $user_name
        )
    };
}

macro_rules! no_user_token_file {
    () => {
        // this cannot use get_env_or_skip because it is used in the callback
        std::env::var("OIDC_TOKEN_FILE").unwrap()
    };
}

macro_rules! explicit_user {
    ( $user_name: literal ) => {
        format!("{}@{}", $user_name, get_env_or_skip!("OIDC_DOMAIN"),)
    };
}

macro_rules! admin_client {
    () => {{
        let opts = crate::client::options::ClientOptions::parse(mongodb_uri_admin!())
            .await
            .unwrap();
        crate::Client::with_options(opts).unwrap()
    }};
}

mod basic {
    use crate::{
        client::auth::{oidc, AuthMechanism, Credential},
        options::ClientOptions,
        test::util::fail_point::{FailPoint, FailPointMode},
        Client,
    };
    use bson::{doc, Document};
    use futures_util::FutureExt;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::sync::Mutex;

    // Machine Callback tests
    #[tokio::test]
    async fn machine_1_1_callback_is_called() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential.as_mut().unwrap().source = None;
        // test the new public API here.
        opts.credential.as_mut().unwrap().oidc_callback =
            crate::options::oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse::builder()
                        .access_token(tokio::fs::read_to_string(token_dir!("test_user1")).await?)
                        .build())
                }
                .boxed()
            });
        let client = Client::with_options(opts)?;

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn machine_1_2_callback_is_called_only_once_for_multiple_connections(
    ) -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let mut handles = Vec::with_capacity(10);
        for _ in 0..10 {
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    client
                        .database("test")
                        .collection::<Document>("test")
                        .find_one(doc! {})
                        .await
                        .unwrap();
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn machine_2_1_valid_callback_inputs() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |c| {
                let call_count = cb_call_count.clone();
                assert!(c.refresh_token.is_none());
                // timeout should be in the future
                assert!(c.timeout.unwrap() >= Instant::now());
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    // 2.2 callback returns null, but that is impossible with the rust type system

    #[tokio::test]
    async fn machine_2_3_oidc_callback_return_missing_data() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: "".to_string(),
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn machine_2_4_invalid_client_configuration_with_callback() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        use crate::client::auth::{ENVIRONMENT_PROP_STR, TOKEN_RESOURCE_PROP_STR};
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .mechanism_properties(
                doc! {ENVIRONMENT_PROP_STR: "test", TOKEN_RESOURCE_PROP_STR: "test"},
            )
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::InvalidArgument { .. },
        ));
        Ok(())
    }

    #[tokio::test]
    async fn machine_3_1_failure_with_cached_tokens_fetch_a_new_token_and_retry_auth(
    ) -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        // poison the cache with a bad token, authentication should still work.
        opts.credential
            .as_mut()
            .unwrap()
            .oidc_callback
            .set_access_token(Some("random happy sunshine token".to_string()))
            .await;
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn machine_3_2_auth_failures_without_cached_tokens_returns_an_error() -> anyhow::Result<()>
    {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: "bad token".to_string(),
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn machine_4_reauthentication() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        let admin_client = admin_client!();

        // Now set a failpoint for find with 391 error code
        let fail_point =
            FailPoint::fail_command(&["find"], FailPointMode::Times(1)).error_code(391);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();

        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::machine(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(2, *(*call_count).lock().await);
        Ok(())
    }

    // Human Callback tests
    #[tokio::test]
    async fn human_1_1_single_principal_implicit_username() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let database = client.database("test");
        let collection = database.collection::<Document>("test");
        collection.find_one(doc! {}).await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn human_1_2_single_principal_explicit_username() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .username(explicit_user!("test_user1"))
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn human_1_3_multiple_principal_user_1() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_multi!()).await?;
        opts.credential = Credential::builder()
            .username(explicit_user!("test_user1"))
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn human_1_4_multiple_principal_user_2() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_multi!()).await?;
        opts.credential = Credential::builder()
            .username(explicit_user!("test_user2"))
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user2")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn human_1_5_multiple_principal_no_user() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_multi!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(no_user_token_file!()).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));
        assert_eq!(0, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn human_1_6_allowed_hosts_blocked() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        use crate::client::auth::ALLOWED_HOSTS_PROP_STR;
        {
            // we need to assert the callback count
            let call_count = Arc::new(Mutex::new(0));
            let cb_call_count = call_count.clone();

            // Use empty list for ALLOWED_HOSTS
            let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
            opts.credential = Credential::builder()
                .mechanism(AuthMechanism::MongoDbOidc)
                .mechanism_properties(bson::doc! {
                    ALLOWED_HOSTS_PROP_STR: [],
                })
                .oidc_callback(oidc::Callback::human(move |_| {
                    let call_count = cb_call_count.clone();
                    async move {
                        *call_count.lock().await += 1;
                        Ok(oidc::IdpServerResponse {
                            access_token: tokio::fs::read_to_string(token_dir!("test_user1"))
                                .await?,
                            expires: None,
                            refresh_token: None,
                        })
                    }
                    .boxed()
                }))
                .build()
                .into();
            let client = Client::with_options(opts)?;
            let res = client
                .database("test")
                .collection::<Document>("test")
                .find_one(doc! {})
                .await;

            assert!(res.is_err());
            assert!(matches!(
                *res.unwrap_err().kind,
                crate::error::ErrorKind::Authentication { .. },
            ));
            // asserting 0 shows that this is a client side error
            assert_eq!(0, *(*call_count).lock().await);
        }

        {
            // we need to assert the callback count
            let call_count = Arc::new(Mutex::new(0));
            let cb_call_count = call_count.clone();

            let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
            opts.credential = Credential::builder()
                .mechanism(AuthMechanism::MongoDbOidc)
                .mechanism_properties(bson::doc! {
                    ALLOWED_HOSTS_PROP_STR: ["example.com"],
                })
                .oidc_callback(oidc::Callback::human(move |_| {
                    let call_count = cb_call_count.clone();
                    async move {
                        *call_count.lock().await += 1;
                        Ok(oidc::IdpServerResponse {
                            access_token: tokio::fs::read_to_string(token_dir!("test_user1"))
                                .await?,
                            expires: None,
                            refresh_token: None,
                        })
                    }
                    .boxed()
                }))
                .build()
                .into();
            let client = Client::with_options(opts)?;
            let res = client
                .database("test")
                .collection::<Document>("test")
                .find_one(doc! {})
                .await;

            assert!(res.is_err());
            assert!(matches!(
                *res.unwrap_err().kind,
                crate::error::ErrorKind::Authentication { .. },
            ));
            // asserting 0 shows that this is a client side error
            assert_eq!(0, *(*call_count).lock().await);
        }

        Ok(())
    }

    #[tokio::test]
    async fn human_2_1_valid_callback_inputs() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |c| {
                let call_count = cb_call_count.clone();
                let idp_info = c.idp_info.unwrap();
                assert!(idp_info.issuer.as_str() != "");
                assert!(idp_info.client_id.is_some());
                assert!(c.timeout.unwrap() <= Instant::now() + Duration::from_secs(60 * 5));
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test]
    async fn human_2_2_callback_returns_missing_data() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: "".to_string(),
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn human_3_1_uses_speculative_authentication_if_there_is_a_cached_token(
    ) -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // get an admin_client for setting failpoints
        let admin_client = admin_client!();

        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        // since this test will use the cached token, this callback shouldn't matter
                        access_token: "".to_string(),
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();

        // put the test_user1 token in the cache
        opts.credential
            .as_mut()
            .unwrap()
            .oidc_callback
            .set_access_token(Some(
                tokio::fs::read_to_string(token_dir!("test_user1")).await?,
            ))
            .await;

        let client = Client::with_options(opts)?;

        // Now set a failpoint for saslStart
        // we use 5 times just because AlwaysOn is dangerous if for some reason we don't run
        // the cleanup, since we will not be able to auth a new connection to turn
        // off the failpoint.
        let fail_point =
            FailPoint::fail_command(&["saslStart"], FailPointMode::Times(5)).error_code(20);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();

        // Now find should succeed even though we have a fail point on saslStart because the spec
        // auth should succeed.
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        // the callback should not have been called at all
        assert_eq!(0, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn human_3_2_does_not_use_speculative_authentication_if_there_is_no_cached_token(
    ) -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // get an admin_client for setting failpoints
        let admin_client = admin_client!();

        // Now set a failpoint for find
        let fail_point =
            FailPoint::fail_command(&["saslStart"], FailPointMode::Times(5)).error_code(20);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;

        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));

        assert_eq!(0, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn human_4_1_succeeds() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        use crate::{
            event::command::CommandEvent,
            test::{util::event_buffer::EventBuffer, Event},
        };

        let admin_client = admin_client!();

        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();

        let buffer = EventBuffer::new();
        opts.command_event_handler = Some(buffer.handler());
        let client = Client::with_options(opts)?;

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        // Now set a failpoint for find with 391 error code
        let fail_point =
            FailPoint::fail_command(&["find"], FailPointMode::Times(1)).error_code(391);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        assert_eq!(2, *(*call_count).lock().await);
        let find_events = buffer.filter_map(|e: &Event| match e.as_command_event() {
            Some(command_event) if command_event.command_name() == "find" => {
                Some(command_event.clone())
            }
            _ => None,
        });
        // assert the first find started
        assert!(matches!(
            find_events.first().unwrap(),
            CommandEvent::Started(_)
        ));
        // assert the first find succeeded
        assert!(matches!(
            find_events.get(1).unwrap(),
            CommandEvent::Succeeded(_)
        ));
        // assert the second find started
        assert!(matches!(
            find_events.get(2).unwrap(),
            CommandEvent::Started(_)
        ));
        // assert the second find failed
        assert!(matches!(
            find_events.get(3).unwrap(),
            CommandEvent::Failed(_)
        ));
        // assert the first find started
        assert!(matches!(
            find_events.get(4).unwrap(),
            CommandEvent::Started(_)
        ));
        // assert the third find succeeded
        assert!(matches!(
            find_events.get(5).unwrap(),
            CommandEvent::Succeeded(_)
        ));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn human_4_2_succeeds_no_refresh() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        let admin_client = admin_client!();

        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        // Now set a failpoint for find with 391 error code
        let fail_point =
            FailPoint::fail_command(&["find"], FailPointMode::Times(1)).error_code(391);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        assert_eq!(2, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn human_4_3_succeeds_after_refresh_fails() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        let admin_client = admin_client!();

        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: Some("fake refresh token".to_string()),
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        assert_eq!(1, *(*call_count).lock().await);

        // Now set a failpoint for find with 391 error code
        let fail_point = FailPoint::fail_command(&["find", "saslStart"], FailPointMode::Times(2))
            .error_code(391);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        assert_eq!(3, *(*call_count).lock().await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn human_4_4_fails() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        let admin_client = admin_client!();

        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |_| {
                let call_count = cb_call_count.clone();
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();
        let client = Client::with_options(opts)?;

        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        assert_eq!(1, *(*call_count).lock().await);

        // Now set a failpoint for find with 391 error code
        let fail_point = FailPoint::fail_command(&["find", "saslStart"], FailPointMode::Times(3))
            .error_code(391);
        let _guard = admin_client.enable_fail_point(fail_point).await.unwrap();

        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));

        assert_eq!(2, *(*call_count).lock().await);
        Ok(())
    }

    // This is not in the spec, but the spec has no test that actually tests refresh flow
    #[tokio::test]
    async fn human_4_5_refresh_token_flow() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        // we need to assert the callback count
        let call_count = Arc::new(Mutex::new(0));
        let cb_call_count = call_count.clone();

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbOidc)
            .oidc_callback(oidc::Callback::human(move |c| {
                let call_count = cb_call_count.clone();
                // assert that the cached refresh token is passed to the callback
                assert_eq!(c.refresh_token.as_deref(), Some("some fake refresh token"));
                async move {
                    *call_count.lock().await += 1;
                    Ok(oidc::IdpServerResponse {
                        // since this test will use the cached token, this callback shouldn't matter
                        access_token: tokio::fs::read_to_string(token_dir!("test_user1")).await?,
                        expires: None,
                        refresh_token: None,
                    })
                }
                .boxed()
            }))
            .build()
            .into();

        // put a fake refresh token in the cache
        opts.credential
            .as_mut()
            .unwrap()
            .oidc_callback
            .set_refresh_token(Some("some fake refresh token".to_string()))
            .await;

        let client = Client::with_options(opts)?;

        // Now find should succeed even though we have a fail point on saslStart because the spec
        // auth should succeed.
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;

        // the callback should have been called once
        assert_eq!(1, *(*call_count).lock().await);
        Ok(())
    }
}

mod azure {
    use crate::client::{options::ClientOptions, Client};
    use bson::{doc, Document};

    #[tokio::test]
    async fn machine_5_1_azure_with_no_username() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");

        let opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn machine_5_2_azure_with_bad_username() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential.as_mut().unwrap().username = Some("bad".to_string());
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;
        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::Authentication { .. },
        ));
        Ok(())
    }

    #[tokio::test]
    async fn machine_5_3_token_resource_must_be_set_for_azure() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        use crate::client::auth::{AZURE_ENVIRONMENT_VALUE_STR, ENVIRONMENT_PROP_STR};

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential.as_mut().unwrap().mechanism_properties = Some(doc! {
            ENVIRONMENT_PROP_STR: AZURE_ENVIRONMENT_VALUE_STR,
        });
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::InvalidArgument { .. },
        ));
        Ok(())
    }
}

mod gcp {
    use crate::client::{options::ClientOptions, Client};
    use bson::{doc, Document};

    #[tokio::test]
    async fn machine_5_4_gcp_with_no_username() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential.as_mut().unwrap().source = None;
        let client = Client::with_options(opts)?;
        client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn machine_5_5_token_resource_must_be_set_for_gcp() -> anyhow::Result<()> {
        get_env_or_skip!("OIDC");
        use crate::client::auth::{ENVIRONMENT_PROP_STR, GCP_ENVIRONMENT_VALUE_STR};

        let mut opts = ClientOptions::parse(mongodb_uri_single!()).await?;
        opts.credential.as_mut().unwrap().source = None;
        opts.credential.as_mut().unwrap().mechanism_properties = Some(doc! {
            ENVIRONMENT_PROP_STR: GCP_ENVIRONMENT_VALUE_STR,
        });
        let client = Client::with_options(opts)?;
        let res = client
            .database("test")
            .collection::<Document>("test")
            .find_one(doc! {})
            .await;

        assert!(res.is_err());
        assert!(matches!(
            *res.unwrap_err().kind,
            crate::error::ErrorKind::InvalidArgument { .. },
        ));
        Ok(())
    }
}
