#[cfg(feature = "oidc-auth")]
type Result<T> = anyhow::Result<T>;

// Prose test 1.1 Single Principal Implicit Username
#[cfg(feature = "oidc-auth")]
#[cfg_attr(all(feature = "tokio-runtime"), tokio::test)]
async fn single_principal_implicit_username() -> Result<()> {
    use bson::Document;
    use futures_util::FutureExt;

    use crate::{
        client::{
            auth::{AuthMechanism, Credential},
            options::ClientOptions,
        },
        oidc,
        test::log_uncaptured,
        Client,
    };
    if std::env::var("OIDC_TOKEN_DIR").is_err() {
        log_uncaptured("Skipping OIDC test");
        return Ok(());
    }
    let mut opts =
        ClientOptions::parse_async("mongodb://localhost/?authMechanism=MONGODB-OIDC").await?;
    opts.credential = Some(Credential {
        mechanism: Some(AuthMechanism::MongoDbOidc),
        oidc_callbacks: Some(oidc::Callbacks::new(|_info, _params| {
            async move {
                Ok(oidc::IdpServerResponse {
                    access_token: tokio::fs::read_to_string("/tmp/tokens/test_user1").await?,
                    expires: None,
                    refresh_token: None,
                })
            }
            .boxed()
        })),
        ..Credential::default()
    });
    let client = Client::with_options(opts)?;
    client
        .database("test")
        .collection::<Document>("test")
        .find_one(None, None)
        .await?;
    Ok(())
}
