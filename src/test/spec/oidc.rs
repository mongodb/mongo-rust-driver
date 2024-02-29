type Result<T> = anyhow::Result<T>;

// Prose test 1.1 Single Principal Implicit Username
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
async fn single_principal_implicit_username() -> Result<()> {
    use crate::{
        client::{
            auth::{oidc, AuthMechanism, Credential},
            options::ClientOptions,
        },
        test::log_uncaptured,
        Client,
    };
    use bson::Document;
    use futures_util::FutureExt;
    if std::env::var("OIDC_TOKEN_DIR").is_err() {
        log_uncaptured("Skipping OIDC test");
        return Ok(());
    }
    let mut opts =
        ClientOptions::parse_async("mongodb://localhost/?authMechanism=MONGODB-OIDC").await?;
    opts.credential = Credential::builder()
        .mechanism(AuthMechanism::MongoDbOidc)
        .oidc_callback(oidc::Callback::machine(|_| {
            async move {
                Ok(oidc::IdpServerResponse {
                    access_token: tokio::fs::read_to_string("/tmp/tokens/test_user1").await?,
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
        .find_one(None, None)
        .await?;
    Ok(())
}
