use std::sync::atomic::{AtomicBool, Ordering};

use aws_config::BehaviorVersion;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};

use crate::{
    bson::{doc, Document},
    options::{aws::get_aws_credentials_from_uri, Credential},
    test::get_client_options,
    Client,
};

#[tokio::test]
async fn auth_aws() {
    let client = Client::for_test().await;
    let coll = client.database("aws").collection::<Document>("somecoll");

    coll.find_one(doc! {}).await.unwrap();
}

static CUSTOM_CALLED: AtomicBool = AtomicBool::new(false);

#[tokio::test]
async fn auth_aws_custom() {
    let mut options = get_client_options().await.clone();
    let credential = options.credential.get_or_insert(Credential::default());
    let tracking = TrackingCredentialProvider::new(credential).await;
    credential.aws_credential_provider = Some(SharedCredentialsProvider::new(tracking));
    let client = Client::for_test().options(options).await;

    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();

    assert!(CUSTOM_CALLED.load(Ordering::SeqCst));
}

#[derive(Debug)]
struct TrackingCredentialProvider(SharedCredentialsProvider);

impl TrackingCredentialProvider {
    async fn new(cred: &Credential) -> Self {
        let inner = if let Some(creds) = get_aws_credentials_from_uri(cred) {
            SharedCredentialsProvider::new(creds)
        } else {
            aws_config::load_defaults(BehaviorVersion::latest())
                .await
                .credentials_provider()
                .unwrap()
        };
        Self(inner)
    }
}

impl ProvideCredentials for TrackingCredentialProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        CUSTOM_CALLED.store(true, Ordering::SeqCst);
        self.0.provide_credentials()
    }
}
