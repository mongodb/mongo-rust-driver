#[cfg(feature = "aws-auth")]
mod aws;

use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    options::{AuthMechanism, ClientOptions, Credential, ServerAddress},
    Client,
};

#[tokio::test]
async fn plain_auth() {
    let options = ClientOptions::builder()
        .hosts(vec![ServerAddress::Tcp {
            host: "ldaptest.10gen.cc".into(),
            port: None,
        }])
        .credential(
            Credential::builder()
                .mechanism(AuthMechanism::Plain)
                .username("drivers-team".to_string())
                .password("mongor0x$xgen".to_string())
                .build(),
        )
        .build();

    let client = Client::with_options(options).unwrap();
    let coll = client.database("ldap").collection("test");

    let doc = coll.find_one(doc! {}).await.unwrap().unwrap();

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestDocument {
        ldap: bool,
        authenticated: String,
    }

    let doc: TestDocument = crate::bson_compat::deserialize_from_document(doc).unwrap();

    assert_eq!(
        doc,
        TestDocument {
            ldap: true,
            authenticated: "yeah".into()
        }
    );
}
