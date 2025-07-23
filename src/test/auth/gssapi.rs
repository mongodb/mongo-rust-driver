use crate::{
    bson::{doc, Document},
    Client,
};

/// Create a GSSAPI e2e test.
///   - test_name is the name of the test.
///   - user_principal is the name of the environment variable that stores the user principal
///   - auth_mechanism_properties is an optional set of authMechanismProperties to append to the uri
macro_rules! test_gssapi_auth {
    ($test_name:ident, user_principal = $user_principal:expr, gssapi_db = $gssapi_db:expr, $(auth_mechanism_properties = $auth_mechanism_properties:expr,)?) => {
        #[tokio::test]
        async fn $test_name() {
            // Get env variables
            let host = std::env::var("SASL_HOST").expect("SASL_HOST not set");
            let user_principal = std::env::var($user_principal)
                .expect(format!("{} not set", $user_principal).as_str())
                .replace("@", "%40");
            let gssapi_db = std::env::var($gssapi_db)
                .expect(format!("{} not set", $gssapi_db).as_str());

            // Optionally create authMechanismProperties
            #[allow(unused_mut, unused_assignments)]
            let mut auth_mechanism_properties = "".to_string();
            $(auth_mechanism_properties = format!("&authMechanismProperties={}", $auth_mechanism_properties);)?

            // Create client
            let uri = format!("mongodb://{user_principal}@{host}/?authSource=%24external&authMechanism=GSSAPI{auth_mechanism_properties}");
            let client = Client::with_uri_str(uri)
                .await
                .expect("failed to create MongoDB Client");

            // Check that auth worked by qurying the test collection
            let coll = client.database(&gssapi_db).collection::<Document>("test");
            let doc = coll.find_one(doc! {}).await;
            match doc {
                Ok(Some(doc)) => {
                    assert!(
                        doc.get_bool(gssapi_db).unwrap(),
                        "expected '{gssapi_db}' field to exist and be 'true'"
                    );
                    assert_eq!(
                        doc.get_str("authenticated").unwrap(),
                        "yeah",
                        "unexpected 'authenticated' value"
                    );
                }
                Ok(None) => panic!("expected `find_one` to return a document, but it did not"),
                Err(e) => panic!("expected `find_one` to return a document, but it failed: {e:?}"),
            }
        }
    };
}

test_gssapi_auth!(
    no_options,
    user_principal = "PRINCIPAL",
    gssapi_db = "GSSAPI_DB",
);

test_gssapi_auth!(
    explicit_canonicalize_host_name_false,
    user_principal = "PRINCIPAL",
    gssapi_db = "GSSAPI_DB",
    auth_mechanism_properties = "CANONICALIZE_HOST_NAME:false",
);

test_gssapi_auth!(
    canonicalize_host_name_forward,
    user_principal = "PRINCIPAL",
    gssapi_db = "GSSAPI_DB",
    auth_mechanism_properties = "CANONICALIZE_HOST_NAME:forward",
);

test_gssapi_auth!(
    canonicalize_host_name_forward_and_reverse,
    user_principal = "PRINCIPAL",
    gssapi_db = "GSSAPI_DB",
    auth_mechanism_properties = "CANONICALIZE_HOST_NAME:forwardAndReverse",
);

test_gssapi_auth!(
    with_service_realm_and_host_options,
    // Use the cross-realm USER principal
    user_principal = "PRINCIPAL_CROSS",
    // The cross-realm user is only authorized on the cross-realm db
    gssapi_db = "GSSAPI_DB_CROSS",
    auth_mechanism_properties = {
        // However, the SERVICE principal is not cross-realm, hence the use of
        // SASL_REALM instead of SASL_REALM_CROSS.
        let service_realm = std::env::var("SASL_REALM").expect("SASL_REALM not set");
        let service_host = std::env::var("SASL_HOST").expect("SASL_HOST not set");
        format!("SERVICE_REALM:{service_realm},SERVICE_HOST:{service_host}").as_str()
    },
);
