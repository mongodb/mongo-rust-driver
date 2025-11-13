use crate::{
    bson::{doc, Document},
    Client,
};

/// Run a GSSAPI e2e test.
///   - user_principal_var is the name of the environment variable that stores the user principal
///   - password_var is the name of the environment variable that stores the Windows password. Only
///     applies to Windows since GSSAPI on non-Windows obtains credentials through `kinit`.
///   - gssapi_db_var is the name tof the environment variable that stores the db name to query
///   - auth_mechanism_properties is an optional set of authMechanismProperties to append to the uri
async fn run_gssapi_auth_test(
    user_principal_var: &str,
    #[cfg(target_os = "windows")] password_var: &str,
    gssapi_db_var: &str,
    auth_mechanism_properties: Option<&str>,
) {
    // Get env variables
    let host = std::env::var("SASL_HOST").expect("SASL_HOST not set");
    let user_principal = std::env::var(user_principal_var)
        .unwrap_or_else(|_| panic!("{user_principal_var} not set"))
        .replace("@", "%40");
    let gssapi_db =
        std::env::var(gssapi_db_var).unwrap_or_else(|_| panic!("{gssapi_db_var} not set"));

    // Optionally create authMechanismProperties
    let props = if let Some(auth_mech_props) = auth_mechanism_properties {
        format!("&authMechanismProperties={auth_mech_props}")
    } else {
        String::new()
    };

    // Create client
    #[cfg(not(target_os = "windows"))]
    let uri = format!(
        "mongodb://{user_principal}@{host}/?authSource=%24external&authMechanism=GSSAPI{props}"
    );
    #[cfg(target_os = "windows")]
    let uri = {
        let password =
            std::env::var(password_var).unwrap_or_else(|_| panic!("{password_var} not set"));
        format!(
            "mongodb://{user_principal}:{password}@{host}/?authSource=%24external&\
             authMechanism=GSSAPI{props}"
        )
    };
    let client = Client::with_uri_str(uri)
        .await
        .expect("failed to create MongoDB Client");

    // Check that auth worked by qurying the test collection
    let coll = client.database(&gssapi_db).collection::<Document>("test");
    let doc = coll.find_one(doc! {}).await;
    match doc {
        Ok(Some(doc)) => {
            assert!(
                doc.get_bool(&gssapi_db).unwrap(),
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

#[tokio::test]
async fn no_options() {
    run_gssapi_auth_test(
        "PRINCIPAL",
        #[cfg(target_os = "windows")]
        "SASL_PASS",
        "GSSAPI_DB",
        None,
    )
    .await
}

#[tokio::test]
async fn explicit_canonicalize_host_name_false() {
    run_gssapi_auth_test(
        "PRINCIPAL",
        #[cfg(target_os = "windows")]
        "SASL_PASS",
        "GSSAPI_DB",
        Some("CANONICALIZE_HOST_NAME:false"),
    )
    .await
}

#[tokio::test]
async fn canonicalize_host_name_forward() {
    run_gssapi_auth_test(
        "PRINCIPAL",
        #[cfg(target_os = "windows")]
        "SASL_PASS",
        "GSSAPI_DB",
        Some("CANONICALIZE_HOST_NAME:forward"),
    )
    .await
}

#[tokio::test]
async fn canonicalize_host_name_forward_and_reverse() {
    run_gssapi_auth_test(
        "PRINCIPAL",
        #[cfg(target_os = "windows")]
        "SASL_PASS",
        "GSSAPI_DB",
        Some("CANONICALIZE_HOST_NAME:forwardAndReverse"),
    )
    .await
}

#[tokio::test]
async fn with_service_realm_and_host_options() {
    // This test uses a "cross-realm" user principal, however the service principal is not
    // cross-realm. This is why we use SASL_REALM and SASL_HOST instead of SASL_REALM_CROSS
    // and SASL_HOST_CROSS.
    let service_realm = std::env::var("SASL_REALM").expect("SASL_REALM not set");
    let service_host = std::env::var("SASL_HOST").expect("SASL_HOST not set");

    run_gssapi_auth_test(
        "PRINCIPAL_CROSS",
        #[cfg(target_os = "windows")]
        "SASL_PASS_CROSS",
        "GSSAPI_DB_CROSS",
        Some(format!("SERVICE_REALM:{service_realm},SERVICE_HOST:{service_host}").as_str()),
    )
    .await
}
