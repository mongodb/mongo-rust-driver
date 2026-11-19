use std::{fs::File, path::PathBuf};

use serde::Deserialize;

use crate::{
    options::{ClientOptions, ConnectionString},
    Client,
};

#[tokio::test]
async fn run_spec_tests() {
    let sentinel = "SENTINELpassword1234".to_lowercase();
    let sentinel_encoded =
        "%53%45%4E%54%49%4E%45%4C%70%61%73%73%77%6F%72%64%31%32%33%34".to_lowercase();

    #[derive(Deserialize)]
    struct TestCase {
        description: String,
        uri: String,
    }
    #[derive(Deserialize)]
    struct TestFile {
        tests: Vec<TestCase>,
    }

    let path: PathBuf = [
        &std::env::var("CARGO_MANIFEST_DIR").unwrap(),
        "..",
        "spec",
        "connection-string",
        "credential-disclosure.json",
    ]
    .iter()
    .collect();
    let mut de = serde_json::Deserializer::from_reader(File::open(&path).unwrap());
    let file: TestFile = serde_path_to_error::deserialize(&mut de).unwrap();

    for test in file.tests {
        let TestCase { description, uri } = test;
        let errs = [
            ConnectionString::parse(&uri).err(),
            ClientOptions::parse(&uri).await.err(),
            Client::with_uri_str(&uri).await.err(),
        ];
        for err in errs {
            let Some(e) = err else {
                continue;
            };
            let err_str = e.to_string();
            let err_lower = err_str.to_lowercase();
            assert!(
                !err_lower.contains(&sentinel) && !err_lower.contains(&sentinel_encoded),
                "[{description}] sentinel found:\n\tURI: {uri:?}\n\terr: {err_str:?}"
            );
        }
    }
}
// Fails to compile if $ty implements $tr.
macro_rules! assert_not_impl {
    ($ty:ty, $tr:path) => {
        const _: fn() = || {
            trait AmbiguousIfImpl<A> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            impl<T: ?Sized + $tr> AmbiguousIfImpl<u8> for T {}

            // If $ty: $tr, both impls apply and `_` can't be inferred -> error.
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
}

const PROSE_SENTINEL_USER: &str = "SENTINELuser5678";
const PROSE_SENTINEL_PASSWD: &str = "SENTINELpassword1234";

/// Prose Test 1: Redacted string representation.
#[tokio::test]
async fn redacted_string() {
    const URI: &str = "mongodb://SENTINELuser5678:SENTINELpassword1234@localhost:27017/db";
    let conn_str = ConnectionString::parse(URI).unwrap();
    let opts = ClientOptions::parse(URI).await.unwrap();
    let client = Client::with_uri_str(URI).await.unwrap();

    let debug = [
        format!("{conn_str:?}"),
        format!("{opts:?}"),
        format!("{client:?}"),
    ];
    for s in debug {
        assert!(
            !s.contains(PROSE_SENTINEL_USER) && !s.contains(PROSE_SENTINEL_PASSWD),
            "[redacted string]: sentinel found\n\tdebug: {s:?}"
        )
    }

    let print = [format!("{conn_str}")];
    assert_not_impl!(ClientOptions, ToString);
    assert_not_impl!(Client, ToString);
    for s in print {
        assert!(
            !s.contains(PROSE_SENTINEL_USER) && !s.contains(PROSE_SENTINEL_PASSWD),
            "[redacted string]: sentinel found\n\tprint: {s:?}"
        )
    }
}

/// Prose Test 2: Explicitly named unredacted API.
#[test]
fn unredacted_string() {
    const URI: &str = "mongodb://SENTINELuser5678:SENTINELpassword1234@localhost:27017/db";
    let conn_str = ConnectionString::parse(URI).unwrap();
    let unredacted = conn_str.to_unredacted_string();
    assert!(
        unredacted.contains(PROSE_SENTINEL_USER) && unredacted.contains(PROSE_SENTINEL_PASSWD),
        "[unredacted string]: sentinel missing\n\tunredacted: {unredacted:?}"
    );
}

/// Asserts that the error rendered via both `Display` and `Debug` contains none of `forbidden`.
fn assert_error_omits(err: &crate::error::Error, forbidden: &[&str], context: &str) {
    for rendered in [format!("{err}"), format!("{err:?}")] {
        for f in forbidden {
            assert!(
                !rendered.contains(f),
                "[{context}]: found {f:?} in error: {rendered:?}"
            );
        }
    }
}

/// Prose Test 3: Canonical authentication mechanism diagnostic.
#[test]
fn canonical_auth_mechanism_diagnostic() {
    // Recognized mechanism, fails credential validation: the diagnostic uses the canonical name.
    let err = ConnectionString::parse(format!(
        "mongodb://{PROSE_SENTINEL_USER}:{PROSE_SENTINEL_PASSWD}@localhost/?\
         authMechanism=MONGODB-X509"
    ))
    .unwrap_err();
    assert!(
        err.to_string().contains("MONGODB-X509"),
        "diagnostic should use the canonical mechanism name: {err}"
    );
    assert_error_omits(
        &err,
        &[PROSE_SENTINEL_USER, PROSE_SENTINEL_PASSWD],
        "recognized mechanism",
    );

    // Mixed-case spellings and unknown mechanisms must not be repeated.
    for spelling in ["Mongodb-X509", "mOnGoDb-x509", "SENTINELpassword1234"] {
        let err = ConnectionString::parse(format!(
            "mongodb://user@localhost/?authMechanism={spelling}"
        ))
        .unwrap_err();
        assert_error_omits(&err, &[spelling], "unrecognized mechanism");
    }
}

/// Prose Test 4: Parsed integer diagnostic.
#[test]
fn parsed_integer_diagnostic() {
    // Integers that parse but fail a range check are reported in their parsed form, not as
    // spelled in the URI.
    for (spelling, forbidden) in [
        ("-0002", &["-0002"][..]),
        ("-000000000000000000002", &["-0000"][..]),
        ("%2D%30%30%32", &["%2D", "%30", "-00"][..]),
    ] {
        let err = ConnectionString::parse(format!(
            "mongodb://localhost/?readPreference=secondary&maxStalenessSeconds={spelling}"
        ))
        .unwrap_err();
        assert!(
            err.to_string().contains("-2"),
            "[{spelling}]: expected parsed integer in diagnostic: {err}"
        );
        assert_error_omits(&err, forbidden, spelling);
    }

    // Arguments that fail integer conversion, or belong to unknown or sensitive options, are
    // never included.
    for query in [
        "maxStalenessSeconds=SENTINELpassword1234",
        "connectTimeoutMS=SENTINELpassword1234",
        "maxPoolSize=SENTINELpassword1234",
        "zlibCompressionLevel=SENTINELpassword1234",
        "SENTINELpassword1234=1",
        "SENTINELpassword1234=SENTINELpassword1234",
        "unknownOption=SENTINELpassword1234",
        "authMechanismProperties=SENTINELpassword1234",
        "authMechanism=MONGODB-OIDC&authMechanismProperties=SENTINELpassword1234:x",
        "tlsCertificateKeyFilePassword=SENTINELpassword1234&tls=false",
        "readPreference=SENTINELpassword1234",
        "readPreference=secondary&readPreferenceTags=SENTINELpassword1234",
        "uuidRepresentation=SENTINELpassword1234",
        "serverMonitoringMode=SENTINELpassword1234",
        "compressors=SENTINELpassword1234",
        "SENTINELpassword1234",
    ] {
        let err = ConnectionString::parse(format!("mongodb://localhost/?{query}"))
            .err()
            .unwrap_or_else(|| panic!("[{query}]: expected a parse failure"));
        assert_error_omits(&err, &[PROSE_SENTINEL_PASSWD], query);
    }
}

/// Unknown keys are reported only by their position relative to the start of the connection
/// options.
#[test]
fn unknown_option_position() {
    let err = ConnectionString::parse(format!(
        "mongodb://localhost/?appName=a&{PROSE_SENTINEL_PASSWD}=b"
    ))
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Unsupported option at offset 10 in the query segment"),
        "unexpected diagnostic: {err}"
    );
    assert_error_omits(&err, &[PROSE_SENTINEL_PASSWD], "unknown key");
}

/// Prose Test 5: Representation of a connection string that did not parse.
#[tokio::test]
async fn unparsed_connection_string() {
    const URI: &str = "mongodb://SENTINELuser5678:SENTINELpassword1234@localhost:99999/db";
    let forbidden = [PROSE_SENTINEL_USER, PROSE_SENTINEL_PASSWD];

    let err = ConnectionString::parse(URI).unwrap_err();
    assert_error_omits(&err, &forbidden, "ConnectionString::parse");
    let err = ClientOptions::parse(URI).await.unwrap_err();
    assert_error_omits(&err, &forbidden, "ClientOptions::parse");
    let err = Client::with_uri_str(URI).await.unwrap_err();
    assert_error_omits(&err, &forbidden, "Client::with_uri_str");
}
