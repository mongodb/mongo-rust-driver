use std::ops::Deref;

use crate::bson::rawdoc;

use super::Handshaker;
use crate::{cmap::establish::handshake::HandshakerOptions, options::DriverInfo};

#[tokio::test]
async fn metadata_no_options() {
    let handshaker = Handshaker::new(HandshakerOptions {
        app_name: None,
        #[cfg(any(
            feature = "zstd-compression",
            feature = "zlib-compression",
            feature = "snappy-compression"
        ))]
        compressors: None,
        driver_info: None,
        server_api: None,
        load_balanced: false,
    });

    let command = handshaker.build_command(None).await.unwrap().0;
    let metadata = command.body.get_document("client").unwrap();
    assert!(!matches!(metadata.get("application"), Ok(Some(_))));

    let driver = metadata.get_document("driver").unwrap();
    assert_eq!(
        driver
            .iter_elements()
            .map(|e| e.unwrap().key())
            .collect::<Vec<_>>(),
        vec!["name", "version"]
    );
    assert_eq!(driver.get_str("name"), Ok("mongo-rust-driver"));
    assert_eq!(driver.get_str("version"), Ok(env!("CARGO_PKG_VERSION")));

    let os = metadata.get_document("os").unwrap();
    assert_eq!(os.get_str("type"), Ok(std::env::consts::OS));
    assert_eq!(os.get_str("architecture"), Ok(std::env::consts::ARCH));
}

#[tokio::test]
async fn metadata_with_options() {
    let app_name = "myspace 2.0";
    let name = "even better Rust driver";
    let version = "the best version, of course";

    let options = HandshakerOptions {
        app_name: Some(app_name.to_string()),
        driver_info: Some(
            DriverInfo::builder()
                .name(name.to_string())
                .version(version.to_string())
                .build(),
        ),
        #[cfg(any(
            feature = "zstd-compression",
            feature = "zlib-compression",
            feature = "snappy-compression"
        ))]
        compressors: None,
        server_api: None,
        load_balanced: false,
    };

    let handshaker = Handshaker::new(options);
    let command = handshaker.build_command(None).await.unwrap().0;
    let metadata = command.body.get_document("client").unwrap();
    assert_eq!(
        metadata.get_document("application"),
        Ok(rawdoc! { "name": app_name }.deref())
    );

    let driver = metadata.get_document("driver").unwrap();
    assert_eq!(
        driver
            .iter_elements()
            .map(|e| e.unwrap().key())
            .collect::<Vec<_>>(),
        vec!["name", "version"]
    );
    assert_eq!(
        driver.get_str("name"),
        Ok(format!("mongo-rust-driver|{}", name).as_str())
    );
    assert_eq!(
        driver.get_str("version"),
        Ok(format!("{}|{}", env!("CARGO_PKG_VERSION"), version).as_str())
    );

    let os = metadata.get_document("os").unwrap();
    assert_eq!(os.get_str("type"), Ok(std::env::consts::OS));
    assert_eq!(os.get_str("architecture"), Ok(std::env::consts::ARCH));
}
