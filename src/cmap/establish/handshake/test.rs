use super::Handshaker;
use crate::{bson::doc, cmap::options::ConnectionPoolOptions, options::DriverInfo};

#[test]
fn metadata_no_options() {
    let handshaker = Handshaker::new(None);

    let metadata = handshaker.command.body.get_document("client").unwrap();
    assert!(!metadata.contains_key("application"));

    let driver = metadata.get_document("driver").unwrap();
    assert_eq!(driver.keys().collect::<Vec<_>>(), vec!["name", "version"]);
    assert_eq!(driver.get_str("name"), Ok("mongo-rust-driver"));
    assert_eq!(driver.get_str("version"), Ok(env!("CARGO_PKG_VERSION")));

    let os = metadata.get_document("os").unwrap();
    assert_eq!(os.get_str("type"), Ok(std::env::consts::OS));
    assert_eq!(os.get_str("architecture"), Ok(std::env::consts::ARCH));
}

#[test]
fn metadata_with_options() {
    let app_name = "myspace 2.0";
    let name = "even better Rust driver";
    let version = "the best version, of course";

    let options = ConnectionPoolOptions::builder()
        .app_name(app_name.to_string())
        .driver_info(
            DriverInfo::builder()
                .name(name.to_string())
                .version(version.to_string())
                .build(),
        )
        .build();

    let handshaker = Handshaker::new(Some(options.into()));

    let metadata = handshaker.command.body.get_document("client").unwrap();
    assert_eq!(
        metadata.get_document("application"),
        Ok(&doc! { "name": app_name })
    );

    let driver = metadata.get_document("driver").unwrap();
    assert_eq!(driver.keys().collect::<Vec<_>>(), vec!["name", "version"]);
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
