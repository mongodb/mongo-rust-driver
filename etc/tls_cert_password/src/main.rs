use std::path::PathBuf;

use mongodb::{
    bson::doc,
    options::{Tls, TlsOptions},
    Client,
};

#[tokio::main]
async fn main() {
    let mut certpath = PathBuf::from(std::env::var("DRIVERS_TOOLS").unwrap());
    certpath.push(".evergreen/x509gen");
    let mut options = mongodb::options::ClientOptions::parse(std::env::var("MONGODB_URI").unwrap())
        .await
        .unwrap();
    options.tls = Some(Tls::Enabled(
        TlsOptions::builder()
            .ca_file_path(certpath.join("ca.pem"))
            .cert_key_file_path(certpath.join("client-pkcs8-encrypted.pem"))
            .tls_certificate_key_file_password(b"password".to_vec())
            .build(),
    ));
    let client = Client::with_options(options).unwrap();
    client
        .database("test")
        .run_command(doc! {"ping": 1})
        .await
        .unwrap();
}
