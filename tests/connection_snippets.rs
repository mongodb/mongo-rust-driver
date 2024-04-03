#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

extern crate mongodb;

mod async_scram {
    // ASYNC SCRAM CONNECTION EXAMPLE STARTS HERE
    use mongodb::{options::ClientOptions, Client};

    #[tokio::main]
    async fn main() -> mongodb::error::Result<()> {
        let client_options = ClientOptions::parse(
            "mongodb+srv://<username>:<password>@<cluster-url>/<dbname>?w=majority",
        )
        .await?;
        let client = Client::with_options(client_options)?;
        let database = client.database("test");
        // do something with database

        Ok(())
    }
    // CONNECTION EXAMPLE ENDS HERE
}

mod async_x509 {
    // ASYNC X509 CONNECTION EXAMPLE STARTS HERE
    use mongodb::{
        options::{AuthMechanism, ClientOptions, Credential, Tls, TlsOptions},
        Client,
    };
    use std::path::PathBuf;

    #[tokio::main]
    async fn main() -> mongodb::error::Result<()> {
        let mut client_options =
            ClientOptions::parse("mongodb+srv://<cluster-url>/<dbname>?w=majority").await?;
        client_options.credential = Some(
            Credential::builder()
                .mechanism(AuthMechanism::MongoDbX509)
                .build(),
        );
        let tls_options = TlsOptions::builder()
            .ca_file_path(PathBuf::from("/path/to/ca-cert"))
            .cert_key_file_path(PathBuf::from("/path/to/cert"))
            .build();
        client_options.tls = Some(Tls::Enabled(tls_options));
        let client = Client::with_options(client_options)?;

        let database = client.database("test");
        // do something with database

        Ok(())
    }
    // CONNECTION EXAMPLE ENDS HERE
}

#[cfg(feature = "sync")]
mod sync_scram {
    // SYNC SCRAM CONNECTION EXAMPLE STARTS HERE
    use mongodb::{options::ClientOptions, sync::Client};

    fn main() -> mongodb::error::Result<()> {
        let client_options = ClientOptions::parse(
            "mongodb+srv://<username>:<password>@<cluster-url>/<dbname>?w=majority",
        )
        .run()?;
        let client = Client::with_options(client_options)?;
        let database = client.database("test");
        // do something with database

        Ok(())
    }
    // CONNECTION EXAMPLE ENDS HERE
}

#[cfg(feature = "sync")]
mod sync_x509 {
    // SYNC X509 CONNECTION EXAMPLE STARTS HERE
    use mongodb::{
        options::{AuthMechanism, ClientOptions, Credential, Tls, TlsOptions},
        sync::Client,
    };
    use std::path::PathBuf;

    fn main() -> mongodb::error::Result<()> {
        let mut client_options =
            ClientOptions::parse("mongodb+srv://<cluster-url>/<dbname>?w=majority").run()?;
        client_options.credential = Some(
            Credential::builder()
                .mechanism(AuthMechanism::MongoDbX509)
                .build(),
        );
        let tls_options = TlsOptions::builder()
            .ca_file_path(PathBuf::from("/path/to/ca-cert"))
            .cert_key_file_path(PathBuf::from("/path/to/cert"))
            .build();
        client_options.tls = Some(Tls::Enabled(tls_options));
        let client = Client::with_options(client_options)?;

        let database = client.database("test");
        // do something with database

        Ok(())
    }
    // CONNECTION EXAMPLE ENDS HERE
}
