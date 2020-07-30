#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

extern crate mongodb;

use mongodb::error::Result;

const CONNECTION_URI: &str =
    "mongodb+srv://<username>:<password>@<cluster-url>/<dbname>?w=majority";

#[cfg(feature = "sync")]
fn main() -> Result<()> {
    use mongodb::sync::Client;
    let client = Client::with_uri_str(CONNECTION_URI)?;
    let database = client.database("test");
    Ok(())
}

#[cfg(feature = "tokio-runtime")]
#[tokio::main]
async fn main() -> Result<()> {
    use mongodb::{options::ClientOptions, Client};
    let client_options = ClientOptions::parse(CONNECTION_URI).await?;
    let client = Client::with_options(client_options)?;
    let database = client.database("test");
    Ok(())
}

#[cfg(feature = "async-std-runtime")]
#[async_std::main]
#[cfg(not(feature = "sync"))]
async fn main() -> Result<()> {
    use mongodb::{options::ClientOptions, Client};
    let client_options = ClientOptions::parse(CONNECTION_URI).await?;
    let client = Client::with_options(client_options)?;
    let database = client.database("test");
    Ok(())
}
