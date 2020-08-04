#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

extern crate mongodb;

#[cfg(feature = "tokio-runtime")]
use tokio::main;

#[cfg(feature = "async-std-runtime")]
use async_std::main;

use mongodb::error::Result;

#[cfg(feature = "sync")]
fn main() -> Result<()> {
    use mongodb::sync::Client;
    let client = Client::with_uri_str(
        "mongodb+srv://<username>:<password>@<cluster-url>/<dbname>?w=majority",
    )?;
    let database = client.database("test");
    Ok(())
}

#[cfg(not(feature = "sync"))]
#[crate::main]
async fn main() -> Result<()> {
    // CONNECTION EXAMPLE STARTS HERE
    use mongodb::{options::ClientOptions, Client};
    let client_options = ClientOptions::parse(
        "mongodb+srv://<username>:<password>@<cluster-url>/<dbname>?w=majority",
    )
    .await?;
    let client = Client::with_options(client_options)?;
    let database = client.database("test");
    // CONNECTION EXAMPLE ENDS HERE
    Ok(())
}
