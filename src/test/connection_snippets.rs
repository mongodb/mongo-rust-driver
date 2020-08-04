#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use crate::error::Result;
use crate as mongodb;

#[cfg(feature = "tokio-runtime")]
async fn connect() -> Result<()> {
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
