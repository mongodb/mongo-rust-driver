#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

extern crate mongodb;

#[cfg(feature = "tokio-runtime")]
// CONNECTION EXAMPLE STARTS HERE
#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    use mongodb::{options::ClientOptions, Client};
    let client_options = ClientOptions::parse(
        "mongodb+srv://<username>:<password>@<cluster-url>/<dbname>?w=majority",
    )
    .await?;
    let client = Client::with_options(client_options)?;
    let database = client.database("test");
    Ok(())
}
// CONNECTION EXAMPLE ENDS HERE
