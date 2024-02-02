use crate as mongodb;

// begin lambda connection example 1
use lambda_runtime::{service_fn, LambdaEvent};
use mongodb::{bson::doc, Client};
use serde_json::Value;
use tokio::sync::OnceCell;

// Initialize a global static MongoDB Client.
static MONGODB_CLIENT: OnceCell<Client> = OnceCell::const_new();

async fn get_mongodb_client() -> &'static Client {
    MONGODB_CLIENT
        .get_or_init(|| async {
            let uri = std::env::var("MONGODB_URI")
                .expect("MONGODB_URI must be set to the URI of the MongoDB deployment");
            Client::with_uri_str(uri)
                .await
                .expect("Failed to create MongoDB Client")
        })
        .await
}

// Runs a ping operation on the "db" database and returns the response.
async fn handler(_: LambdaEvent<Value>) -> Result<Value, lambda_runtime::Error> {
    let client = get_mongodb_client().await;
    let response = client
        .database("db")
        .run_command(doc! { "ping": 1 })
        .await?;
    let json = serde_json::to_value(response)?;
    Ok(json)
}

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let service = service_fn(handler);
    lambda_runtime::run(service).await?;
    Ok(())
}
// end lambda connection example 1

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_handler() {
    if std::env::var("MONGODB_API_VERSION").is_ok() {
        return;
    }
    if std::env::var("MONGODB_URI").is_err() {
        std::env::set_var("MONGODB_URI", "mongodb://localhost:27017");
    }
    let event = LambdaEvent::new(Value::Null, Default::default());
    handler(event).await.unwrap();
}
