use crate as mongodb;

// begin lambda connection example 2
use lambda_runtime::{service_fn, LambdaEvent};
use mongodb::{
    bson::doc,
    options::{AuthMechanism, ClientOptions, Credential},
    Client,
};
use serde_json::Value;
use tokio::sync::OnceCell;

// Initialize a global static MongoDB Client with AWS authentication. The following environment
// variables should also be set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and, optionally,
// AWS_SESSION_TOKEN.
static MONGODB_CLIENT: OnceCell<Client> = OnceCell::const_new();

async fn get_mongodb_client() -> &'static Client {
    MONGODB_CLIENT
        .get_or_init(|| async {
            let uri = std::env::var("MONGODB_URI")
                .expect("MONGODB_URI must be set to the URI of the MongoDB deployment");
            let mut options = ClientOptions::parse(&uri)
                .await
                .expect("Failed to parse options from URI");
            let credential = Credential::builder()
                .mechanism(AuthMechanism::MongoDbAws)
                .build();
            options.credential = Some(credential);
            Client::with_options(options).expect("Failed to create MongoDB Client")
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
// end lambda connection example 2

// Runs a ping operation on the "db" database and returns the response.
async fn handler_create_client(_: LambdaEvent<Value>) -> Result<Value, lambda_runtime::Error> {
    let uri = std::env::var("MONGODB_URI").unwrap();
    let client = Client::with_uri_str(uri).await.unwrap();
    let response = client
        .database("db")
        .run_command(doc! { "ping": 1 })
        .await?;
    let json = serde_json::to_value(response)?;
    Ok(json)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_handler() {
    let event = LambdaEvent::new(Value::Null, Default::default());
    handler_create_client(event).await.unwrap();
}
