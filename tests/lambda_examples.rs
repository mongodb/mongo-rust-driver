use async_once::AsyncOnce;
use lambda_runtime::{service_fn, LambdaEvent};
use lazy_static::lazy_static;
#[cfg(feature = "aws-auth")]
use mongodb::options::{AuthMechanism, ClientOptions, Credential};
use mongodb::{bson::doc, Client};
use serde_json::Value;

#[cfg(not(feature = "aws-auth"))]
// START EXAMPLE 1
// Initialize a global static MongoDB Client.
//
// The client can be accessed as follows:
// let client = MONGODB_CLIENT.get().await;
lazy_static! {
    static ref MONGODB_CLIENT: AsyncOnce<Client> = AsyncOnce::new(async {
        let uri = std::env::var("MONGODB_URI")
            .expect("MONGODB_URI must be set to the URI of the MongoDB deployment");
        Client::with_uri_str(uri)
            .await
            .expect("Failed to create MongoDB Client")
    });
}

// Runs a ping operation on the "db" database and returns the response.
async fn handler(_: LambdaEvent<Value>) -> Result<Value, lambda_runtime::Error> {
    let client = MONGODB_CLIENT.get().await;
    let response = client
        .database("db")
        .run_command(doc! { "ping": 1 }, None)
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
// END EXAMPLE 1

#[cfg(feature = "aws-auth")]
// START EXAMPLE 2
// Initialize a global static MongoDB Client with AWS authentication.
//
// The client can be accessed as follows:
// let client = MONGODB_CLIENT.get().await;
lazy_static! {
    static ref MONGODB_CLIENT: AsyncOnce<Client> = AsyncOnce::new(async {
        let uri = std::env::var("MONGODB_URI")
            .expect("MONGODB_URI must be set to the URI of the MongoDB deployment");
        let mut options = ClientOptions::parse(uri)
            .await
            .expect("Failed to parse options from URI");
        let credential = Credential::builder()
            .mechanism(AuthMechanism::MongoDbAws)
            .build();
        options.credential = Some(credential);
        Client::with_options(options).expect("Failed to create MongoDB Client")
    });
}
// END EXAMPLE 2

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_handler() {
    let event = LambdaEvent::new(Value::Null, Default::default());
    handler(event).await.unwrap();
}
