use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use mongodb::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

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

#[derive(Deserialize)]
struct Request {
}

#[derive(Serialize)]
struct Response {
    #[serde(rename = "statusCode")]
    status_code: i32,
    body: String,
}

async fn function_handler(_event: LambdaEvent<Request>) -> Result<Response, Error> {
    let client = get_mongodb_client().await;
    let names = client.list_database_names(None, None).await?;
    let resp = Response {
        status_code: 200,
        body: format!("{:?}", names),
    };

    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(function_handler)).await
}