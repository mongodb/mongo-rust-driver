use mongodb::{bson::Document, Client};

#[tokio::main]
async fn main() {
    let uri = std::env::var("MONGODB_URI").expect("no URI given!");
    let client = Client::with_uri_str(&uri).await.unwrap();
    
    client
        .database("aws")
        .collection::<Document>("somecoll")
        .find_one(None, None)
        .await
        .unwrap();
}
