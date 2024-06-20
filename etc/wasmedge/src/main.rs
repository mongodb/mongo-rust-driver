#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .unwrap();
    println!("{:?}", client.list_database_names().await);
}
