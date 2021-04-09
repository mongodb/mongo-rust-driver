// This file ensures the examples from the README compile.
// Be sure not to `use` anything outside of the examples, since the examples are in charge of
// specifying anything that needs to be imported.

struct Err {}
impl From<mongodb::error::Error> for Err {
    fn from(_error: mongodb::error::Error) -> Self {
        Err {}
    }
}
#[allow(dead_code)]
type Result<T> = std::result::Result<T, Err>;

#[cfg(not(feature = "sync"))]
async fn _connecting() -> Result<()> {
    use mongodb::{options::ClientOptions, Client};

    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;

    // Manually set an option.
    client_options.app_name = Some("My App".to_string());

    // Get a handle to the deployment.
    let client = Client::with_options(client_options)?;

    // List the names of the databases in that deployment.
    for db_name in client.list_database_names(None, None).await? {
        println!("{}", db_name);
    }

    Ok(())
}

#[cfg(not(feature = "sync"))]
async fn _getting_handle_to_database(client: mongodb::Client) -> Result<()> {
    // Get a handle to a database.
    let db = client.database("mydb");

    // List the names of the collections in that database.
    for collection_name in db.list_collection_names(None).await? {
        println!("{}", collection_name);
    }

    Ok(())
}

#[cfg(not(feature = "sync"))]
async fn _inserting_documents_into_a_collection(db: mongodb::Database) -> Result<()> {
    use mongodb::bson::{doc, Document};

    // Get a handle to a collection in the database.
    let collection = db.collection::<Document>("books");

    let docs = vec![
        doc! { "title": "1984", "author": "George Orwell" },
        doc! { "title": "Animal Farm", "author": "George Orwell" },
        doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
    ];

    // Insert some documents into the "mydb.books" collection.
    collection.insert_many(docs, None).await?;

    Ok(())
}

#[cfg(not(feature = "sync"))]
async fn _finding_documents_into_a_collection(
    collection: mongodb::Collection<mongodb::bson::Document>,
) -> Result<()> {
    use futures::stream::StreamExt;
    use mongodb::{
        bson::{doc, Bson},
        options::FindOptions,
    };

    // Query the documents in the collection with a filter and an option.
    let filter = doc! { "author": "George Orwell" };
    let find_options = FindOptions::builder().sort(doc! { "title": 1 }).build();
    let mut cursor = collection.find(filter, find_options).await?;

    // Iterate over the results of the cursor.
    while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                if let Some(title) = document.get("title").and_then(Bson::as_str) {
                    println!("title: {}", title);
                } else {
                    println!("no title found");
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}

#[cfg(feature = "sync")]
async fn _using_the_sync_api() -> Result<()> {
    use mongodb::{
        bson::{doc, Bson, Document},
        sync::Client,
    };

    let client = Client::with_uri_str("mongodb://localhost:27017")?;
    let database = client.database("mydb");
    let collection = database.collection::<Document>("books");

    let docs = vec![
        doc! { "title": "1984", "author": "George Orwell" },
        doc! { "title": "Animal Farm", "author": "George Orwell" },
        doc! { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
    ];

    // Insert some documents into the "mydb.books" collection.
    collection.insert_many(docs, None)?;

    let cursor = collection.find(doc! { "author": "George Orwell" }, None)?;
    for result in cursor {
        match result {
            Ok(document) => {
                if let Some(title) = document.get("title").and_then(Bson::as_str) {
                    println!("title: {}", title);
                } else {
                    println!("no title found");
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
