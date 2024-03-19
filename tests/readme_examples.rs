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

async fn _connecting() -> Result<()> {
    use mongodb::{options::ClientOptions, Client};

    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;

    // Manually set an option.
    client_options.app_name = Some("My App".to_string());

    // Get a handle to the deployment.
    let client = Client::with_options(client_options)?;

    // List the names of the databases in that deployment.
    for db_name in client.list_database_names().await? {
        println!("{}", db_name);
    }

    Ok(())
}

async fn _getting_handle_to_database(client: mongodb::Client) -> Result<()> {
    // Get a handle to a database.
    let db = client.database("mydb");

    // List the names of the collections in that database.
    for collection_name in db.list_collection_names().await? {
        println!("{}", collection_name);
    }

    Ok(())
}

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
    collection.insert_many(docs).await?;

    Ok(())
}

use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}

async fn _inserting_documents_into_a_typed_collection(db: mongodb::Database) -> Result<()> {
    // Get a handle to a collection of `Book`.
    let typed_collection = db.collection::<Book>("books");

    let books = vec![
        Book {
            title: "The Grapes of Wrath".to_string(),
            author: "John Steinbeck".to_string(),
        },
        Book {
            title: "To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
    ];

    // Insert the books into "mydb.books" collection, no manual conversion to BSON necessary.
    typed_collection.insert_many(books).await?;

    Ok(())
}

async fn _finding_documents_into_a_collection(
    typed_collection: mongodb::Collection<Book>,
) -> Result<()> {
    // This trait is required to use `try_next()` on the cursor
    use futures::stream::TryStreamExt;
    use mongodb::bson::doc;

    // Query the books in the collection with a filter and an option.
    let filter = doc! { "author": "George Orwell" };
    let mut cursor = typed_collection.find(filter).sort(doc! { "title": 1 }).await?;

    // Iterate over the results of the cursor.
    while let Some(book) = cursor.try_next().await? {
        println!("title: {}", book.title);
    }

    Ok(())
}

#[cfg(feature = "sync")]
async fn _using_the_sync_api() -> Result<()> {
    use mongodb::{bson::doc, sync::Client};

    let client = Client::with_uri_str("mongodb://localhost:27017")?;
    let database = client.database("mydb");
    let collection = database.collection::<Book>("books");

    let docs = vec![
        Book {
            title: "1984".to_string(),
            author: "George Orwell".to_string(),
        },
        Book {
            title: "Animal Farm".to_string(),
            author: "George Orwell".to_string(),
        },
        Book {
            title: "The Great Gatsby".to_string(),
            author: "F. Scott Fitzgerald".to_string(),
        },
    ];

    // Insert some books into the "mydb.books" collection.
    collection.insert_many(docs).run()?;

    let cursor = collection.find(doc! { "author": "George Orwell" }).run()?;
    for result in cursor {
        println!("title: {}", result?.title);
    }

    Ok(())
}

async fn _windows_dns_note() -> Result<()> {
    use mongodb::{
        options::{ClientOptions, ResolverConfig},
        Client,
    };

    let options = ClientOptions::parse_with_resolver_config(
        "mongodb+srv://my.host.com",
        ResolverConfig::cloudflare(),
    )
    .await?;
    let client = Client::with_options(options)?;

    drop(client);
    Ok(())
}
