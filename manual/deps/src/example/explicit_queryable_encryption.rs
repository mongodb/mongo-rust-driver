use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    mongocrypt::ctx::{Algorithm, KmsProvider},
    options::{ClientOptions, CreateCollectionOptions},
    Client,
    Namespace,
};
use rand::Rng;

static URI: &str = "mongodb://localhost:27017";

type Result<T> = anyhow::Result<T>;

pub async fn example() -> Result<()> {
    // This must be the same master key that was used to create
    // the encryption key.
    let mut key_bytes = vec![0u8; 96];
    rand::thread_rng().fill(&mut key_bytes[..]);
    let local_master_key = bson::Binary {
        subtype: bson::spec::BinarySubtype::Generic,
        bytes: key_bytes,
    };
    let kms_providers = vec![(KmsProvider::Local, doc! { "key": local_master_key }, None)];

    // The MongoDB namespace (db.collection) used to store
    // the encryption data keys.
    let key_vault_namespace = Namespace::new("keyvault", "datakeys");

    // Set up the key vault (key_vault_namespace) for this example.
    let client = Client::with_uri_str(URI).await?;
    let key_vault = client
        .database(&key_vault_namespace.db)
        .collection::<Document>(&key_vault_namespace.coll);
    key_vault.drop().await?;
    let client_encryption = ClientEncryption::new(
        // The MongoClient to use for reading/writing to the key vault.
        // This can be the same MongoClient used by the main application.
        client,
        key_vault_namespace.clone(),
        kms_providers.clone(),
    )?;

    // Create a new data key for the encryptedField.
    let indexed_key_id = client_encryption
        .create_data_key(MasterKey::Local)
        .run()
        .await?;
    let unindexed_key_id = client_encryption
        .create_data_key(MasterKey::Local)
        .run()
        .await?;

    let encrypted_fields = doc! {
      "escCollection": "enxcol_.default.esc",
      "eccCollection": "enxcol_.default.ecc",
      "ecocCollection": "enxcol_.default.ecoc",
      "fields": [
        {
          "keyId": indexed_key_id.clone(),
          "path": "encryptedIndexed",
          "bsonType": "string",
          "queries": {
            "queryType": "equality"
          }
        },
        {
          "keyId": unindexed_key_id.clone(),
          "path": "encryptedUnindexed",
          "bsonType": "string",
        }
      ]
    };

    // The MongoClient used to read/write application data.
    let encrypted_client = Client::encrypted_builder(
        ClientOptions::parse(URI).await?,
        key_vault_namespace,
        kms_providers,
    )?
    .bypass_query_analysis(true)
    .build()
    .await?;
    let db = encrypted_client.database("test");
    db.drop().await?;

    // Create the collection with encrypted fields.
    db.create_collection(
        "coll",
        CreateCollectionOptions::builder()
            .encrypted_fields(encrypted_fields)
            .build(),
    )
    .await?;
    let coll = db.collection::<Document>("coll");

    // Create and encrypt an indexed and unindexed value.
    let val = "encrypted indexed value";
    let unindexed_val = "encrypted unindexed value";
    let insert_payload_indexed = client_encryption
        .encrypt(val, indexed_key_id.clone(), Algorithm::Indexed)
        .contention_factor(1)
        .run()
        .await?;
    let insert_payload_unindexed = client_encryption
        .encrypt(unindexed_val, unindexed_key_id, Algorithm::Unindexed)
        .run()
        .await?;

    // Insert the payloads.
    coll.insert_one(
        doc! {
            "encryptedIndexed": insert_payload_indexed,
            "encryptedUnindexed": insert_payload_unindexed,
        },
        None,
    )
    .await?;

    // Encrypt our find payload using QueryType.EQUALITY.
    // The value of `data_key_id` must be the same as used to encrypt the values
    // above.
    let find_payload = client_encryption
        .encrypt(val, indexed_key_id, Algorithm::Indexed)
        .query_type("equality")
        .contention_factor(1)
        .run()
        .await?;

    // Find the document we inserted using the encrypted payload.
    // The returned document is automatically decrypted.
    let doc = coll
        .find_one(doc! { "encryptedIndexed": find_payload }, None)
        .await?;
    println!("Returned document: {:?}", doc);

    Ok(())
}
