use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    mongocrypt::ctx::KmsProvider,
    options::ClientOptions,
    Client,
    Namespace,
};
use rand::Rng;

static URI: &str = "mongodb://localhost:27017";

type Result<T> = anyhow::Result<T>;

pub async fn example() -> Result<()> {
    // The MongoDB namespace (db.collection) used to store the
    // encrypted documents in this example.
    let encrypted_namespace = Namespace::new("test", "coll");

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
    let key_vault_namespace = Namespace::new("encryption", "__testKeyVault");

    // The MongoClient used to access the key vault (key_vault_namespace).
    let key_vault_client = Client::with_uri_str(URI).await?;
    let key_vault = key_vault_client
        .database(&key_vault_namespace.db)
        .collection::<Document>(&key_vault_namespace.coll);
    key_vault.drop().await?;

    let client_encryption = ClientEncryption::new(
        key_vault_client,
        key_vault_namespace.clone(),
        kms_providers.clone(),
    )?;
    // Create a new data key and json schema for the encryptedField.
    // https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules
    let data_key_id = client_encryption
        .create_data_key(MasterKey::Local)
        .key_alt_names(["encryption_example_1".to_string()])
        .run()
        .await?;
    let schema = doc! {
        "properties": {
            "encryptedField": {
                "encrypt": {
                    "keyId": [data_key_id],
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                }
            }
        },
        "bsonType": "object",
    };

    let client = Client::encrypted_builder(
        ClientOptions::parse(URI).await?,
        key_vault_namespace,
        kms_providers,
    )?
    .schema_map([(encrypted_namespace.to_string(), schema)])
    .build()
    .await?;
    let coll = client
        .database(&encrypted_namespace.db)
        .collection::<Document>(&encrypted_namespace.coll);
    // Clear old data.
    coll.drop().await?;

    coll.insert_one(doc! { "encryptedField": "123456789" }, None)
        .await?;
    println!("Decrypted document: {:?}", coll.find_one(None, None).await?);
    let unencrypted_coll = Client::with_uri_str(URI)
        .await?
        .database(&encrypted_namespace.db)
        .collection::<Document>(&encrypted_namespace.coll);
    println!(
        "Encrypted document: {:?}",
        unencrypted_coll.find_one(None, None).await?
    );

    Ok(())
}
