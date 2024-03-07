use mongodb::{
    bson::{self, doc, Bson, Document},
    client_encryption::{ClientEncryption, MasterKey},
    mongocrypt::ctx::{Algorithm, KmsProvider},
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

    // The MongoClient used to read/write application data.
    let client = Client::with_uri_str(URI).await?;
    let coll = client.database("test").collection::<Document>("coll");
    // Clear old data
    coll.drop().await?;

    // Set up the key vault (key_vault_namespace) for this example.
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
    let data_key_id = client_encryption
        .create_data_key(MasterKey::Local)
        .key_alt_names(["encryption_example_3".to_string()])
        .run()
        .await?;

    // Explicitly encrypt a field:
    let encrypted_field = client_encryption
        .encrypt(
            "123456789",
            data_key_id,
            Algorithm::AeadAes256CbcHmacSha512Deterministic,
        )
        .run()
        .await?;
    coll.insert_one(doc! { "encryptedField": encrypted_field }, None)
        .await?;
    let mut doc = coll.find_one(None, None).await?.unwrap();
    println!("Encrypted document: {:?}", doc);

    // Explicitly decrypt the field:
    let field = match doc.get("encryptedField") {
        Some(Bson::Binary(bin)) => bin,
        _ => panic!("invalid field"),
    };
    let decrypted: Bson = client_encryption
        .decrypt(field.as_raw_binary())
        .await?
        .try_into()?;
    doc.insert("encryptedField", decrypted);
    println!("Decrypted document: {:?}", doc);

    Ok(())
}
