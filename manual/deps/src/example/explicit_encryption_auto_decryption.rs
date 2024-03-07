use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    mongocrypt::ctx::{Algorithm, KmsProvider},
    options::ClientOptions,
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

    // `bypass_auto_encryption(true)` disables automatic encryption but keeps
    // the automatic _decryption_ behavior. bypass_auto_encryption will
    // also disable spawning mongocryptd.
    let client = Client::encrypted_builder(
        ClientOptions::parse(URI).await?,
        key_vault_namespace.clone(),
        kms_providers.clone(),
    )?
    .bypass_auto_encryption(true)
    .build()
    .await?;
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
        .key_alt_names(["encryption_example_4".to_string()])
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
    // Automatically decrypts any encrypted fields.
    let doc = coll.find_one(None, None).await?.unwrap();
    println!("Decrypted document: {:?}", doc);
    let unencrypted_coll = Client::with_uri_str(URI)
        .await?
        .database("test")
        .collection::<Document>("coll");
    println!(
        "Encrypted document: {:?}",
        unencrypted_coll.find_one(None, None).await?
    );

    Ok(())
}
