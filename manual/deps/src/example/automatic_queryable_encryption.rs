use futures::TryStreamExt;
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
    let mut key_bytes = vec![0u8; 96];
    rand::thread_rng().fill(&mut key_bytes[..]);
    let local_master_key = bson::Binary {
        subtype: bson::spec::BinarySubtype::Generic,
        bytes: key_bytes,
    };
    let kms_providers = vec![(KmsProvider::Local, doc! { "key": local_master_key }, None)];
    let key_vault_namespace = Namespace::new("keyvault", "datakeys");
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
    let key1_id = client_encryption
        .create_data_key(MasterKey::Local)
        .key_alt_names(["firstName".to_string()])
        .run()
        .await?;
    let key2_id = client_encryption
        .create_data_key(MasterKey::Local)
        .key_alt_names(["lastName".to_string()])
        .run()
        .await?;

    let encrypted_fields_map = vec![(
        "example.encryptedCollection",
        doc! {
            "escCollection": "encryptedCollection.esc",
            "eccCollection": "encryptedCollection.ecc",
            "ecocCollection": "encryptedCollection.ecoc",
            "fields": [
              {
                "path": "firstName",
                "bsonType": "string",
                "keyId": key1_id,
                "queries": [{"queryType": "equality"}],
              },
                {
                  "path": "lastName",
                  "bsonType": "string",
                  "keyId": key2_id,
                }
            ]
        },
    )];

    let client = Client::encrypted_builder(
        ClientOptions::parse(URI).await?,
        key_vault_namespace,
        kms_providers,
    )?
    .encrypted_fields_map(encrypted_fields_map)
    .build()
    .await?;
    let db = client.database("example");
    let coll = db.collection::<Document>("encryptedCollection");
    coll.drop().await?;
    db.create_collection("encryptedCollection", None).await?;
    coll.insert_one(
        doc! { "_id": 1, "firstName": "Jane", "lastName": "Doe" },
        None,
    )
    .await?;
    let docs: Vec<_> = coll
        .find(doc! {"firstName": "Jane"}, None)
        .await?
        .try_collect()
        .await?;
    println!("{:?}", docs);

    Ok(())
}
