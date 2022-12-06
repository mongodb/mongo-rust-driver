# Client-Side Field Level Encryption

Starting with MongoDB 4.2, client-side field level encryption allows an application to encrypt specific data fields in addition to pre-existing MongoDB encryption features such as [Encryption at Rest](https://dochub.mongodb.org/core/security-encryption-at-rest) and [TLS/SSL (Transport Encryption)](https://dochub.mongodb.org/core/security-tls-transport-encryption).

With field level encryption, applications can encrypt fields in documents prior to transmitting data over the wire to the server. Client-side field level encryption supports workloads where applications must guarantee that unauthorized parties, including server administrators, cannot read the encrypted data.

See also the MongoDB documentation on [Client Side Field Level Encryption](https://dochub.mongodb.org/core/client-side-field-level-encryption).

## Dependencies

To get started using client-side field level encryption in your project, you will need to install [libmongocrypt](https://github.com/mongodb/libmongocrypt), which can be compiled from source or fetched from a variety of package repositories; for more information, see the libmongocrypt [README](https://github.com/mongodb/libmongocrypt/blob/master/README.md).  If you install libmongocrypt in a location outside of the system library search path, the `MONGOCRYPT_LIB_DIR` environment variable will need to be set when compiling your project.

Additionally, either `crypt_shared` or `mongocryptd` are required in order to use automatic client-side encryption.

### crypt_shared

The Automatic Encryption Shared Library (crypt_shared) provides the same functionality as mongocryptd, but does not require you to spawn another process to perform automatic encryption.

By default, the `mongodb` crate attempts to load crypt_shared from the system and if found uses it automatically. To load crypt_shared from another location, set the `"cryptSharedLibPath"` field in `extra_options`:
```rust,no_run
# extern crate mongodb;
# use mongodb::{bson::doc, Client, error::Result};
#
# async fn func() -> Result<()> {
# let options = todo!();
# let kv_namespace = todo!();
# let kms_providers: Vec<_> = todo!();
let client = Client::encrypted_builder(options, kv_namespace, kms_providers)?
    .extra_options(doc! {
        "cryptSharedLibPath": "/path/to/crypt/shared",
    })
    .build();
#
# Ok(())
# }
```
If the `mongodb` crate load crypt_shared it will attempt to fallback to using mongocryptd by default.  Include `"cryptSharedRequired": true` in the `extra_options` document to always use crypt_shared and fail if it could not be loaded.

For detailed installation instructions see the [MongoDB documentation on Automatic Encryption Shared Library](https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/shared-library).

### mongocryptd

The `mongocryptd` binary is required for automatic client-side encryption and is included as a component in the [MongoDB Enterprise Server package](https://dochub.mongodb.org/core/install-mongodb-enterprise). For detailed installation instructions see the [MongoDB documentation on mongocryptd](https://dochub.mongodb.org/core/client-side-field-level-encryption-mongocryptd).

`mongocryptd` performs the following:
* Parses the automatic encryption rules specified to the database connection. If the JSON schema contains invalid automatic encryption syntax or any document validation syntax, `mongocryptd` returns an error.
* Uses the specified automatic encryption rules to mark fields in read and write operations for encryption.
* Rejects read/write operations that may return unexpected or incorrect results when applied to an encrypted field. For supported and unsupported operations, see [Read/Write Support with Automatic Field Level Encryption](https://dochub.mongodb.org/core/client-side-field-level-encryption-read-write-support).

A `Client` configured with auto encryption will automatically spawn the `mongocryptd` process from the application's `PATH`. Applications can control the spawning behavior as part of the automatic encryption options:
```rust,no_run
# extern crate mongodb;
# use mongodb::{bson::doc, Client, error::Result};
#
# async fn func() -> Result<()> {
# let options = todo!();
# let kv_namespace = todo!();
# let kms_providers: Vec<_> = todo!();
let client = Client::encrypted_builder(options, kv_namespace, kms_providers)?
    .extra_options(doc! {
        "mongocryptdSpawnPath": "/path/to/mongocryptd",
        "mongocryptdSpawnArgs": ["--logpath=/path/to/mongocryptd.log", "--logappend"],
    })
    .build();
#
# Ok(())
# }
```
If your application wishes to manage the `mongocryptd` process manually, it is possible to disable spawning `mongocryptd`:
```rust,no_run
# extern crate mongodb;
# use mongodb::{bson::doc, Client, error::Result};
#
# async fn func() -> Result<()> {
# let options = todo!();
# let kv_namespace = todo!();
# let kms_providers: Vec<_> = todo!();
let client = Client::encrypted_builder(options, kv_namespace, kms_providers)?
    .extra_options(doc! {
        "mongocryptdBypassSpawn": true,
        "mongocryptdURI": "mongodb://localhost:27020",
    })
    .build();
#
# Ok(())
# }
```
`mongocryptd` is only responsible for supporting automatic client-side field level encryption and does not itself perform any encryption or decryption.

## Automatic Client-Side Field Level Encryption

Automatic client-side field level encryption is enabled by using the `Client::encrypted_builder` constructor method.  The following examples show how to setup automatic client-side field level encryption using `ClientEncryption` to create a new encryption data key.

_Note_: Automatic client-side field level encryption requires MongoDB 4.2 enterprise or a MongoDB 4.2 Atlas cluster. The community version of the server supports automatic decryption as well as explicit client-side encryption.

### Providing Local Automatic Encryption Rules

The following example shows how to specify automatic encryption rules via the `schema_map` option. The automatic encryption rules are expressed using a [strict subset of the JSON Schema syntax](https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules).

Supplying a `schema_map` provides more security than relying on JSON Schemas obtained from the server. It protects against a malicious server advertising a false JSON Schema, which could trick the client into sending unencrypted data that should be encrypted.

JSON Schemas supplied in the `schema_map` only apply to configuring automatic client-side field level encryption. Other validation rules in the JSON schema will not be enforced by the driver and will result in an error.

```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate rand;
# static URI: &str = "mongodb://example.com";
use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    error::Result,
    mongocrypt::ctx::KmsProvider,
    options::ClientOptions,
    Client,
    Namespace,
};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
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
    // Ensure that two data keys cannot share the same keyAltName.
    key_vault.drop(None).await?;
    key_vault
        .create_index(
            mongodb::IndexModel::builder()
                .options(
                    mongodb::options::IndexOptions::builder()
                        .name("keyAltNames".to_string())
                        .unique(true)
                        .partial_filter_expression(doc! { "keyAltNames": {"$exists": true} })
                        .build(),
                )
                .build(),
            None,
        )
        .await?;

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
    coll.drop(None).await?;

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
```

### Server-Side Field Level Encryption Enforcement

The MongoDB 4.2 server supports using schema validation to enforce encryption of specific fields in a collection. This schema validation will prevent an application from inserting unencrypted values for any fields marked with the `"encrypt"` JSON schema keyword.

The following example shows how to setup automatic client-side field level encryption using `ClientEncryption` to create a new encryption data key and create a collection with the [Automatic Encryption JSON Schema Syntax](https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules):

```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate rand;
# static URI: &str = "mongodb://example.com";
use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    error::Result,
    mongocrypt::ctx::KmsProvider,
    options::{ClientOptions, CreateCollectionOptions, WriteConcern},
    Client,
    Namespace,
};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
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
    key_vault.drop(None).await?;
    
    let client_encryption = ClientEncryption::new(
        key_vault_client,
        key_vault_namespace.clone(),
        kms_providers.clone(),
    )?;

    // Create a new data key and json schema for the encryptedField.
    let data_key_id = client_encryption
        .create_data_key(MasterKey::Local)
        .key_alt_names(["encryption_example_2".to_string()])
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
    .build()
    .await?;
    let db = client.database(&encrypted_namespace.db);
    let coll = db.collection::<Document>(&encrypted_namespace.coll);
    // Clear old data
    coll.drop(None).await?;
    // Create the collection with the encryption JSON Schema.
    db.create_collection(
        &encrypted_namespace.coll,
        CreateCollectionOptions::builder()
            .write_concern(WriteConcern::MAJORITY)
            .validator(doc! { "$jsonSchema": schema })
            .build(),
    ).await?;

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
```