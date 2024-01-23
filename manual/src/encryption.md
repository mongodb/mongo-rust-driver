# Unstable API

To enable support for in-use encryption ([client-side field level encryption](https://www.mongodb.com/docs/manual/core/csfle/) and [queryable encryption](https://www.mongodb.com/docs/manual/core/queryable-encryption/)), enable the `"in-use-encryption-unstable"` feature of the `mongodb` crate.  As the name implies, the API for this feature is unstable, and may change in backwards-incompatible ways in minor releases.

# Client-Side Field Level Encryption

Starting with MongoDB 4.2, client-side field level encryption allows an application to encrypt specific data fields in addition to pre-existing MongoDB encryption features such as [Encryption at Rest](https://dochub.mongodb.org/core/security-encryption-at-rest) and [TLS/SSL (Transport Encryption)](https://dochub.mongodb.org/core/security-tls-transport-encryption).

With field level encryption, applications can encrypt fields in documents prior to transmitting data over the wire to the server. Client-side field level encryption supports workloads where applications must guarantee that unauthorized parties, including server administrators, cannot read the encrypted data.

See also the MongoDB documentation on [Client Side Field Level Encryption](https://dochub.mongodb.org/core/client-side-field-level-encryption).

## Dependencies

To get started using client-side field level encryption in your project, you will need to install [libmongocrypt](https://github.com/mongodb/libmongocrypt), which can be fetched from a [variety of package repositories](https://www.mongodb.com/docs/manual/core/csfle/reference/libmongocrypt/#std-label-csfle-reference-libmongocrypt).  If you install libmongocrypt in a location outside of the system library search path, the `MONGOCRYPT_LIB_DIR` environment variable will need to be set when compiling your project.

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
If the `mongodb` crate cannot load crypt_shared it will attempt to fallback to using mongocryptd by default.  Include `"cryptSharedRequired": true` in the `extra_options` document to always use crypt_shared and fail if it could not be loaded.

For detailed installation instructions see the [MongoDB documentation on Automatic Encryption Shared Library](https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/shared-library).

### mongocryptd

If using `crypt_shared` is not an option, the `mongocryptd` binary is required for automatic client-side encryption and is included as a component in the [MongoDB Enterprise Server package](https://dochub.mongodb.org/core/install-mongodb-enterprise). For detailed installation instructions see the [MongoDB documentation on mongocryptd](https://dochub.mongodb.org/core/client-side-field-level-encryption-mongocryptd).

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

Automatic client-side field level encryption is enabled by using the `Client::encrypted_builder` constructor method. The following examples show how to setup automatic client-side field level encryption using `ClientEncryption` to create a new encryption data key.

_Note_: Automatic client-side field level encryption requires MongoDB 4.2+ enterprise or a MongoDB 4.2+ Atlas cluster. The community version of the server supports automatic decryption as well as explicit client-side encryption.

### Providing Local Automatic Encryption Rules

The following example shows how to specify automatic encryption rules via the `schema_map` option. The automatic encryption rules are expressed using a [strict subset of the JSON Schema syntax](https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules).

Supplying a `schema_map` provides more security than relying on JSON Schemas obtained from the server. It protects against a malicious server advertising a false JSON Schema, which could trick the client into sending unencrypted data that should be encrypted.

JSON Schemas supplied in the `schema_map` only apply to configuring automatic client-side field level encryption. Other validation rules in the JSON schema will not be enforced by the driver and will result in an error.

<!--- Changes to this example should also be made to manual/deps/src/example/local_rules.rs --->
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
    key_vault.drop(None).await?;

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

The MongoDB 4.2+ server supports using schema validation to enforce encryption of specific fields in a collection. This schema validation will prevent an application from inserting unencrypted values for any fields marked with the `"encrypt"` JSON schema keyword.

The following example shows how to setup automatic client-side field level encryption using `ClientEncryption` to create a new encryption data key and create a collection with the [Automatic Encryption JSON Schema Syntax](https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules):

<!--- Changes to this example should also be made to manual/deps/src/example/server_side_enforcement.rs --->
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
            .write_concern(WriteConcern::majority())
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
    // This would return a Write error with the message "Document failed validation".
    // unencrypted_coll.insert_one(doc! { "encryptedField": "123456789" }, None)
    //    .await?;

    Ok(())
}
```

### Automatic Queryable Encryption

Verison 2.4.0 of the `mongodb` crate brings support for Queryable Encryption with MongoDB >=6.0.

Queryable Encryption is the second version of Client-Side Field Level Encryption. Data is encrypted client-side. Queryable Encryption supports indexed encrypted fields, which are further processed server-side.

You must have MongoDB 6.0 Enterprise to preview the feature.

Automatic encryption in Queryable Encryption is configured with an `encrypted_fields` mapping, as demonstrated by the following example:

<!--- Changes to this example should also be made to manual/deps/src/example/automatic_queryable_encryption.rs --->
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate rand;
# extern crate futures;
# static URI: &str = "mongodb://example.com";
use futures::TryStreamExt;
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
    key_vault.drop(None).await?;
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
    coll.drop(None).await?;
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
```

### Explicit Queryable Encryption

Verison 2.4.0 of the `mongodb` crate brings support for Queryable Encryption with MongoDB >=6.0.

Queryable Encryption is the second version of Client-Side Field Level Encryption. Data is encrypted client-side. Queryable Encryption supports indexed encrypted fields, which are further processed server-side.

Explicit encryption in Queryable Encryption is performed using the `encrypt` and `decrypt` methods. Automatic encryption (to allow the `find_one` to automatically decrypt) is configured using an `encrypted_fields` mapping, as demonstrated by the following example:

<!--- Changes to this example should also be made to manual/deps/src/example/explicit_queryable_encryption.rs --->
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate rand;
# static URI: &str = "mongodb://example.com";
use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    error::Result,
    mongocrypt::ctx::{KmsProvider, Algorithm},
    options::{ClientOptions, CreateCollectionOptions},
    Client,
    Namespace,
};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
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
    key_vault.drop(None).await?;
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
```

## Explicit Encryption

Explicit encryption is a MongoDB community feature and does not use the mongocryptd process. Explicit encryption is provided by the `ClientEncryption` struct, for example:

<!--- Changes to this example should also be made to manual/deps/src/example/explicit_encryption.rs --->
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate rand;
# static URI: &str = "mongodb://example.com";
use mongodb::{
    bson::{self, doc, Bson, Document},
    client_encryption::{ClientEncryption, MasterKey},
    error::Result,
    mongocrypt::ctx::{Algorithm, KmsProvider},
    Client,
    Namespace,
};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
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
    coll.drop(None).await?;

    // Set up the key vault (key_vault_namespace) for this example.
    let key_vault = client
        .database(&key_vault_namespace.db)
        .collection::<Document>(&key_vault_namespace.coll);
    key_vault.drop(None).await?;

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
```

## Explicit Encryption with Automatic Decryption

Although automatic encryption requires MongoDB 4.2+ enterprise or a MongoDB 4.2+ Atlas cluster, automatic decryption is supported for all users. To configure automatic decryption without automatic encryption set `bypass_auto_encryption` to `true` in the `EncryptedClientBuilder`:

<!--- Changes to this example should also be made to manual/deps/src/example/explicit_encryption_auto_decryption.rs --->
```rust,no_run
# extern crate mongodb;
# extern crate tokio;
# extern crate rand;
# static URI: &str = "mongodb://example.com";
use mongodb::{
    bson::{self, doc, Document},
    client_encryption::{ClientEncryption, MasterKey},
    error::Result,
    mongocrypt::ctx::{Algorithm, KmsProvider},
    options::ClientOptions,
    Client,
    Namespace,
};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
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
    coll.drop(None).await?;

    // Set up the key vault (key_vault_namespace) for this example.
    let key_vault = client
        .database(&key_vault_namespace.db)
        .collection::<Document>(&key_vault_namespace.coll);
    key_vault.drop(None).await?;

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
```