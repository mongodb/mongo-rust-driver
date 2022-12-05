# Client-Side Field Level Encryption

Starting with MongoDB 4.2, client-side field level encryption allows an application to encrypt specific data fields in addition to pre-existing MongoDB encryption features such as [Encryption at Rest](https://dochub.mongodb.org/core/security-encryption-at-rest) and [TLS/SSL (Transport Encryption)](https://dochub.mongodb.org/core/security-tls-transport-encryption).

With field level encryption, applications can encrypt fields in documents prior to transmitting data over the wire to the server. Client-side field level encryption supports workloads where applications must guarantee that unauthorized parties, including server administrators, cannot read the encrypted data.

See also the MongoDB documentation on [Client Side Field Level Encryption](https://dochub.mongodb.org/core/client-side-field-level-encryption).

## Dependencies

To get started using client-side field level encryption in your project, you will need to install [libmongocrypt](https://github.com/mongodb/libmongocrypt), which can be compiled from source or fetched from a variety of package repositories; for more information, see the libmongocrypt [README](https://github.com/mongodb/libmongocrypt/blob/master/README.md).  If you install libmongocrypt in a location outside of the system library search path, the `MONGOCRYPT_LIB_DIR` environment variable will need to be set when compiling your project.

Additionally, either [crypt_shared](https://github.com/mongodb/mongo-python-driver/blob/master/doc/examples/encryption.rst#crypt-shared) or [mongocryptd](https://github.com/mongodb/mongo-python-driver/blob/master/doc/examples/encryption.rst#mongocryptd) are required in order to use automatic client-side encryption.

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

