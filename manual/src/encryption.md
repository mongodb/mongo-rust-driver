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
# let client_options = todo!();
# let key_vault_namespace = todo!();
# let kms_providers: Vec<_> = todo!();
let client = Client::encrypted_builder(
    client_options,
    key_vault_namespace,
    kms_providers
)?
.extra_options(doc! {
    "cryptSharedLibPath": "/path/to/crypt/shared",
});
#
# Ok(())
# }