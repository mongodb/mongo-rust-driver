# In-Use Encryption: Benchmarks

______________________________________________________________________

## Benchmarking Bindings

Drivers are encouraged to benchmark the bindings to libmongocrypt. Benchmarking may help to identify performance issues
due to the cost of calling between the native language and the C library.

A handle to libmongocrypt (`mongocrypt_t`) is needed for the benchmark. In the public driver API, `mongocrypt_t` is an
implementation detail contained in a `MongoClient`. The bindings API may more directly interface `mongocrypt_t`.
Example: the Java bindings API contains a
[MongoCrypt class](https://github.com/mongodb/libmongocrypt/blob/master/bindings/java/mongocrypt/src/main/java/com/mongodb/crypt/capi/MongoCrypt.java)
closely wrapping the `mongocrypt_t`.

If possible, drivers are encouraged to use the bindings API and mock responses from the MongoDB server. This may help to
narrow the scope of the benchmarked code. See
[BenchmarkRunner.java](https://github.com/mongodb/libmongocrypt/blob/b81e66e0208d13e07c2e5e60b3170f0cfc61e1e2/bindings/java/mongocrypt/benchmarks/src/main/java/com/mongodb/crypt/benchmark/BenchmarkRunner.java)
for an example using the Java bindings API. If that is not possible, the benchmark can be implemented using the
`MongoClient` API, and the benchmark will include the time spent communicating with the MongoDB server.

### Benchmarking Bulk Decryption

Set up the benchmark data:

- Create a data key with the "local" KMS provider.
- Encrypt 1500 string values of the form `value 0001`, `value 0002`, `value 0003`, ... with the algorithm
  `AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic`.
- Create a document of the form:
  `{ "key0001": <encrypted "value 0001">, "key0002": <encrypted "value 0002">, "key0003": <encrypted "value 0003"> }`.
- Create a handle to `mongocrypt_t`. This may be through the bindings API (preferred) or through a `MongoClient`
  configured with `AutoEncryptionOpts`.

Warm up the benchmark:

- Use the handle to decrypt the document repeatedly for one second.

Run the benchmark. Repeat benchmark for thread counts: (1, 2, 8, 64):

- Start threads. Use the same handle between all threads (`mongocrypt_t` is thread-safe).
- In each thread: decrypt the document repeatedly for one second.
- Count the number of decrypt operations performed (ops/sec).
- Repeat 10 times.

Produce results:

- Report the median result of the ops/sec for each thread count.

**Note:** The `mongocrypt_t` handle caches the decrypted Data Encryption Key (DEK) for a fixed time period of one
minute. If the benchmark exceeds one minute, the DEK will be requested again from the key vault collection. Reporting
the median of trials is expected to prevent this impacting results.
