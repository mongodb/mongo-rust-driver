# Client Backpressure Tests

______________________________________________________________________

## Introduction

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
retryable reads. These tests utilize the [Unified Test Format](../../unified-test-format/unified-test-format.md).

Several prose tests, which are not easily expressed in YAML, are also presented in this file. Those tests will need to
be manually implemented by each driver.

### Prose Tests

#### Test 1: Operation Retry Uses Exponential Backoff

Drivers should test that retries do not occur immediately when a SystemOverloadedError is encountered.

1. Let `client` be a `MongoClient`
2. Let `collection` be a collection
3. Now, run transactions without backoff:
    1. Configure the random number generator used for jitter to always return `0` -- this effectively disables backoff.

    2. Configure the following failPoint:

        ```javascript
            {
                configureFailPoint: 'failCommand',
                mode: 'alwaysOn',
                data: {
                    failCommands: ['insert'],
                    errorCode: 2,
                    errorLabels: ['SystemOverloadedError', 'RetryableError']
                }
            }
        ```

    3. Insert the document `{ a: 1 }`. Expect that the command errors. Measure the duration of the command execution.

        ```javascript
           const start = performance.now();
           expect(
            await coll.insertOne({ a: 1 }).catch(e => e)
           ).to.be.an.instanceof(MongoServerError);
           const end = performance.now();
        ```

    4. Configure the random number generator used for jitter to always return `1`.

    5. Execute step 3 again.

    6. Compare the two time between the two runs.

        ```python
        assertTrue(with_backoff_time - no_backoff_time >= 2.1)
        ```

        The sum of 5 backoffs is 3.1 seconds. There is a 1-second window to account for potential variance between the two
        runs.

#### Test 2: Token Bucket Capacity is Enforced

Drivers should test that retry token buckets are created at their maximum capacity and that that capacity is enforced.

1. Let `client` be a `MongoClient` with `adaptiveRetries=True`.
2. Assert that the client's retry token bucket is at full capacity and that the capacity is
    `DEFAULT_RETRY_TOKEN_CAPACITY`.
3. Using `client`, execute a successful `ping` command.
4. Assert that the successful command did not increase the number of tokens in the bucket above
    `DEFAULT_RETRY_TOKEN_CAPACITY`.

#### Test 3: Overload Errors are Retried a Maximum of MAX_RETRIES times

Drivers should test that without adaptive retries enabled, overload errors are retried a maximum of five times.

1. Let `client` be a `MongoClient` with command event monitoring enabled.

2. Let `coll` be a collection.

3. Configure the following failpoint:

    ```javascript
        {
            configureFailPoint: 'failCommand',
            mode: 'alwaysOn',
            data: {
                failCommands: ['find'],
                errorCode: 462,  // IngressRequestRateLimitExceeded
                errorLabels: ['SystemOverloadedError', 'RetryableError']
            }
        }
    ```

4. Perform a find operation with `coll` that fails.

5. Assert that the raised error contains both the `RetryableError` and `SystemOverloadedError` error labels.

6. Assert that the total number of started commands is MAX_RETRIES + 1 (6).

#### Test 4: Adaptive Retries are Limited by Token Bucket Tokens

Drivers should test that when enabled, adaptive retries are limited by the number of tokens in the bucket.

1. Let `client` be a `MongoClient` with `adaptiveRetries=True` and command event monitoring enabled.

2. Set `client`'s retry token bucket to have 2 tokens.

3. Let `coll` be a collection.

4. Configure the following failpoint:

    ```javascript
        {
            configureFailPoint: 'failCommand',
            mode: {times: 3},
            data: {
                failCommands: ['find'],
                errorCode: 462,  // IngressRequestRateLimitExceeded
                errorLabels: ['SystemOverloadedError', 'RetryableError']
            }
        }
    ```

5. Perform a find operation with `coll` that fails.

6. Assert that the raised error contains both the `RetryableError` and `SystemOverloadedError` error labels.

7. Assert that the total number of started commands is 3: one for the initial attempt and two for the retries.
