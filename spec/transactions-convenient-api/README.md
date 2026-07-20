# Convenient API for Transactions Tests

______________________________________________________________________

## Introduction

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
the Convenient API for Transactions spec. These tests utilize the
[Unified Test Format](../../unified-test-format/unified-test-format.md).

Several prose tests, which are not easily expressed in YAML, are also presented in this file. Those tests will need to
be manually implemented by each driver.

## Prose Tests

### Callback Raises a Custom Error

Write a callback that raises a custom exception or error that does not include either UnknownTransactionCommitResult or
TransientTransactionError error labels. Execute this callback using `withTransaction` and assert that the callback's
error bypasses any retry logic within `withTransaction` and is propagated to the caller of `withTransaction`.

### Callback Returns a Value

Write a callback that returns a custom value (e.g. boolean, string, object). Execute this callback using
`withTransaction` and assert that the callback's return value is propagated to the caller of `withTransaction`.

### Retry Timeout is Enforced

Drivers should test that `withTransaction` enforces a non-configurable timeout before retrying both commits and entire
transactions. Specifically, three cases should be checked:

- If the callback raises an error with the `TransientTransactionError` label and the retry timeout has been exceeded,
    `withTransaction` should propagate the error as described in the
    [propagation mechanism](../transactions-convenient-api.md#timeout-error-propagation) to its caller.
- If committing raises an error with the `UnknownTransactionCommitResult` label, and the retry timeout has been
    exceeded, `withTransaction` should propagate the error as described in the
    [propagation mechanism](../transactions-convenient-api.md#timeout-error-propagation) to its caller
- If committing raises an error with the `TransientTransactionError` label and the retry timeout has been exceeded,
    `withTransaction` should propagate the error as described in the
    [propagation mechanism](../transactions-convenient-api.md#timeout-error-propagation) to its caller. This case may
    occur if the commit was internally retried against a new primary after a failover and the second primary returned a
    `NoSuchTransaction` error response.

If possible, drivers should implement these tests without requiring the test runner to block for the full duration of
the retry timeout. This might be done by internally modifying the timeout value used by `withTransaction` with some
private API or using a mock timer.

The drivers should assert that the timeout error propagated has the same labels as the error it wraps.

### Retry Backoff is Enforced

Drivers should test that retries within `withTransaction` do not occur immediately.

1. let `client` be a `MongoClient`
2. let `coll` be a collection
3. Now, run transactions without backoff:
    1. Configure the random number generator used for jitter to always return `0` -- this effectively disables backoff.

    2. Configure a fail point that forces 13 retries like so:

        ```python
           set_fail_point(
              {
                  "configureFailPoint": "failCommand",
                  "mode": {
                      "times": 13
                  },  # sufficiently high enough such that the time effect of backoff is noticeable
                  "data": {
                      "failCommands": ["commitTransaction"],
                      "errorCode": 251,
                  },
              }
           )
        ```

        > Note: errorCode 251 is NoSuchTransaction.

    3. Define the callback for the transaction as follows:

        ```python
           def callback(session):
              coll.insert_one({}, session=session)
        ```

    4. Let `no_backoff_time` be the duration of the withTransaction API call:

        ```python
           start = time.monotonic()
           with client.start_session() as s:
              s.with_transaction(callback)
           end = time.monotonic()
           no_backoff_time = end - start
        ```
4. Now run the command with backoff:
    1. Configure the random number generator used for jitter to always return a number as close as possible to `1`.
    2. Configure a fail point that forces 13 retries like in step 3.2.
    3. Use the same callback defined in 3.3.
    4. Let `with_backoff_time` be the duration of the withTransaction API call:
        ```python
           start = time.monotonic()
           with client.start_session() as s:
              s.with_transaction(callback)
           end = time.monotonic()
           no_backoff_time = end - start
        ```
5. Compare the durations of the two runs.
    ```python
    assertTrue(absolute_value(with_backoff_time - (no_backoff_time + 3.6 seconds)) < 0.5 seconds)
    ```
    The sum of 13 backoffs is roughly 3.6 seconds. There is a half-second window to account for potential variance
    between the two runs.

## Changelog

- 2206-07-08: Update Backoff test to use updated exponential formula.
- 2026-04-02: [DRIVERS-3436](https://github.com/mongodb/specifications/pull/1920) Refine withTransaction timeout error
    wrapping semantics and label propagation in spec and prose tests
- 2026-03-03: Clarify exponential backoff jitter upper bound.
- 2026-02-17: Clarify expected error when timeout is reached
    [DRIVERS-3391](https://jira.mongodb.org/browse/DRIVERS-3391).
- 2026-01-07: Fixed Retry Backoff is Enforced test accordingly to the updated spec.
- 2025-11-18: Added Backoff test.
- 2024-09-06: Migrated from reStructuredText to Markdown.
- 2024-02-08: Converted legacy tests to unified format.
- 2021-04-29: Remove text about write concern timeouts from prose test.
