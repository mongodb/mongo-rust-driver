# Transactions Tests

______________________________________________________________________

## Introduction

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
transactions. These tests utilize the [Unified Test Format](../../unified-test-format/unified-test-format.md).

Several prose tests, which are not easily expressed in YAML, are also presented in this file. Those tests will need to
be manually implemented by each driver.

## Mongos Pinning Prose Tests

The following tests ensure that a ClientSession is properly unpinned after a sharded transaction. Initialize these tests
with a MongoClient connected to multiple mongoses.

These tests use a cursor's address field to track which server an operation was run on. If this is not possible in your
driver, use command monitoring instead.

1. Test that starting a new transaction on a pinned ClientSession unpins the session and normal server selection is
    performed for the next operation.

    ```python
    @require_server_version(4, 1, 6)
    @require_mongos_count_at_least(2)
    def test_unpin_for_next_transaction(self):
      # Increase localThresholdMS and wait until both nodes are discovered
      # to avoid false positives.
      client = MongoClient(mongos_hosts, localThresholdMS=1000)
      wait_until(lambda: len(client.nodes) > 1)
      # Create the collection.
      client.test.test.insert_one({})
      with client.start_session() as s:
        # Session is pinned to Mongos.
        with s.start_transaction():
          client.test.test.insert_one({}, session=s)

        addresses = set()
        for _ in range(50):
          with s.start_transaction():
            cursor = client.test.test.find({}, session=s)
            assert next(cursor)
            addresses.add(cursor.address)

        assert len(addresses) > 1
    ```

2. Test non-transaction operations using a pinned ClientSession unpins the session and normal server selection is
    performed.

    ```python
    @require_server_version(4, 1, 6)
    @require_mongos_count_at_least(2)
    def test_unpin_for_non_transaction_operation(self):
      # Increase localThresholdMS and wait until both nodes are discovered
      # to avoid false positives.
      client = MongoClient(mongos_hosts, localThresholdMS=1000)
      wait_until(lambda: len(client.nodes) > 1)
      # Create the collection.
      client.test.test.insert_one({})
      with client.start_session() as s:
        # Session is pinned to Mongos.
        with s.start_transaction():
          client.test.test.insert_one({}, session=s)

        addresses = set()
        for _ in range(50):
          cursor = client.test.test.find({}, session=s)
          assert next(cursor)
          addresses.add(cursor.address)

        assert len(addresses) > 1
    ```

3. Test that `PoolClearedError` has `TransientTransactionError` label. Since there is no simple way to trigger
    `PoolClearedError`, this test should be implemented in a way that suites each driver the best.

## Options Inside Transaction Prose Tests.

These prose tests ensure drivers handle options inside a transaction where the unified tests do not suffice. Ensure
these tests do not run against a standalone server.

### 1.0 Write concern not inherited from collection object inside transaction.

- Create a MongoClient running against a configured sharded/replica set/load balanced cluster.
- Start a new session on the client.
- Start a transaction on the session.
- Instantiate a collection object in the driver with a default write concern of `{ w: 0 }`.
- Insert the document `{ n: 1 }` on the instantiated collection.
- Commit the transaction.
- End the session.
- Ensure the document was inserted and no error was thrown from the transaction.

## Changelog

- 2024-10-31: Add test for PoolClearedError.
- 2024-02-15: Migrated from reStructuredText to Markdown.
- 2024-02-07: Converted legacy transaction tests to unified format and moved the legacy test format docs to a separate
    file.
