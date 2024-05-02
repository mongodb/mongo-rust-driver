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

## Changelog

- 2024-02-15: Migrated from reStructuredText to Markdown.
- 2024-02-07: Converted legacy transaction tests to unified format and moved the\
  legacy test format docs to a separate
  file.
