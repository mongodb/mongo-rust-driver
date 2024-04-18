=====================================
Convenient API for Transactions Tests
=====================================

.. contents::

----

Introduction
============

The YAML and JSON files in this directory are platform-independent tests
meant to exercise a driver's implementation of the Convenient API for
Transactions spec. These tests utilize the
`Unified Test Format <../../unified-test-format/unified-test-format.rst>`__.

Several prose tests, which are not easily expressed in YAML, are also presented
in this file. Those tests will need to be manually implemented by each driver.

Prose Tests
===========

Callback Raises a Custom Error
``````````````````````````````

Write a callback that raises a custom exception or error that does not include
either UnknownTransactionCommitResult or TransientTransactionError error labels.
Execute this callback using ``withTransaction`` and assert that the callback's
error bypasses any retry logic within ``withTransaction`` and is propagated to
the caller of ``withTransaction``.

Callback Returns a Value
````````````````````````

Write a callback that returns a custom value (e.g. boolean, string, object).
Execute this callback using ``withTransaction`` and assert that the callback's
return value is propagated to the caller of ``withTransaction``.

Retry Timeout is Enforced
`````````````````````````

Drivers should test that ``withTransaction`` enforces a non-configurable timeout
before retrying both commits and entire transactions. Specifically, three cases
should be checked:

 * If the callback raises an error with the TransientTransactionError label and
   the retry timeout has been exceeded, ``withTransaction`` should propagate the
   error to its caller.
 * If committing raises an error with the UnknownTransactionCommitResult label,
   and the retry timeout has been exceeded, ``withTransaction`` should
   propagate the error to its caller.
 * If committing raises an error with the TransientTransactionError label and
   the retry timeout has been exceeded, ``withTransaction`` should propagate the
   error to its caller. This case may occur if the commit was internally retried
   against a new primary after a failover and the second primary returned a
   NoSuchTransaction error response.

 If possible, drivers should implement these tests without requiring the test
 runner to block for the full duration of the retry timeout. This might be done
 by internally modifying the timeout value used by ``withTransaction`` with some
 private API or using a mock timer.

Changelog
=========

:2024-02-08: Converted legacy tests to unified format.

:2021-04-29: Remove text about write concern timeouts from prose test.
