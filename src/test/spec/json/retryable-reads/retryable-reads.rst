===============
Retryable Reads
===============

:Spec Title: Retryable Reads
:Spec Version: 1.1
:Author: Vincent Kam 
:Lead: Bernie Hackett
:Advisory Group: Shane Harvey, Scott L’Hommedieu, Jeremy Mikola
:Approvers: Jason Carey, Bernie Hackett, Shane Harvey, Eliot Horowitz, Scott L’Hommedieu, Jeremy Mikola, Dan Pasette, Jeff Yemin
:Status: Accepted
:Type: Standards
:Minimum Server Version: 3.6
:Last Modified: 2019-05-29
   
.. contents::

--------

Abstract
========

This specification is about the ability for drivers to automatically retry any
read operation that has not yet received any results—due to a transient network
error, a "not master" error after a replica set failover, etc.—exactly once.

This specification will

- outline how an API for retryable read operations will be implemented in
  drivers

- define an option to enable retryable reads for an application.

META
====

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in `RFC 2119 <https://www.ietf.org/rfc/rfc2119.txt>`_.  

Specification
=============

Terms
-----

Retryable Error
~~~~~~~~~~~~~~~

An error is considered retryable if it meets any of the criteria defined under
`Retryable Writes: Terms: Retryable Error
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#terms>`__,
minus the final criterion about write concern errors. For convenience, the
relevant criteria have been adapted to retryable reads and reproduced below.

An error is considered retryable if it meets any of the following criteria:

- any network exception (e.g. socket timeout or error)

- a server error response with any the following codes:

=============================== ==============
**Error Name**                  **Error Code**
=============================== ==============
InterruptedAtShutdown           11600
InterruptedDueToReplStateChange 11602
NotMaster                       10107
NotMasterNoSlaveOk              13435
NotMasterOrSecondary            13436
PrimarySteppedDown              189
ShutdownInProgress              91
HostNotFound                    7
HostUnreachable                 6
NetworkTimeout                  89
SocketException                 9001
=============================== ==============

- a server error response without an error code or one different from those
  listed above, but with an error message containing the substring "not
  master" or "node is recovering"

MongoClient Configuration 
--------------------------

This specification introduces the following client-level configuration option.

retryReads
~~~~~~~~~~

This boolean option determines whether retryable behavior will be applied to all
read operations executed within the MongoClient. This option MUST default to
true. `As with retryable writes
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#retrywrites>`__,
this option MUST NOT be configurable at the level of an individual read
operation, collection object, or database object. Drivers that expose a "high"
and "core" API (e.g. Java and C# driver) MUST NOT expose a configurable option
at the level of an individual read operation, collection object, or database
object in "high", but MAY expose the option in "core."

Naming Deviations 
^^^^^^^^^^^^^^^^^^

`As with retryable writes
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#retrywrites>`__,
drivers MUST use the defined name of ``retryReads`` for the connection string
parameter to ensure portability of connection strings across applications and
drivers. If drivers solicit MongoClient options through another mechanism
(e.g. an options dictionary provided to the MongoClient constructor), drivers
SHOULD use the defined name but MAY deviate to comply with their existing
conventions. For example, a driver may use ``retry_reads`` instead of
``retryReads``. For any other names in the spec, drivers SHOULD use the defined
name but MAY deviate to comply with their existing conventions.

Requirements for Retryable Reads
--------------------------------

Supported Server Versions
~~~~~~~~~~~~~~~~~~~~~~~~~

Drivers MUST verify server eligibility by ensuring that ``maxWireVersion`` is at
least 6 because retryable reads require a MongoDB 3.6 standalone, replica set or
shard cluster, MongoDB 3.6 server wire version is 6 as defined in the `Server
Wire version and Feature List specification
<https://github.com/mongodb/specifications/blob/master/source/wireversion-featurelist.rst>`__.

The minimum server version is 3.6 because

1. It gives us version parity with retryable writes.
2. It forces the retry attempt to use the same implicit session, which would
   make it it easier to track operations and kill any errant longer running
   operation.
3. It limits the scope of the implementation (``OP_QUERY`` will not need to be
   supported).

Supported Read Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

Drivers MUST support retryability for the following operations:

- All read operations defined in the CRUD specification i.e.
   - ``Collection.find()``
      - This includes the ``find`` operations backing the GridFS API.
   - ``Collection.aggregate()``
      - Only if the pipeline does not include a write stage (e.g. ``$out``, ``$merge``)
   - ``Collection.distinct()``
   - ``Collection.count()``
      - Only required if the driver already provides ``count()``
   - ``Collection.estimatedDocumentCount()``
   - ``Collection.countDocuments()``
- All read operation helpers in the change streams specification i.e.
   - ``Collection.watch()``
   - ``Database.watch()``
   - ``MongoClient.watch()``
- All enumeration commands e.g.
   - ``MongoClient.listDatabases()``
   - ``Database.listCollections()``
   - ``Collection.listIndexes()``
- Any read operations not defined in the aforementioned specifications:
   - Any read operation helpers e.g. ``Collection.findOne()``

Drivers SHOULD support retryability for the following operations:

- Any driver that provides generic command runners for read commands(with logic
  to inherit a client-level read concerns) SHOULD implement retryability for the
  read-only command runner.

Most of the above methods are defined in the following specifications:

- `Change Streams
  <https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst>`__

- `CRUD
  <https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst>`__

- `Enumerating Collections
  <https://github.com/mongodb/specifications/blob/master/source/enumerate-collections.rst>`__

- `Enumerating Indexes
  <https://github.com/mongodb/specifications/blob/master/source/enumerate-indexes.rst>`__

- `Enumerating Databases
  <https://github.com/mongodb/specifications/blob/master/source/enumerate-databases.rst>`__

- `GridFS Spec
  <https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst>`__

Unsupported Read Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Drivers MUST NOT retry the following operations:

- ``Collection.mapReduce()``
  
  - This is due to the "Early Failure on Socket Disconnect" feature not
    supporting ``mapReduce``.
    
  - N.B. If ``mapReduce`` is executed via a generic command runner for read
    commands, drivers SHOULD NOT inspect the command to prevent ``mapReduce``
    from retrying.

-  Cursor.getMore()

   - See `Why is retrying Cursor.getMore() not supported?
     <#why-is-retrying-cursor.getmore-not-supported>`__

-  The generic runCommand helper, even if it is passed a read command.

   - N.B.: This applies only to a generic command runner, which is agnostic
     about the read/write nature of the command.

Implementing Retryable Reads
----------------------------

Executing Retryable Read Commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Executing retryable read commands is extremely similar to `executing retryable
write commands
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#executing-retryable-write-commands>`__.
The following explanation for executing retryable read commands has been adapted
from the explanation for executing retryable write commands.

1. Selecting the initial server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The driver selects the initial server for the command as usual. When selecting a
server for the first attempt of a retryable read command, drivers MUST allow a
server selection error to propagate. In this case, the caller is able to infer
that no attempt was made.

2. Determining whether retry should be allowed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A driver then determines if it should attempt to retry next.

2a. When not to allow retry
'''''''''''''''''''''''''''

Drivers MUST attempt to execute the read command exactly once and allow any
errors to propagate under any of the the following conditions:

-  if retryable reads is not enabled **or**

-  if the selected server does not support retryable reads **or**

-  if the session in a transaction

By allowing the error to propagate, the caller is able to infer that one attempt
was made.

2b. When to allow retry
'''''''''''''''''''''''

Drivers MUST only attempt to retry a read command if

-  retryable reads are enabled **and**

-  the selected server supports retryable reads **and**

-  the first attempt yields a retryable error

3. Deciding to allow retry, encountering the initial retryable error, and selecting a server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the driver decides to allow retry and the first attempt of a retryable read
command encounters a retryable error, the driver MUST update its topology
according to the Server Discovery and Monitoring spec (see `SDAM: Error Handling
<https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#error-handling>`__)
and capture this original retryable error. Drivers should then proceed with
selecting a server for the retry attempt.

3a. Selecting the server for retry
''''''''''''''''''''''''''''''''''

If the driver cannot select a server for the retry attempt or the newly selected
server does not support retryable reads, retrying is not possible and drivers
MUST raise the original retryable error. In both cases, the caller is able to
infer that an attempt was made.

3b. Sending an equivalent command for the second attempt
''''''''''''''''''''''''''''''''''''''''''''''''''''''''

After server selection, a driver MUST send a valid command to the newly selected
server that is equivalent [1]_ to the initial command sent to the first
server. If the driver determines that the newly selected server may not be able
to support a command equivalent to the initial command, drivers MUST NOT retry
and MUST raise the original retryable error

The above requirement can be fulfilled in one of two ways:

1. During the retry attempt, the driver SHOULD recreate the command while
   adhering to that operation's specification's server/wire version
   requirements. If an error occurs while recreating the command, then the
   driver MUST raise the original retryable error.

   For example, if the wire version dips from *W*\ :sub:`0` to *W*\ :sub:`1`
   after server selection, and the spec for operation *O* notes that for wire
   version *W*\ :sub:`1`, that field *F* should be omitted, then field *F*
   should be omitted. If the spec for operation *O* requires the driver to error
   out if field *F* is defined when talking to a server with wire version *W*\
   :sub:`1`, then the driver must error out and raise the original retryable
   error.

2. Alternatively, if a driver chooses not to recreate the command as described
   above, then a driver MUST NOT retry if the server/wire version dips after
   server selection and MUST raise the original retryable error.

   For example, if the wire version dips after server selection, the driver can
   choose to not retry and simply raise the original retryable error because
   there is no guarantee that the lower versioned server can support the
   original command.

3c. If the retry attempt fails
''''''''''''''''''''''''''''''

If the retry attempt also fails, drivers MUST update their topology according to
the SDAM spec (see `SDAM: Error Handling
<https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#error-handling>`__).
If an error would not allow the caller to infer that an attempt was made
(e.g. connection pool exception originating from the driver), the original error
should be raised. If the retry failed due to another retryable error or some
other error originating from the server, that error should be raised instead as
the caller can infer that an attempt was made and the second error is likely
more relevant (with respect to the current topology state).

4. Implementation constraints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When retrying a read command, drivers MUST NOT resend the original wire protocol
message (see: `Can drivers resend the same wire protocol message on retry
attempts? <#_uctengj3f6jh>`__).

Pseudocode
~~~~~~~~~~

The following pseudocode for executing retryable read commands has been adapted
from `the pseudocode for executing retryable write commands
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#executing-retryable-write-commands>`__
and reflects the flow described above.

.. code:: typescript

  /**
   * Checks if a connection supports retryable reads.
   */
  function isRetryableReadsSupported(connection) {
    return connection.MaxWireVersion >= RETRYABLE_READS_MIN_WIRE_VERSION);
  }
  
  /**
   * Executes a read command in the context of a MongoClient where a retryable
   * read have been enabled. The session parameter may be an implicit or
   * explicit client session (depending on how the CRUD method was invoked).
   */
  function executeRetryableRead(command, session) {
    /* Allow ServerSelectionException to propagate to our caller, which
     * can then assume that no attempts were made. */
    server = selectServer();
    connection = server.getConnection()   
  
    /* If the server does not support retryable reads or if the session in a
     * transaction execute the read as if retryable reads are not enabled. */
    if ( !isRetryableReadsSupported(connection) || session.inTransaction()) {
      return executeCommand(connection, command);
    }
  
  
    /* NetworkException and NotMasterException are both retryable errors. If
     * caught, remember the exception, update SDAM accordingly, and proceed with
     * retrying the operation. */
    try {
      return executeCommand(server, command);
    } catch (NetworkException originalError) {
      updateTopologyDescriptionForNetworkError(server, originalError);
    } catch (NotMasterException originalError) {
      updateTopologyDescriptionForNotMasterError(server, originalError);
    }
  
    /* If we cannot select a server, do not proceed with retrying and
     * throw the original error. The caller can then infer that an attempt was
     * made and failed. */
    try {
      server = selectServer();
      connection = server.getConnection()
    } catch (Exception ignoredError) {
      throw originalError;
    }
  
    /* If the server selected for retrying is too old, throw the original error.
     * The caller can then infer that an attempt was made and failed. This case
     * is very rare, and likely means that the cluster is in the midst of a
     * downgrade. */
    if ( !isRetryableReadsSupported(connection)) {
      throw originalError;
    }
    try {
      secondCommand = createCommand(server);
    } catch (Exception ignoredError) {
      throw originalError;
    }
  
    /* Allow any retryable error from the second attempt to propagate to our
     * caller, as it will be just as relevant (if not more relevant) than the
     * original error. For exceptions that originate from the driver (e.g. no
     * socket available from the connection pool), we should raise the original
     * error. Other exceptions originating from the server should be allowed to
     * propagate. */
    try {
      return executeCommand(connection, secondCommand);
    } catch (NetworkException secondError) {
      updateTopologyDescriptionForNetworkError(server, secondError);
      throw secondError;
    } catch (NotMasterException secondError) {
      updateTopologyDescriptionForNotMasterError(server, secondError);
      throw secondError;
    } catch (DriverException ignoredError) {
      throw originalError;
    }
  }



Logging Retry Attempts
----------------------

`As with retryable writes
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#logging-retry-attempts>`__,
drivers MAY choose to log retry attempts for read operations. This specification
does not define a format for such log messages.

Command Monitoring
------------------

`As with retryable writes
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#command-monitoring>`__,
in accordance with the `Command Monitoring
<https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst>`__
specification, drivers MUST guarantee that each ``CommandStartedEvent`` has
either a correlating ``CommandSucceededEvent`` or ``CommandFailedEvent``. If the
first attempt of a retryable read operation encounters a retryable error,
drivers MUST fire a ``CommandFailedEvent`` for the retryable error and fire a
separate ``CommandStartedEvent`` when executing the subsequent retry
attempt. Note that the second ``CommandStartedEvent`` may have a different
``connectionId``, since a server is reselected for the retry attempt.

Documentation
-------------

1. Drivers MUST document all read operations that support retryable behavior.

2. Drivers MUST document that the operations in `Unsupported Read Operations
   <#unsupported-read-operations>`__ do not support retryable behavior.

3. Driver release notes MUST make it clear to users that they may need to adjust
   custom retry logic to prevent an application from inadvertently retrying for
   too long (see `Backwards Compatibility <#backwards-compatibility>`__ for
   details).

4. Drivers implementing retryability for their generic command runner for read
   commands MUST document that ``mapReduce`` will be retried if it is passed as a
   command to the command runner. These drivers also MUST document the potential
   for degraded performance given that "Early Failure on Socket Disconnect"
   feature does not support ``mapReduce``.

Test Plan
=========

See the `README
<https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.rst>`__
for tests.

At a high level, the test plan will cover executing supported read operations
within a MongoClient where retryable reads have been enabled, ensuring that
reads are retried exactly once.

Motivation for Change 
======================

Drivers currently have an API for the retryability of write operations but not
for read operations. The driver API needs to be extended to include support for
retryable behavior for read operations.

Design Rationale 
=================

The design of this specification is based off the `Retryable Writes
specification
<https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#design-rationale>`__.
It modifies the driver API as little as possible to introduce the concept
retryable behavior for read operations.

Alternative retry strategies (e.g. exponential back-off, incremental intervals,
regular intervals, immediate retry, randomization) were considered, but the
behavior of a single, immediate retry attempt was chosen in the interests of
simplicity as well as consistency with the design for retryable writes.

See the `future work`_ section for potential upcoming changes
to retry mechanics.

Backwards Compatibility 
========================

The API changes to support retryable reads extend the existing API but do not
introduce any backward breaking changes. Existing programs that do not make use
of retryable reads will continue to compile and run correctly.

N.B.: Applications with custom retry logic that choose to enable retryable reads
may need to redo their custom retry logic to ensure that the reads are retried
as desired. e.g. if an application has custom logic that retries reads n times
and enables retryable reads, then the application could end up retrying reads up
to 2n times.

The note above will also apply if an application upgrades to a version of the
driver where that defaults to enabling retryable reads.

Rejected Designs
----------------

1. To improve performance on servers without “Early Failure on Socket
   Disconnect”, we considered using ``killSessions`` to automatically kill the
   previous attempt before running a retry.  We decided against this because
   after killing the session, parts of it still may be running if there are any
   errors.  Additionally, killing sessions takes time because a kill has to talk
   to every non-config ``mongod`` in the cluster (i.e. all the primaries and
   secondaries of each shard). In addition, in order to protect the system
   against getting overloaded with these requests, every server allows no more
   than one killsession operation at a time.  Operations that attempt to
   ``killsessions`` while a killsession is running are batched together and run
   simultaneously after the current one finishes.


Reference Implementation 
=========================

The C# and Python drivers will provide the reference implementations. See
`CSHARP-2429 <https://jira.mongodb.org/browse/CSHARP-2429>`__ and `PYTHON-1674
<https://jira.mongodb.org/browse/PYTHON-1674>`__.

Security Implications
=====================

None.

Future work
===========

1. A later specification may allow operations (including read) to be retried any
   number of times during a singular timeout period.

2. Any future changes to the the applicable parts of `retryable writes
   specification
   <https://github.com/mongodb/specifications/blob/master/source/retryable-writes/>`__
   may also need to be reflected in the retryable reads specification, and vice
   versa.

3. We may revisit the decision not retry ``Cursor.getMore()`` (see `Q&A`_).

4. Once `DRIVERS-560`_ is resolved, tests will be added to allow testing
   Retryable Reads on MongoDB 3.6. See the `test plan
   <https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.rst>`__
   for additional information.

.. _DRIVERS-560: https://jira.mongodb.org/browse/DRIVERS-560

Q&A 
====

Why is retrying ``Cursor.getMore()`` not supported?
---------------------------------------------------

``Cursor.getMore()`` cannot be retried because of the inability for the client
to discern if the cursor was advanced. In other words, since the driver does not
know if the original ``getMore()`` succeeded or not, the driver cannot reliably
know if results might be inadvertently skipped.

For example, if a transient network error occurs as a driver requests the second
batch of results via a getMore() and the driver were to silently retry the
``getMore()``, it is possible that the server had actually received the initial
``getMore()``. In such a case, the server will advance the cursor once more and
return the third batch instead of the desired second batch.

Furthermore, even if the driver could detect such a scenario, it is impossible
to return previously iterated data from a cursor because the server currently
only allows forward iteration.

It is worth noting that the "Cursors survive primary stepdown" feature avoids
this issue in certain common circumstances, so that we may revisit this decision
to disallow trying ``getMore()`` in the future.

Why are read operations only retried once?
------------------------------------------

`Read operations are only retried once for the same reasons that writes are also
only retried
once. <https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#why-are-write-operations-only-retried-once>`__
For convenience's sake, that reasoning has been adapted for reads and reproduced
below:

The spec concerns itself with retrying read operations that encounter a
retryable error (i.e. no response due to network error or a response indicating
that the node is no longer a primary). A retryable error may be classified as
either a transient error (e.g. dropped connection, replica set failover) or
persistent outage. In the case of a transient error, the driver will mark the
server as "unknown" per the `SDAM
<https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst>`__
spec. A subsequent retry attempt will allow the driver to rediscover the primary
within the designated server selection timeout period (30 seconds by
default). If server selection times out during this retry attempt, we can
reasonably assume that there is a persistent outage. In the case of a persistent
outage, multiple retry attempts are fruitless and would waste time. See `How To
Write Resilient MongoDB Applications
<https://emptysqua.re/blog/how-to-write-resilient-mongodb-applications/>`__ for
additional discussion on this strategy.

Can drivers resend the same wire protocol message on retry attempts?
--------------------------------------------------------------------

No. `This is in contrast to the answer supplied in in the retryable writes
specification. <https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#can-drivers-resend-the-same-wire-protocol-message-on-retry-attempts>`__
However, when retryable writes were implemented, no driver actually chose to
resend the same wire protocol message. Today, if a driver attempted to resend
the same wire protocol message, this could violate `the rules for gossiping
$clusterTime
<https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#gossipping-the-cluster-time>`__:
specifically `the rule that a driver must send the highest seen $clusterTime
<https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#sending-the-highest-seen-cluster-time>`__.

Additionally, there would be a behavioral difference between a driver resending
the same wire protocol message and one that does not. For example, a driver
creates a new wire protocol message could exhibit the following characteristics:

1. The second attempt to send the read command could have a higher ``$clusterTime``.

2. If the initial attempt failed with a server error, then the session's
   ``operationTime`` would be advanced and the next read would include a larger
   ``readConcern.afterClusterTime``.

A driver resends the same wire protocol message would not exhibit the above
characteristics. Thus, in order to avoid this behavioral difference and not
violate the rules about gossiping ``$clusterTime``, drivers MUST not resend the same
wire protocol message.

Why isn't MongoDB 4.2 required?
-------------------------------

MongoDB 4.2 was initially considered as a requirement for retryable reads
because MongoDB 4.2 implements support for “Early Failure on Socket Disconnect,”
changing the the semantics of socket disconnect to prevent ops from doing work
that no client is interested in. This prevents applications from seeing degraded
performance when an expensive read is retried. Upon further discussion, we
decided that "Early Failure on Socket Disconnect" should not be required to
retry reads because the resilience benefit of retryable reads outweighs the
minor risk of degraded performance. Additionally, any customers experiencing
degraded performance can simply disable ``retryableReads``.


Changelog 
==========

2019-06-07: Mention $merge stage for aggregate alongside $out

2019-05-29: Renamed InterruptedDueToStepDown to InterruptedDueToReplStateChange

Endnotes
========
.. [1] The first and second commands will be identical unless variations in
       parameters exist between wire/server versions.
