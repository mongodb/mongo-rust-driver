# A Guided Tour of the Rust Driver Codebase

These are notes intended to accompany an informal walkthrough of key parts of the driver's code; they may be useful on their own but are not intended to be comprehensive or prescriptive.

## Constructing the Client

[src/client.rs](../src/client.rs)

Entry point of the API.  The first thing most users will interact with.

* It's just a wrapper around an `Arc`-wrapped internal struct so users can cheaply `clone` it for convenient storage, passing to spawned tasks, etc.
  * (actually a `TrackingArc`, which if a compile-time flag is turned on will track where clones are constructed for debugging)
* Notable internal bits:
  * `topology`: tracks the servers we're connected to and maintains a pool of connections for each server.
  * `options`: usually parsed from user-provided URI
* `Client` can be constructed from:
  * A URI string.  By far the most common.
  * An options object directly.  Power user tool.
  * A builder if the user needs to enable in-use encryption.
* Events!
  * Three different kinds:
    * `Command`: "we told the server to do something"
    * `CMAP`: "something happened with an open connection"
    * `SDAM`: "something happened with a monitored server"
    * plus logging (via `tracing`)!
* `pub fn database`: gateway to the rest of the public API
* `register_async_drop`: Rust doesn't have `async drop`, so we built our own.
* `select_server`: apply criteria to topology, get server (which has connection pool)

## Doing Stuff to Data

[src/db.rs](../src/db.rs)

Gotta go through `Database` to do just about anything.  Primarily users will be getting handles to `Collection`s but there are a bunch of bulk actions that can be done directly.

* Like `Client`, it's just an `Arc` around an inner struct so users can cheaply `clone` it and pass it around.
  * The inner struct is much lighter: a handle on the parent `Client`, a string name, and some options.
* `new` isn't public.  Have to get it from `Client`.
* Can get a `Collection` from it, but there aren't any data operations in here, leading to...

## Anatomy of an Action

[src/action/aggregate.rs](../src/action/aggregate.rs)

*Actions* are the leaves of the public API.  They allow for fluent minimal-boilerplate option setting with Rustic type safety and minimal overhead; the drawback is that the internals are a little gnarly.

A usage example:

```rust
let cursor = db
    .aggregate([doc!{ ... }])           // [1]
    .bypass_document_validation(true)   // [2]
    .session(s)                         // [3]
    .await?;                            // [4]
```

Breaking down what's happening here:

1. This constructs the transient `Aggregate` type.  Typically users will never deal with this type directly; it'll be constructed and consumed in the same method call chain.  The transient action types can be thought of as _pending_ actions; they contain a reference to the target of the call, the parameters, the options, and the session.
2. This sets an option in the contained options object via the `option_setters!` proc macro, which also generates helpful doc links.
3. This sets the pending action to use an explicit session.  Note that this can only be done if the action was using an implicit session; Rust lets us enforce at compile-time that you can't call `.session` twice :)  We track this at the type level because it changes the type of the returned `Cursor`.
4. The `action_impl` proc macro will generate an `IntoFuture` impl for `Aggregate`, so when `await` is called it will be converted into a call to `execute`.

With all that, the body of `execute` is pretty small - it constructs an `Aggregate` _operation_ and hands that to the client's `execute_cursor_operation`.  This pairing between action and operation is very common: _action_ is the public API and _operation_ is the command sent to the server.

## Observing an Operation

[src/operation/insert.rs](../src/operation/insert.rs)

Redirecting from aggregate to insert here; aggregate has the cursor machinery on top of operation.

An `Operation` is a command that can be run on a mongodb server, with the bundled knowledge of how to construct the `Command` from the parameters of the `Operation` and how to interpret the `RawCommandResponse` from the server.

The `Operation` trait is split into `Operation` (used for type constraints) and `OperationWithDefaults` (provides default impls and a blanket impl of `Operation`) to allow forwarding types to implement the base `Operation` without new methods silently introducing bugs.

Most `Operation` impls are straightforward: aggregate components into a buffer with minor conditional logic, deserialize the response.

## Examining an Executor

[src/client/executor.rs](../src/client/executor.rs)

This is very much where the sausage is made; it's also very rare for it to need changes.

* `execute_operation` ... throws away some output of `execute_operation_with_details`
* `execute_operation_with_details` does some pre-retry-loop validation, calls into `execute_operation_with_retry`
* `execute_operation_with_retry`
  * tracks transaction state
  * selects a server
  * checks out a connection from the pool of the selected server
  * `execute_operation_on_connection`
  * handles errors and retries
* `execute_operation_on_connection`
  * builds command from operation
  * lots of session-handling
  * sends wire-format `Message` from `Command`
  * does bookkeeping from response

  ## Future Fields

  This is far from comprehensive; most notably, this doesn't cover:
  * the internals of `Topology`
  * the `bson` crate and our integration with `serde`
  * the testing infrastructure
  