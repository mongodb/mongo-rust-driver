# Migrating from 2.x to 3.0
3.0 introduces a wide variety of improvements that required backwards-incompatible API changes; in most cases these changes should require only minor updates in application code.

## Fluent API
Async methods that accepted options have been updated to allow the individual options to be given directly in line with the call to reduce required boilerplate.  A session can now also be set using a chained method rather than a distinct `_with_session` method.

In 2.x:
```rust
// Without options
let cursor = collection.find(doc! { "status": "A" }, None).await?;
// With options
let cursor = collection
    .find(
        doc! { "status": "A" },
        FindOptions::builder()
            .projection(doc! {
                "status": 0,
                "instock": 0,
            })
            .build(),
    )
    .await?;
// With options and a session
let cursor = collection
    .find_with_session(
        doc! { "status": "A" },
        FindOptions::builder()
            .projection(doc! {
                "status": 0,
                "instock": 0,
            })
            .build(),
        &mut session,
    )
    .await?;
```

In 3.0:
```rust
// Without options
let cursor = collection.find(doc! { "status": "A" }).await?;
// With options
let cursor = collection
    .find(doc! { "status": "A" })
    .projection(doc! {
        "status": 0,
        "instock": 0,
    })
    .await?;
// With options and a session
let cursor = collection
    .find(doc! { "status": "A" })
    .projection(doc! {
        "status": 0,
        "instock": 0,
    })
    .session(&mut session)
    .await?;
```

If an option needs to be conditionally set, the `optional` convenience method can be used:
```rust
async fn run_find(opt_projection: Option<Document>) -> Result<()> {
    let cursor = collection
        .find(doc! { "status": "A" })
        .optional(opt_projection, |f, p| f.projection(p))
        .await?;
    ...
}
```

For those cases where options still need to be constructed independently, the `with_options` method is provided:
```rust
let options = FindOptions::builder()
    .projection(doc! {
        "status": 0,
        "instock": 0,
    })
    .build();
let cursor = collection
    .find(doc! { "status": "A" })
    .with_options(options)
    .await?;
```

## Listening for Events
The 2.x event API required that applications provide an `Arc<dyn CmapEventHandler>` (or `CommandEventHandler` / `SdamEventHandler`).  This required a fair amount of boilerplate, especially for simpler handlers (e.g. logging), and did not provide any way for event handlers to perform async work.  To address both of those, 3.0 introduces the `EventHandler` type.

In 2.x:
```rust
struct CmapEventLogger;
impl CmapEventHandler for CmapEventLogger {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        println!("{:?}", event);
    }
    /// repeat for handle_pool_ready_event, etc.
}
let options = ClientOptions::builder()
    .cmap_event_handler(Arc::new(CmapEventLogger))
    .build();
```

In 3.0:
```rust
let options = ClientOptions::builder()
    .cmap_event_handler(EventHandler::callback(|ev| println!("{:?}", ev)))
    .build();
```

`EventHandler` can be constructed from a callback, async callback, or an async channel sender.  To ease migration, it can also be constructed from the now-deprecated 2.x `CmapEventHandler`/`CommandEventHandler`/`SdamEventHandler` types.

## Async Runtime
2.x supported both `tokio` (default) and `async-std` as async runtimes.  Maintaining this dual support was a significant ongoing development cost, and with both Rust mongodb driver usage and overall Rust ecosystem usage heavily favoring `tokio`, 3.0 only supports `tokio`.

## Future-proof Features
Starting in 3.0, if the Rust driver is compiled with `no-default-features` it will require the use of a `compat` feature; this provides the flexibility to make features optional in future versions of the driver.  Lack of this had prevented `rustls` and `dns-resolution` from becoming optional in 2.x; they are now optional in 3.0.

## ReadConcern / WriteConcern Helpers
The Rust driver provides convenience helpers for constructing commonly used read or write concerns (e.g. "majority").  In 2.x, these were an overlapping mix of constants and methods (`ReadConcern::MAJORITY` and `ReadConcern::majority()`).  In 3.0, these are only provided as methods.

## `Send + Sync` Constraints on `Collection`
In 2.x, whether a method on `Collection<T>` required `T: Send` or `T: Send + Sync` was ad-hoc and inconsistent.  Whether these constraints were required for compilation depended on details like whether the transitive call graph of the implementation held a value of `T` over an `await` point, which could easily change as development continued.  For consistency and to allow for future development, in 3.0 `Collection<T>` requires `T: Send + Sync`.

## Compression Option Behind Feature Flags
In 2.x, the `ClientOptions::compressor` field was always present, but with no compressor features selected could not be populated.  For simplicity and consistency with our other optional features, in 3.0 that field will not be present if no compressor features are enabled.

## Optional `ReadPreferenceOptions`
In 3.0, the `ReadPreferenceOptions` fields of `ReadPreference` arms are now `Option<ReadPreferenceOptions>`.

## `human_readable_serialization` Option Removed in favor of `HumanReadable`
In some uncommon cases, applications prefer user data types to be serialized in their human-readable form; the `CollectionOptions::human_readable_serialization` option enabled that.  In 3.x, this option has been removed; the `bson::HumanReadable` wrapper type can be used to achieve the same effect.

## Consistent Defaults for `TypedBuilder` Implementations
In 2.x, most but not all option struct builders would allow value conversion via `Into` and would use the `default()` value if not set;  this is now the behavior across all option struct builders.

## `comment_bson` is now `comment`
In 2.x, the `AggregateOptions`, `FindOptions`, and `FindOneOptions` structs had both `comment_bson` and legacy `comment` fields.  In 3.x, `comment_bson` has been renamed to `comment`, replacing the legacy field.

## `bson-*` features removed
The 2.x driver provided features like `bson-chrono-0_4` that did not add any additional driver functionality but would enable the corresponding feature of the `bson` dependency.  These have been removed from 3.x; if your project needs specific `bson` features, you should list it as a top-level dependency with those features enabled.