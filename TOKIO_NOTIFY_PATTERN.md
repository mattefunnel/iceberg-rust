# `tokio::sync::Notify` lost-wakeup pattern

The fix in this branch (`82e9db97`) applies the documented `Notified::enable`
pattern from the tokio rustdoc to three observe-then-await sites in the
scan/delete coordination path.

## The canonical pattern

Source: <https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html>
(search for "mpmc" or `Notified::enable`).

The docs give this multi-consumer channel example:

```rust
pub async fn recv(&self) -> T {
    let future = self.notify_on_sent.notified();
    tokio::pin!(future);

    loop {
        // Make sure that no wakeup is lost if we get
        // `None` from `try_recv`.
        future.as_mut().enable();

        if let Some(msg) = self.try_recv() {
            return msg;
        }

        future.as_mut().await;
        future.set(self.notify_on_sent.notified());
    }
}
```

## Why `enable()` matters

`Notified::enable()` registers the waiter into `Notify`'s internal queue
*synchronously*, before the await point. Any subsequent `notify_waiters()` /
`notify_one()` then sees this waiter and wakes it.

Without `enable()`, registration only happens on the first `poll`, i.e. inside
`.await`. A `notify_waiters()` that fires in the gap between the "is it ready?"
check and the `.await` is then lost — `notify_waiters()` wakes only currently
registered waiters and stores no permit.

## How we adjusted it for a one-shot transition

The tokio example loops because an mpmc channel has repeated
`Some/None` transitions. Our sites are one-shot state machines
(`Populating → Populated`, `Loading → Loaded`), so the loop collapses to a
single recheck:

```rust
let notified = notifier.notified();
tokio::pin!(notified);
notified.as_mut().enable();          // register first

if let DeleteFileIndexState::Populated(ref index) = *self.state.read().unwrap() {
    return index.get_deletes_for_data_file(data_file, seq_num);  // try_recv equivalent
}

notified.await;                       // only reached if still Populating
```

Mapping back to the tokio example:

| tokio example                    | our one-shot adaptation                    |
| -------------------------------- | ------------------------------------------ |
| `self.notify_on_sent.notified()` | `notifier.notified()`                      |
| `future.as_mut().enable()`       | `notified.as_mut().enable()`               |
| `self.try_recv()` returning      | state read returning `Populated(..)`       |
| `future.as_mut().await`          | `notified.await`                           |
| loop + `future.set(...)`         | dropped — only one transition is possible  |

Same shape is applied at:

- `crates/iceberg/src/delete_file_index.rs` — `get_deletes_for_data_file`
- `crates/iceberg/src/arrow/delete_filter.rs` — `wait_for_pos_del_load`
- `crates/iceberg/src/arrow/delete_filter.rs` — `get_equality_delete_predicate_for_delete_file_path`
