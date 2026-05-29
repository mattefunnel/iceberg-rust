# Orphaned `Notify` on the equality-delete coordination path

## Summary

`DeleteFilter`'s equality-delete loading uses **two distinct `Notify`
instances** for a single "load this file" transaction: one created by
`try_start_eq_del_load`, another created by `insert_equality_delete`. The
loader task only calls `notify_waiters()` on the second one. A concurrent
reader can subscribe to the first (orphaned) notifier and park forever, even
after the load has completed and `notify_waiters` has run on the second
notifier.

This is **independent of** the lost-wakeup race fixed in
`NOTIFY_LOST_WAKEUP.md` / commit `82e9db97`. That fix corrects the
`observe → drop lock → subscribe` window in the consumer; the bug described
here is a producer-side defect that leaves a stale doorbell visible to
consumers regardless of how carefully they subscribe.

The bug exists on `main` today (verified against `main`'s
`crates/iceberg/src/arrow/delete_filter.rs`) and is therefore **not**
introduced by the lost-wakeup branch. It survives the lost-wakeup fix because
no amount of consumer-side care can rescue a subscription to a notifier the
producer has discarded.

## Plain-language explanation

Using the doorbell analogy from `NOTIFY_LOST_WAKEUP.md`:

The producer hangs **two doorbells** at the same address.

1. Bell A is hung up by `try_start_eq_del_load` and labeled "loading"; the
   state map now points to Bell A. The caller is handed Bell A but ignores
   it.
2. Bell B is hung up moments later by `insert_equality_delete`, replacing
   the entry. The state map now points to Bell B. Bell A is still hanging
   in the air but no one will ever ring it.
3. The spawned loader task waits for the predicate, writes `Loaded`, and
   rings Bell B.

A second listener who walked up to the address while Bell A was the one in
the map — even just for the duration of one read lock — has Bell A in their
hand. They put their ear to Bell A and wait. Bell B rings, but they are not
listening to Bell B, and Bell A never rings again.

## Affected code

All in `crates/iceberg/src/`:

- `arrow/delete_filter.rs::DeleteFilter::try_start_eq_del_load`
- `arrow/delete_filter.rs::DeleteFilter::insert_equality_delete`
- `arrow/delete_filter.rs::DeleteFilter::get_equality_delete_predicate_for_delete_file_path`
  (the consumer that subscribes to whatever notifier happens to be in state)
- `arrow/caching_delete_file_loader.rs::CachingDeleteFileLoader::load_file_for_task`
  (the producer that calls both `try_start_eq_del_load` and
  `insert_equality_delete` sequentially)

## The race

### Producer side

`CachingDeleteFileLoader::load_file_for_task`, equality-delete branch
(`crates/iceberg/src/arrow/caching_delete_file_loader.rs`, lines 261–286 at
the time of writing):

```rust
DataContentType::EqualityDeletes => {
    let Some(notify) = del_filter.try_start_eq_del_load(&task.file_path) else {
        return Ok(DeleteFileContext::ExistingEqDel);
    };
    // ^ `notify` is `notifier_A`, returned but never used after this point.
    //   This unused binding is the visible symptom of the bug.

    let (sender, receiver) = channel();
    del_filter.insert_equality_delete(&task.file_path, receiver);
    // ^ overwrites the state entry with `Loading(notifier_B)`, then spawns
    //   a task that, on completion, sets `Loaded(predicate)` and calls
    //   `notifier_B.notify_waiters()`. `notifier_A` is never notified.

    // ... build evolved_stream ...
    Ok(DeleteFileContext::FreshEqDel { batch_stream, sender, equality_ids })
}
```

`DeleteFilter::try_start_eq_del_load` writes `Loading(notifier_A)` under a
write lock, drops the lock, and returns `notifier_A`:

```rust
pub(crate) fn try_start_eq_del_load(&self, file_path: &str) -> Option<Arc<Notify>> {
    let mut state = self.state.write().unwrap();
    if state.equality_deletes.contains_key(file_path) {
        return None;
    }
    let notifier = Arc::new(Notify::new());
    state.equality_deletes.insert(
        file_path.to_string(),
        EqDelState::Loading(notifier.clone()),
    );
    Some(notifier)
}
```

`DeleteFilter::insert_equality_delete` creates a **fresh** `Notify`,
overwrites the state entry, and spawns the completion task that notifies
only that fresh notifier:

```rust
pub(crate) fn insert_equality_delete(
    &self,
    delete_file_path: &str,
    eq_del: Receiver<Predicate>,
) {
    let notify = Arc::new(Notify::new()); // notifier_B — a new doorbell
    {
        let mut state = self.state.write().unwrap();
        state.equality_deletes.insert(
            delete_file_path.to_string(),
            EqDelState::Loading(notify.clone()),
        );
    }

    let state = self.state.clone();
    let delete_file_path = delete_file_path.to_string();
    self.runtime.cpu().spawn(async move {
        let eq_del = eq_del.await.unwrap();
        {
            let mut state = state.write().unwrap();
            state.equality_deletes.insert(delete_file_path, EqDelState::Loaded(eq_del));
        }
        notify.notify_waiters(); // only notifier_B is ever rung
    });
}
```

There is no `.await` between `try_start_eq_del_load` returning and
`insert_equality_delete` running, but the two functions release and reacquire
the write lock independently, so the state momentarily holds
`Loading(notifier_A)` and any reader that grabs the read lock in that window
observes notifier_A.

### Consumer side

`DeleteFilter::get_equality_delete_predicate_for_delete_file_path` (already
patched on the lost-wakeup branch to use `enable()`-then-recheck) clones
whichever notifier is currently in state and subscribes to it:

```rust
let notifier = {
    match self.state.read().unwrap().equality_deletes.get(file_path) {
        None => return None,
        Some(EqDelState::Loading(notifier)) => notifier.clone(), // could be A or B
        Some(EqDelState::Loaded(predicate)) => return Some(predicate.clone()),
    }
};

let notified = notifier.notified();
tokio::pin!(notified);
notified.as_mut().enable();

if let Some(EqDelState::Loaded(predicate)) =
    self.state.read().unwrap().equality_deletes.get(file_path)
{
    return Some(predicate.clone());
}

notified.await; // <-- if `notifier` is `notifier_A`, this never wakes
```

The lost-wakeup fix saves us **only when** the cloned notifier is the same
notifier the producer eventually rings. Here, the producer may have
discarded the cloned notifier already.

### Interleaving that hangs

Let `T0..T5` be ordered moments in real time:

- `T0`: producer enters `try_start_eq_del_load`, acquires write lock,
  inserts `Loading(notifier_A)`, releases write lock.
- `T1`: consumer acquires read lock, observes `Loading(notifier_A)`,
  releases read lock.
- `T2`: producer enters `insert_equality_delete`, acquires write lock,
  inserts `Loading(notifier_B)` (overwriting), releases write lock; spawns
  loader task with `notifier_B`.
- `T3`: consumer creates `notified` against `notifier_A`,
  pins, calls `enable()`. Consumer is now subscribed to notifier_A.
- `T4`: consumer re-reads state under read lock and observes
  `Loading(notifier_B)` (still loading, just a different notifier) — so it
  proceeds to `notified.await` on notifier_A.
- `T5`: loader task finishes, writes `Loaded(predicate)`, calls
  `notifier_B.notify_waiters()`. Consumer is not subscribed to notifier_B.
  Consumer hangs forever.

`T1` and `T2` only need to be ordered as written; the consumer's later
operations can happen at any point after `T1` as long as `T3` is the moment
its `enable()` returns. The lost-wakeup fix's recheck at `T4` does not save
us, because the state is still `Loading` (just `Loading(notifier_B)`), and
the consumer has already committed to waiting on notifier_A.

The race window between `T0` and `T2` is small — no `.await` separates them
on the producer task — but it is not zero, and any read-lock acquisition
that lands inside it is enough.

## Why the lost-wakeup fix does not close this

The lost-wakeup fix guarantees that **if** the consumer subscribes to a
notifier the producer is going to ring, **and** the producer's
state-transition is sequenced before the producer's `notify_waiters` call,
then the consumer either re-observes the new state before awaiting, or is
awoken by `notify_waiters`.

It does not, and cannot, guarantee that the notifier the consumer subscribes
to is the same notifier the producer rings. That equality is a producer-side
invariant: the producer must commit, before any reader can observe the
`Loading` entry, to which `Notify` instance will eventually be the wake
target.

The equality-delete path violates that invariant by splitting the producer
across two write-lock sections, each of which installs its own notifier.

## Symptoms

The runtime signature is the same as the lost-wakeup hang documented in
`NOTIFY_LOST_WAKEUP.md`:

- the offending task is parked in `tokio::sync::Notify`'s wait
- every tokio worker is in `park_condvar` → `_pthread_cond_wait`
- the process makes 0% CPU forward progress

A `lldb` or `sample` capture cannot distinguish this hang from a
lost-wakeup hang from the stack alone; the underlying primitive and parking
shape are identical. Discrimination requires reading `DeleteFilter::state`
for the affected `file_path` and comparing the `Arc<Notify>` pointer in
`EqDelState::Loading(_)` against the `Arc<Notify>` the awaiting future
holds. If the two pointers differ, this is the orphaned-notifier bug; if
they are equal, it is the lost-wakeup race (or some other defect).

Practically: any post-lost-wakeup-fix hang on the equality-delete path
should be treated as this bug until proven otherwise.

## Reproducer

No deterministic reproducer is checked in. To exercise the race:

1. Spawn N concurrent `CachingDeleteFileLoader::load_deletes` calls against
   the same `FileScanTask` whose `deletes` list contains at least one
   equality delete file. The same `file_path` must be visible to both the
   producer (the loader winning `try_start_eq_del_load`) and a concurrent
   consumer (anyone calling
   `get_equality_delete_predicate_for_delete_file_path` for that path,
   typically via `build_equality_delete_predicate` during scan execution).
2. Run on a multi-threaded tokio runtime (≥ 4 worker threads).
3. Repeat to amplify the small `T0..T2` window.

The fuzz harness in `iceberg-rust-compactor::fuzz_parquet_file_rewriter`
already drives concurrent scans against delete-heavy tables; once the
lost-wakeup branch is applied, any residual hang from that harness on
delete-heavy tables is a candidate instance of this bug.

A deterministic in-tree test would require a test-only scheduler hook to
park the producer between `try_start_eq_del_load` and
`insert_equality_delete` while a consumer reads state.

## Fix directions

Three options, in order of minimality.

### Option 1 (recommended): reuse the existing notifier in `insert_equality_delete`

Change `insert_equality_delete` to look up the current entry; if it is
`Loading(notifier_existing)`, keep that notifier and reuse it as the wake
target rather than creating a fresh one.

```rust
pub(crate) fn insert_equality_delete(
    &self,
    delete_file_path: &str,
    eq_del: Receiver<Predicate>,
) {
    let notify = {
        let state = self.state.read().unwrap();
        match state.equality_deletes.get(delete_file_path) {
            Some(EqDelState::Loading(n)) => n.clone(),
            // Either no prior `try_start_eq_del_load` call or the entry
            // is already `Loaded` — both are programming errors at the
            // call sites we have today; assert or insert a fresh notifier
            // as appropriate.
            _ => unreachable!(
                "insert_equality_delete called without a prior try_start_eq_del_load"
            ),
        }
    };

    let state = self.state.clone();
    let delete_file_path = delete_file_path.to_string();
    self.runtime.cpu().spawn(async move {
        let eq_del = eq_del.await.unwrap();
        {
            let mut state = state.write().unwrap();
            state
                .equality_deletes
                .insert(delete_file_path, EqDelState::Loaded(eq_del));
        }
        notify.notify_waiters();
    });
}
```

The producer state-machine invariant becomes: **the notifier installed by
`try_start_eq_del_load` is the one that will ever be rung for this entry.**
Once that invariant holds, the lost-wakeup fix in
`get_equality_delete_predicate_for_delete_file_path` is sufficient.

This is the smallest change; it does not touch the consumer or the caller
in `caching_delete_file_loader.rs`.

### Option 2: merge the two producer steps into one

Combine `try_start_eq_del_load` and `insert_equality_delete` into a single
method, taking the `Receiver<Predicate>` as a parameter and performing both
the "mark loading" state insertion and the spawn under one write-lock
section. The caller would no longer have a dead `notify` binding.

This is a slightly larger refactor and changes the public API surface of
`DeleteFilter` (one fewer method), but it eliminates the two-step shape
that the bug depends on entirely.

### Option 3: switch primitive to `tokio::sync::watch` or stored-permit channel

A `watch::Sender<EqDelState>` per entry, or a `oneshot::Receiver<Predicate>`
shared via `Shared`, would store the completion event and eliminate the
producer-side "ring once, lose listeners" hazard altogether. This is the
direction `NOTIFY_LOST_WAKEUP.md` already flagged as preferable in the long
term and is a larger change than either option above.

## Why not just fix this in the lost-wakeup PR

Three reasons to keep them separate:

1. The two bugs are independent: the lost-wakeup race is a consumer-side
   subscribe-after-notify window; this is a producer-side multi-notifier
   defect. They have different reproducers, different root causes, and
   different fixes.
2. The lost-wakeup PR has a clean correctness argument anchored on
   `Notify::enable()` semantics. Bundling a producer-state-machine fix
   would muddy the review story.
3. The fix surface here is small (Option 1 is ~10 lines) and benefits
   from its own test once a deterministic scheduler hook is available.

## Test plan

Recommended once the producer-side fix is in place:

- Unit test that calls `try_start_eq_del_load` then asserts
  `insert_equality_delete` reuses the same `Arc<Notify>` pointer (compare
  `Arc::as_ptr`).
- Multi-threaded stress test mirroring the lost-wakeup regression test in
  `delete_file_index.rs`: spawn N readers calling
  `get_equality_delete_predicate_for_delete_file_path` against a path that
  one producer task is loading; assert all readers either get the
  predicate or `None`, none time out.
- If a test-only scheduler hook is added, a deterministic interleaving
  that parks the producer between the two write-lock sections and lets a
  reader observe the orphaned notifier first.
