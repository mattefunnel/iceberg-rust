# Lost-wakeup race in scan/delete coordination

## Summary

`iceberg 0.9.0` has a lost-wakeup race in three `tokio::sync::Notify`-based
coordination points in scan/delete loading. Under concurrent scans against
delete-heavy tables, a reader task can observe state as `Loading` /
`Populating`, drop the lock, and then call `notified().await`. If the
publisher transitions state and calls `notify_waiters()` in the gap between
the lock release and the subscription, the wakeup is lost — `notify_waiters`
only wakes already-subscribed tasks and stores no permit — and the reader
hangs forever.

## Plain-language explanation

For readers unfamiliar with Rust or tokio, here is the gist.

### The doorbell analogy

`tokio::sync::Notify` is the synchronisation primitive at the heart of this
bug. Picture it as a doorbell:

- **Waiters** ring up and listen for a ding.
- A **publisher** can call `notify_waiters()`, which dings the bell *once*
  for everyone currently listening.
- Crucially, if `notify_waiters()` runs and nobody is listening yet, the
  ding is lost forever. The bell stores no note; there is no "I missed it,
  ring again" semantics. (A different method, `notify_one`, *does* store
  one permit, but the publishers here use `notify_waiters` because there
  may be many concurrent readers.)

### Why the old code was wrong

Each affected site followed this shape:

1. Read shared state under a lock. See "still loading". Clone the doorbell.
2. *Drop the lock.*
3. Subscribe to the doorbell and sleep until it dings.

Between step 2 and step 3, on a multi-core machine the publisher can finish,
write the new state, and ring the bell. By the time the consumer reaches
step 3 the ding has already happened — and `notify_waiters` did not store
it. The consumer subscribes to a bell that will never ring again and parks
forever.

### Why the fix works

The new code uses the standard `Notify` race-free dance:

1. Read state. If already done, return immediately.
2. Create a subscription via `notifier.notified()` — a *lazy* listener that
   has not yet armed itself.
3. **Arm** it with `enable()`. From this moment on, any future
   `notify_waiters()` call is guaranteed to wake this listener.
4. **Re-check** the state. If the publisher finished between steps 1 and 3,
   we observe the new state here and return without sleeping.
5. Otherwise, await — guaranteed to wake.

Every interleaving is now safe (see the *Correctness argument* below).

### Per-file changes

- **`Cargo.toml`** — add `tokio` as a dev-dependency with `macros`,
  `rt-multi-thread`, and `time` features so the new test can use
  `#[tokio::test]`, run on a multi-threaded runtime, and call
  `tokio::time::timeout`.
- **`arrow/caching_delete_file_loader.rs`** — the caller no longer awaits
  the raw notifier directly; it delegates to a new
  `DeleteFilter::wait_for_pos_del_load` helper that performs the
  subscribe-then-recheck dance under the right lock.
- **`arrow/delete_filter.rs`** —
  - `PosDelLoadAction::WaitFor` no longer carries the `Arc<Notify>`; the
    new helper looks the notifier up itself, so the raw doorbell never
    leaves the type that owns the state.
  - New `wait_for_pos_del_load` method implements the race-free pattern.
  - `wait_for_eq_del_load` is patched in place with the same pattern.
- **`delete_file_index.rs`** — `get_deletes_for_data_file` is patched in
  place with the same pattern, and a new multi-threaded stress test
  `get_deletes_for_data_file_wait_path` exercises the wait path with 64
  concurrent readers across 50 iterations to amplify the (now-impossible)
  race.

### tl;dr

Three callers of `tokio::sync::Notify::notified().await` could lose wakeups
when the producer raced between their state observation and their
subscription. Each was rewritten to **subscribe (`enable()`) first, then
re-check state, then await**. A new helper `wait_for_pos_del_load`
encapsulates the pattern for positional deletes so its caller no longer
touches the raw notifier, and a multi-threaded stress test guards the
`DeleteFileIndex` path.

## Symptoms

The hang shape is consistent and easy to recognise:

- the test or scan thread is parked in `Runtime::block_on` /
  `CachedParkThread::block_on` → `Inner::park` → `_pthread_cond_wait`
- every Tokio worker thread is parked in
  `multi_thread::park::Inner::park_condvar` → `_pthread_cond_wait`
- the process consumes 0% CPU, no forward progress is possible

Sampled stack from a hung scan (extract):

```
test thread:
  tokio::runtime::runtime::Runtime::block_on
    tokio::runtime::park::CachedParkThread::block_on
      tokio::runtime::park::Inner::park
        _pthread_cond_wait
          __psynch_cvwait

each tokio-rt-worker (×N):
  tokio::runtime::scheduler::multi_thread::worker::run
    tokio::runtime::scheduler::multi_thread::worker::Context::park_internal
      tokio::runtime::scheduler::multi_thread::park::Inner::park_condvar
        _pthread_cond_wait
          __psynch_cvwait
```

## Attaching a debugger to a hung process

The hang is non-progressing — every thread is parked in a kernel wait —
so a debugger or sampler is enough to confirm the signature; no need for
a deterministic reproducer.

### lldb (macOS / Linux with rust-lldb)

```sh
lldb -p <pid> -batch -o 'thread list' -o 'thread backtrace all' \
                      -o 'detach' -o 'quit'
```

`lldb -p` works on a user-owned process without `sudo` on macOS (and
without any `developer mode` toggle on a default-configured machine).

Verified output from a real hang reproduced via the
`iceberg-rust-compactor` `fuzz_parquet_file_rewriter` harness against
unpatched `iceberg 0.9.0` at `PROPTEST_CASES=2048`:

#### `thread list`

```
* thread #1:  ... semaphore_wait_trap ..., name = 'main'
  thread #2:  ... __psynch_cvwait ...,    name = 'rewrite_preserves_row_multiset'
  thread #3:  ... kevent ...,             name = 'tokio-rt-worker'
  thread #4–#16: ... __psynch_cvwait ..., name = 'tokio-rt-worker'
```

Three distinct shapes — confirm all three to rule out a non-race hang.

#### Test harness thread (`#1 main`) — waiting on the property-test thread

```
frame #0: libsystem_kernel.dylib`semaphore_wait_trap + 8
frame #1: libdispatch.dylib`_dispatch_sema4_wait + 28
frame #2: libdispatch.dylib`_dispatch_semaphore_wait_slow + 132
frame #3: ...`std::thread::Thread::park
frame #4: ...`std::sync::mpmc::list::Channel<T>::recv
frame #5: ...`test::console::run_tests_console
frame #6: ...`test::test_main
frame #7: ...`test::test_main_static
frame #8: ...`std::sys::backtrace::__rust_begin_short_backtrace
frame #9: ...`std::rt::lang_start::{closure}
frame #10: ...`std::rt::lang_start_internal
frame #11: ...`main
```

Cargo's libtest harness is parked in `mpmc::Channel::recv` waiting for
the property-test thread to publish a result. **This is the harness
itself, not the bug.** It parks here in any healthy test run too — it
matters only that we know to look elsewhere for the cause.

#### Property-test thread (`#2 rewrite_preserves_row_multiset`) — the smoking gun

```
frame #0: libsystem_kernel.dylib`__psynch_cvwait + 8
frame #1: libsystem_pthread.dylib`_pthread_cond_wait + 980
frame #2: ...`tokio::runtime::park::Inner::park + 184
frame #3: ...`tokio::runtime::park::CachedParkThread::block_on + 6380
frame #4: ...`tokio::runtime::runtime::Runtime::block_on + 564
frame #5: ...`proptest::test_runner::scoped_panic_hook::internal::with_hook
frame #6: ...`proptest::test_runner::runner::call_test
frame #7: ...`proptest::test_runner::runner::TestRunner::gen_and_run_case
frame #8: ...`proptest::test_runner::runner::TestRunner::run_in_process
... (libtest scaffolding) ...
```

`Runtime::block_on` → `CachedParkThread::block_on` → `Inner::park` is
the runtime-level wait. The `block_on`-driven future (the rewrite
property body) has yielded control because some inner future is `Pending`
on a notifier that will never fire. The thread is named after the
`proptest!` test (`rewrite_preserves_row_multiset`) which makes it easy
to identify in any libtest output.

#### Tokio I/O driver (`#3`) — kqueue/epoll

```
frame #0: libsystem_kernel.dylib`kevent + 8
frame #1: ...`mio::poll::Poll::poll
frame #2: ...`tokio::runtime::io::driver::Driver::turn
frame #3: ...`tokio::runtime::time::Driver::park_internal
frame #4: ...`tokio::runtime::scheduler::multi_thread::worker::Context::park_internal
... (worker scaffolding) ...
```

Parked in `kevent` (macOS) / `epoll_wait` (Linux) waiting for I/O readiness.
Healthy in a quiescent runtime — but in a running test it should be
intermittently woken; sustained parking here means there is no I/O work
to drive *and* no inter-thread wakeup pending (which is exactly what
we see).

#### Worker threads (`#4–#16`) — all parked in the condvar

```
frame #0: libsystem_kernel.dylib`__psynch_cvwait + 8
frame #1: libsystem_pthread.dylib`_pthread_cond_wait + 980
frame #2: ...`tokio::runtime::scheduler::multi_thread::park::Inner::park_condvar + 280
frame #3: ...`tokio::runtime::scheduler::multi_thread::worker::Context::park_internal + 376
frame #4: ...`tokio::runtime::context::scoped::Scoped::set
frame #5: ...`tokio::runtime::context::runtime::enter_runtime
frame #6: ...`tokio::runtime::scheduler::multi_thread::worker::run
... (blocking pool / task::harness::poll scaffolding) ...
```

**Every** non-I/O `tokio-rt-worker` is parked in
`multi_thread::park::Inner::park_condvar` → `_pthread_cond_wait` →
`__psynch_cvwait`, with byte-identical stack offsets. No worker is in
user code. The runtime has no runnable task to dispatch.

This is the unambiguous lost-wakeup signature: a future is `Pending` on a
`Notify` that has already had its only `notify_waiters()` call complete.

### macOS without `sudo`: `sample`

`/usr/bin/sample` is the right tool when you want call-graph summary
rather than per-thread frame chains, and it works on user-owned processes
without privileges:

```sh
/usr/bin/sample <pid> 2 -mayDie    # 2-second sample
```

In a real hang the call-graph aggregates with identical hit counts at
every depth (e.g. `1765 1765 1765 ...` from root to leaf): every sample
caught the process in the same place.

### Linux: gdb / rust-gdb

```sh
gdb -p <pid> -batch \
  -ex 'thread apply all bt' -ex 'detach' -ex 'quit'
```

Same shape, different leaf: `__psynch_cvwait` → `pthread_cond_wait`
becomes `futex_wait`, and `kevent` becomes `epoll_wait`. The
`tokio::runtime::scheduler::multi_thread::park::Inner::park_condvar`
frame is the invariant identifier of "tokio worker parked".

### Build-flag note: release vs. debug symbols

The lldb output above is from a **release** build (`cargo test --release`).
There are no source-line annotations and no inlined-frame expansion, but
all the relevant Rust frames demangle cleanly from `_ZN5tokio...` /
`_ZN8proptest...` symbols, so the diagnosis works without rebuilding.

If you want line numbers in `frame info` output, add to the workspace
`Cargo.toml`:

```toml
[profile.release]
debug = "line-tables-only"
```

This roughly doubles binary size but adds no codegen difference, and
turns frame addresses into `file.rs:NNN` references.

### What rules out other causes

- **Tokio runtime drop / lifecycle bug:** ruled out by switching the
  fuzz harness from per-case `Runtime::new()` to a shared runtime. The
  failure threshold moved upward but the hang persisted with the same
  stack signature, so it is not `Runtime::drop` blocking on a live task.
- **Deadlock on `RwLock`:** every parked frame is in `pthread_cond_wait`
  via Tokio's parker, never `pthread_rwlock_*`. No thread is holding a
  lock; the `RwLock` inside `DeleteFileIndex` / `DeleteFilter` is
  unowned at the time of the hang. (We did not need to confirm this via
  `frame variable` because the absence of `pthread_rwlock_*` frames is
  already conclusive.)
- **Stalled I/O:** the I/O driver thread parks in `kevent` /
  `epoll_wait` because the runtime has no future asking it to drive
  anything. No syscall is outstanding. If it were a stuck read or a
  hung connection the stack would terminate in `read` / `recvfrom` /
  similar, not `kevent`.

## Reproducer

The race is timing-sensitive but reliably reproducible at scale. We hit it
in seconds via a property-test harness that drives `Table::scan()` against
delete-heavy tables on a multi-core runtime
(`iceberg-rust-compactor::fuzz_parquet_file_rewriter`). It does not require
a soak — once enough concurrent readers cross delete-loading paths, one of
them lands in the gap.

The per-reader window between observing the in-progress state and subscribing
is small — no `.await` sits inside it — so a *single* reader rarely straddles
the publisher's one-shot `notify_waiters`. But the bug is **fan-out driven,
not needle-threading**: `notify_waiters` fires exactly once and stores no
permit, so *every* reader that observes the loading state and finishes
subscribing after that single notify is lost forever. Under broad deletes —
one small delete file referenced by many data files — a whole cohort of
readers piles onto the same notifier, and many are still pre-subscribe when
the fast load completes and notifies. This is why the
`fuzz_parquet_file_rewriter` harness hits it constantly, and why throttling
fuzz fan-out reduces CI hangs.

### Which site reproduces in isolation

The three sites are **not** equally easy to hit, which matters a lot when
building a minimal reproducer:

- **`DeleteFileIndex::get_deletes_for_data_file` (during planning)** is the
  *hardest*. `plan_files` never reads delete-file content — the delete stream
  just turns manifest entries into `DeleteFileContext`s — so the `Populating`
  phase is near-instant and readers almost never actually reach the wait. A
  black-box stress harness (up to 20k rounds × 2048 readers, across both the
  real `plan_files` scan and a synthetic driver, on a 100-worker-thread
  runtime) did **not** reproduce it in ~20M reader calls.
- **`CachingDeleteFileLoader` positional `WaitFor` path** is the *easiest*, and
  is what the rewriter actually exercises. The first task to request a
  positional-delete file wins `Load` and reads the (small) parquet; every
  other task sharing the loader gets `WaitFor`. The waiters are serialized
  through the write-locked `try_start_pos_del_load`, so while they trickle in,
  the fast load finishes and `finish_pos_del_load` calls `notify_waiters` —
  before many of them have subscribed.

### Kept reproduction

`arrow::caching_delete_file_loader::tests::broad_positional_delete_reproduces_lost_wakeup`
drives the real `load_deletes` path with a broad positional delete: one tiny
delete file, 2048 data-file tasks sharing one `CachingDeleteFileLoader`, on a
100-worker-thread runtime, repeated for 200 rounds with a 10 s per-round
timeout. The `load_deletes` API is identical on `main` and on the fix (only
the internal wait was patched), so the same test source covers both:

- against the **pre-fix** `wait_for_pos_del_load`
  (`notifier.notified().await` straight after observing `Loading`): a waiter
  subscribes after the notify and parks forever; its `load_deletes` future
  never resolves and the round times out. Verified **red at round 15 (~10 s)**.
- against the **subscribe-before-recheck fix**: every waiter wakes; 200 rounds
  pass in **~1.1 s**.

Oversubscribing worker threads (100 on far fewer cores) widens the
read→subscribe gap via OS preemption, raising the hit rate.

### Manual stress verification

> **Caveat (added later).** The numbers below come from an early throwaway
> harness that "released 2048 readers at the wait path" — i.e. it poised the
> readers at the subscribe point with explicit synchronisation before dropping
> the sender. A *pure* black-box `DeleteFileIndex` stress loop (no such
> poising) does **not** reproduce in isolation on a fast machine — see "Which
> site reproduces in isolation" above. The reliable kept reproduction is the
> positional-delete loader test, not this harness.

A temporary local stress harness was also used to demonstrate the old bug
without keeping extra regression-test code in the PR. The harness repeatedly
created a `DeleteFileIndex`, released 2048 readers at the wait path, dropped
the delete-file sender, and failed as soon as any reader timed out.

With the pre-fix wait restored locally (`notifier.notified().await` directly
after the first state read), ten consecutive harness runs all reproduced the
hang quickly on macOS:

```text
run  1: failed at iteration 353  after  3.435s
run  2: failed at iteration 311  after  2.961s
run  3: failed at iteration 974  after  7.255s
run  4: failed at iteration 2950 after 19.324s
run  5: failed at iteration 1210 after  8.664s
run  6: failed at iteration 543  after  4.428s
run  7: failed at iteration 2815 after 19.999s
run  8: failed at iteration 976  after  7.207s
run  9: failed at iteration 136  after  1.845s
run 10: failed at iteration 553  after  4.559s
```

The slowest buggy run failed in roughly 20 seconds. With the fix in place,
the same stress shape was then run for 200 seconds (10x the slowest failure)
and completed successfully:

```text
completed 28937 lost-wakeup stress iterations with 2048 readers each in 200.001s
```

The same stress harness was also run against latest `upstream/main` at
`88ca8b6f` (`feat(encryption) [3/N] Support encryption: KMS (#2339)`), which
still had the direct `notifier.notified().await` wait in
`DeleteFileIndex::get_deletes_for_data_file`. It reproduced the bug:

```text
reader timed out - possible lost wakeup (iteration 939, readers 2048, elapsed 6.665s)
```

## Root cause

All three sites share the same shape:

```rust
// Reader
let notifier = {
    match self.state.read().unwrap()....get(key) {
        Some(Loading(notifier)) => notifier.clone(),
        Some(Loaded(value))     => return Some(value.clone()),
        None                    => return None,
    }
};                                  // <-- read lock dropped
notifier.notified().await;          // <-- subscription happens here
// re-read state, expect Loaded
```

```rust
// Publisher (in another task)
{
    let mut state = self.state.write().unwrap();
    state....insert(key, Loaded(value));
}                                   // <-- write lock dropped
notifier.notify_waiters();          // <-- wakes only subscribed waiters
```

If the publisher's write/notify sequence completes between the reader's
lock-release and the reader's `notified().await` poll, no waiter is yet
subscribed when `notify_waiters()` runs, so it wakes nothing.
`notify_waiters` does not store a permit. The reader then subscribes to a
notifier that will never fire again, and parks indefinitely.

## Affected sites

All three are in `crates/iceberg/src/`:

1. `delete_file_index.rs::DeleteFileIndex::get_deletes_for_data_file`
   - reader observes `DeleteFileIndexState::Populating(notifier)`
   - publisher (the background task spawned in `DeleteFileIndex::new`)
     transitions to `Populated` once the input stream closes
2. `arrow/delete_filter.rs::DeleteFilter::get_equality_delete_predicate_for_delete_file_path`
   - reader observes `EqDelState::Loading(notifier)`
   - publisher (`insert_equality_delete`'s spawned task) transitions to
     `EqDelState::Loaded` once the receiver yields the predicate
3. `arrow/caching_delete_file_loader.rs::CachingDeleteFileLoader::load_file_for_task`
   (positional-delete `WaitFor` branch)
   - reader receives `PosDelLoadAction::WaitFor(notify)` from
     `DeleteFilter::try_start_pos_del_load` and awaits the notifier
   - publisher (`DeleteFilter::finish_pos_del_load`) transitions to
     `PosDelState::Loaded` once positional-delete loading completes

## Fix

Use the standard `Notify` race-free pattern — register intent before
re-checking state — so any subsequent `notify_waiters` is guaranteed to
wake the subscribed future:

```rust
let notifier = {
    match self.state.read().unwrap()....get(key) {
        Some(Loading(notifier)) => notifier.clone(),
        Some(Loaded(value))     => return Some(value.clone()),
        None                    => return None,
    }
};

// Subscribe BEFORE re-reading state. Once `enable()` has run, any future
// `notify_waiters` is guaranteed to wake this future. notify_waiters
// stores no permit, so the order of operations matters.
let notified = notifier.notified();
tokio::pin!(notified);
notified.as_mut().enable();

// Re-check: the publisher may have completed between our first read and
// our subscription. If so, return the result directly without awaiting.
if let Some(Loaded(value)) = self.state.read().unwrap()....get(key) {
    return Some(value.clone());
}

notified.await;
// state is now guaranteed to be Loaded
```

### Correctness argument

Let `T_obs` be the moment the reader's first state read sees `Loading`,
`T_sub` the moment `notified.as_mut().enable()` returns, `T_recheck` the
moment the second state read returns, `T_trans` the moment the publisher's
write lock release makes the new `Loaded` state visible, and `T_notify`
the moment `notify_waiters()` runs.

The publisher's invariant is `T_trans < T_notify`. Three cases:

1. `T_notify < T_sub`: publisher already finished. By `T_recheck > T_sub`,
   the recheck observes `Loaded` (because `T_trans < T_notify < T_sub
   < T_recheck`). The reader returns without awaiting. Correct.
2. `T_sub < T_notify < T_recheck`: notify hits us while subscribed. The
   notification is delivered to our enabled `Notified` future. Recheck
   observes `Loaded` and we return without awaiting. Correct.
3. `T_recheck < T_notify`: recheck observes `Loading`, we proceed to
   `notified.await`. Subscription is already in place, so `notify_waiters`
   wakes us. Correct.

In all three cases the reader makes progress. The lost-wakeup case from
before — `T_notify < T_sub` with no re-check — is eliminated.

## Why not other primitives

`tokio::sync::watch::Sender<State>` or `tokio::sync::oneshot` wrapped in
`Arc<Shared<...>>` would also fix the bug by storing the completion event,
and may be preferable in the long term. The patch in this PR keeps the
`Notify` shape intact and only adds the subscribe-before-recheck step,
which is the smallest change that closes the race.
