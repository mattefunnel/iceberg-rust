# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Code Review

This branch should not be sent for Apache Iceberg review as-is. The core
`tokio::sync::Notify` pattern is directionally correct, but the branch still
has correctness and review-readiness issues that would likely block it.

## Findings

### Blocker: equality-delete waiters can still hang on an orphaned notifier

`DeleteFilter::try_start_eq_del_load` stores one `Notify`, but
`DeleteFilter::insert_equality_delete` immediately overwrites the state with a
different `Notify`. A concurrent reader can observe the first notifier and wait
forever, because completion only notifies the second notifier.

DeleteFilter::try_start_eq_del_load and DeleteFilter::insert_equality_delete
still use different Notify instances for the same equality-delete load.

Relevant code:

```rust
pub(crate) fn try_start_eq_del_load(&self, file_path: &str) -> Option<Arc<Notify>> {
    let mut state = self.state.write().unwrap();

    if state.equality_deletes.contains_key(file_path) {
        return None;
    }

    let notifier = Arc::new(Notify::new());
    state
        .equality_deletes
        .insert(file_path.to_string(), EqDelState::Loading(notifier.clone()));

    Some(notifier)
}
```

```rust
pub(crate) fn insert_equality_delete(
    &self,
    delete_file_path: &str,
    eq_del: Receiver<Predicate>,
) {
    let notify = Arc::new(Notify::new());
    {
        let mut state = self.state.write().unwrap();
        state.equality_deletes.insert(
            delete_file_path.to_string(),
            EqDelState::Loading(notify.clone()),
        );
    }

    // completion notifies only this second notifier
}
```

Fix direction: preserve the original notifier, or merge "mark loading" and
receiver installation into one state transition. The unused `notify` binding in
`CachingDeleteFileLoader` is a red flag here.

### Blocker: `NOTIFY_LOST_WAKEUP.md` is not suitable to commit

The new root Markdown file has no ASF license header, and `.licenserc.yaml`
does not ignore general Markdown files. This is likely to fail Apache header
checks.

Separately, a 494-line forensic/debug note with local harness details belongs
in the PR description or issue discussion, not in the source tree.

### High: regression coverage is too weak for a concurrency bug

The new test is explicitly non-deterministic, only covers
`DeleteFileIndex::get_deletes_for_data_file`, and does not prove any spawned
reader actually reached the wait state before `drop(tx)`.

For this kind of async race fix, reviewers are likely to expect deterministic
tests around the affected state machines, including:

- `DeleteFileIndex::get_deletes_for_data_file`
- `DeleteFilter::get_equality_delete_predicate_for_delete_file_path`
- `DeleteFilter::wait_for_pos_del_load`

The current stress-style test is useful as smoke coverage, but not as a
convincing regression test.

### Medium: `NOTIFY_LOST_WAKEUP.md` is stale and inconsistent with the code

The note references a non-existent `wait_for_eq_del_load`, while the code
patches `get_equality_delete_predicate_for_delete_file_path` directly. It also
still describes `PosDelLoadAction::WaitFor(notify)`, even though the enum no
longer carries a notifier.

## Checks Run

- `cargo test -p iceberg get_deletes_for_data_file_wait_path`
- `cargo fmt --check`
- `cargo check -p iceberg`
- `cargo clippy -p iceberg --all-targets -- -D warnings`

All checks passed.

## Overall Assessment

The `DeleteFileIndex` wait path and the positional-delete wait helper use the
right `Notify` shape: create `Notified`, pin it, call `enable`, re-check state,
then await. That pattern is sound for avoiding `notify_waiters` lost wakeups.

However, the equality-delete notifier replacement bug, weak regression
coverage, and committed debug note mean the branch is not production-quality
yet.
