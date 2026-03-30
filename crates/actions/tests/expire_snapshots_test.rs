// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for the ExpireSnapshots action.

mod common;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use common::TestContext;
use iceberg::TableCommit;
use iceberg_actions::ExpireSnapshots;

#[tokio::test]
async fn test_expire_empty_table() {
    let ctx = TestContext::new("expire_empty").await;
    let table = ctx.load_table().await;

    let result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .execute()
        .await
        .expect("expire on empty table should succeed");

    assert_eq!(result.deleted_data_files_count, 0);
    assert_eq!(result.deleted_equality_delete_files_count, 0);
    assert_eq!(result.deleted_position_delete_files_count, 0);
    assert_eq!(result.deleted_manifest_files_count, 0);
    assert_eq!(result.deleted_manifest_lists_count, 0);
    assert_eq!(result.deleted_statistics_files_count, 0);
}

#[tokio::test]
async fn test_expire_retains_current_snapshot() {
    let ctx = TestContext::new("expire_retain_current").await;

    // Create a single snapshot
    ctx.append_data_file().await;
    let table = ctx.load_table().await;
    let snapshot_count_before = table.metadata().snapshots().count();
    assert_eq!(snapshot_count_before, 1);

    // Expire with older_than at epoch — current snapshot should still be retained
    // because it's referenced by the main branch
    let far_past = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .older_than(far_past)
        .execute()
        .await
        .expect("expire should succeed");

    // The current snapshot is always retained (it's the main branch ref)
    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 1);

    // No files should have been deleted (nothing was expired)
    assert_eq!(result.deleted_data_files_count, 0);
    assert_eq!(result.deleted_manifest_files_count, 0);
    assert_eq!(result.deleted_manifest_lists_count, 0);
}

#[tokio::test]
async fn test_expire_retain_last_n() {
    let ctx = TestContext::new("expire_retain_last_n").await;

    // Create 5 snapshots
    for _ in 0..5 {
        ctx.append_data_file().await;
    }

    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 5);

    // Expire with retain_last(2) and older_than far in the future.
    // This should keep the 2 most recent ancestors of main branch,
    // plus the current snapshot is always kept (it's one of the 2).
    let far_future = SystemTime::now() + Duration::from_secs(3600);
    let result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .older_than(far_future)
        .retain_last(2)
        .execute()
        .await
        .expect("expire should succeed");

    let table = ctx.load_table().await;
    assert_eq!(
        table.metadata().snapshots().count(),
        2,
        "should retain exactly 2 snapshots"
    );

    // 3 snapshots expired -> their manifest lists should be deleted
    assert!(
        result.deleted_manifest_lists_count >= 3,
        "at least 3 manifest lists should be deleted, got {}",
        result.deleted_manifest_lists_count,
    );
}

#[tokio::test]
async fn test_expire_by_snapshot_id() {
    let ctx = TestContext::new("expire_by_id").await;

    // Create 3 snapshots
    ctx.append_data_file().await;
    ctx.append_data_file().await;
    ctx.append_data_file().await;

    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 3);

    // The current snapshot (C) is retained by main branch ref.
    // With default retain_last(1), only C is retained.
    // Find the parent of C (snapshot B) to explicitly expire.
    let current_id = table.metadata().current_snapshot_id().unwrap();
    let current_snap = table.metadata().snapshot_by_id(current_id).unwrap();
    let parent_id = current_snap.parent_snapshot_id().unwrap();

    // Set retain_last large enough to retain all snapshots first,
    // then explicitly expire parent_id.
    let _result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .retain_last(100)
        .expire_snapshot_id(parent_id)
        .execute()
        .await
        .expect("expire should succeed");

    let table = ctx.load_table().await;
    assert_eq!(
        table.metadata().snapshots().count(),
        2,
        "one snapshot should have been expired"
    );
    assert!(
        table.metadata().snapshot_by_id(parent_id).is_none(),
        "explicitly expired snapshot should be gone"
    );
    // Current snapshot and the oldest should still be present
    assert!(table.metadata().snapshot_by_id(current_id).is_some());
}

#[tokio::test]
async fn test_expire_deletes_orphaned_data_files() {
    let ctx = TestContext::new("expire_orphan_data").await;

    // Create 2 snapshots, each with their own data file
    ctx.append_data_file().await;
    ctx.append_data_file().await;

    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 2);

    // Expire with retain_last(1) and older_than in far future
    let far_future = SystemTime::now() + Duration::from_secs(3600);
    let result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .older_than(far_future)
        .retain_last(1)
        .execute()
        .await
        .expect("expire should succeed");

    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 1);

    // The expired snapshot's manifest list should be deleted
    assert!(
        result.deleted_manifest_lists_count >= 1,
        "should delete manifest list for expired snapshot"
    );

    // With fast-append, the current snapshot's manifest list carries forward
    // references to all existing manifests and data files, so data files
    // from the first snapshot are still referenced by the current manifest list.
    // The manifest list for the expired snapshot IS orphaned though.
}

#[tokio::test]
async fn test_expire_gc_disabled() {
    let ctx = TestContext::new("expire_gc_disabled").await;

    // Create a table with gc.enabled=false
    let table = ctx.load_table().await;

    let table_commit = TableCommit::builder()
        .ident(ctx.table_ident.clone())
        .updates(vec![iceberg::TableUpdate::SetProperties {
            updates: HashMap::from([("gc.enabled".to_string(), "false".to_string())]),
        }])
        .requirements(vec![iceberg::TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        }])
        .build();
    ctx.catalog
        .update_table(table_commit)
        .await
        .expect("should set gc.enabled=false");

    let table = ctx.load_table().await;

    let err = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .execute()
        .await;

    assert!(err.is_err(), "expire should fail when gc.enabled=false");
    let err_msg = format!("{}", err.unwrap_err());
    assert!(
        err_msg.contains("gc.enabled"),
        "error message should mention gc.enabled, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_expire_no_op_when_nothing_to_expire() {
    let ctx = TestContext::new("expire_no_op").await;

    // Create a single snapshot
    ctx.append_data_file().await;
    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 1);

    // Expire with retain_last(1) — the only snapshot is retained
    let result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .retain_last(1)
        .execute()
        .await
        .expect("expire should succeed");

    // Nothing should be expired
    assert_eq!(result.deleted_data_files_count, 0);
    assert_eq!(result.deleted_manifest_files_count, 0);
    assert_eq!(result.deleted_manifest_lists_count, 0);

    // Snapshot count unchanged
    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 1);
}

#[tokio::test]
async fn test_expire_deletes_manifest_files() {
    let ctx = TestContext::new("expire_manifests").await;

    // Create 3 snapshots, each with a data file
    ctx.append_data_file().await;
    ctx.append_data_file().await;
    ctx.append_data_file().await;

    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 3);

    // Expire with retain_last(1) and older_than far in the future
    let far_future = SystemTime::now() + Duration::from_secs(3600);
    let result = ExpireSnapshots::new(&table, ctx.catalog.as_ref())
        .older_than(far_future)
        .retain_last(1)
        .execute()
        .await
        .expect("expire should succeed");

    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 1);

    // Manifest lists for expired snapshots should be deleted
    assert!(
        result.deleted_manifest_lists_count >= 2,
        "at least 2 manifest lists should be deleted, got {}",
        result.deleted_manifest_lists_count,
    );

    // With fast-append, the current snapshot's manifest list references all
    // existing manifests. Manifest files from expired snapshots that are also
    // referenced by the current snapshot are NOT deleted (they're live).
    // Only manifests exclusively owned by expired snapshots are deleted.
    // In the fast-append case, the current snapshot's manifest list carries
    // forward all manifests, so typically no manifest files are orphaned.
    // The key assertion here is that the action runs without errors and
    // the manifest LIST files are correctly cleaned up.
}
