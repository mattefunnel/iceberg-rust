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

//! Integration tests for the RewriteManifests action.

mod common;

use std::collections::HashSet;

use common::TestContext;
use iceberg_actions::RewriteManifests;

/// Empty table (no snapshots) should return (0, 0).
#[tokio::test]
async fn test_rewrite_manifests_empty_table() {
    let ctx = TestContext::new("rewrite_empty").await;
    let table = ctx.load_table().await;

    let result = RewriteManifests::new(&table, ctx.catalog.as_ref())
        .execute()
        .await
        .expect("rewrite on empty table should succeed");

    assert_eq!(result.rewritten_manifests_count, 0);
    assert_eq!(result.added_manifests_count, 0);
}

/// Only 1 manifest — nothing to merge, should return (0, 0).
#[tokio::test]
async fn test_rewrite_manifests_single_manifest_noop() {
    let ctx = TestContext::new("rewrite_single").await;

    // Single append creates a single manifest.
    ctx.append_data_file().await;
    let table = ctx.load_table().await;

    let result = RewriteManifests::new(&table, ctx.catalog.as_ref())
        .target_size_bytes(u64::MAX)
        .execute()
        .await
        .expect("rewrite with single manifest should succeed");

    assert_eq!(result.rewritten_manifests_count, 0);
    assert_eq!(result.added_manifests_count, 0);
}

/// Multiple small manifests should be merged into fewer manifests.
#[tokio::test]
async fn test_rewrite_manifests_merges_small_manifests() {
    let ctx = TestContext::new("rewrite_merge").await;

    // Create 4 separate snapshots, each adding a manifest.
    for _ in 0..4 {
        ctx.append_data_file().await;
    }

    let table = ctx.load_table().await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    let manifest_count_before = manifest_list.entries().len();
    assert_eq!(manifest_count_before, 4);

    // Set target_size very high so all manifests are considered "small".
    let result = RewriteManifests::new(&table, ctx.catalog.as_ref())
        .target_size_bytes(u64::MAX)
        .execute()
        .await
        .expect("rewrite should succeed");

    assert_eq!(result.rewritten_manifests_count, 4);
    assert_eq!(result.added_manifests_count, 1);

    // Verify the new table has only 1 manifest.
    let table = ctx.load_table().await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);
}

/// If all manifests exceed target_size, nothing should be merged.
#[tokio::test]
async fn test_rewrite_manifests_noop_when_already_large() {
    let ctx = TestContext::new("rewrite_large").await;

    // Create 3 separate snapshots.
    for _ in 0..3 {
        ctx.append_data_file().await;
    }

    let table = ctx.load_table().await;

    // Set target_size to 0 so no manifest is considered "small".
    let result = RewriteManifests::new(&table, ctx.catalog.as_ref())
        .target_size_bytes(0)
        .execute()
        .await
        .expect("rewrite with all-large manifests should succeed");

    assert_eq!(result.rewritten_manifests_count, 0);
    assert_eq!(result.added_manifests_count, 0);
}

/// After rewrite, the same data files should be accessible.
#[tokio::test]
async fn test_rewrite_manifests_preserves_entries() {
    let ctx = TestContext::new("rewrite_preserves").await;

    // Create 3 separate snapshots, each with 1 data file.
    for _ in 0..3 {
        ctx.append_data_file().await;
    }

    // Collect all file paths before rewrite.
    let table = ctx.load_table().await;
    let file_paths_before = collect_data_file_paths(&table).await;
    assert_eq!(file_paths_before.len(), 3);

    // Rewrite manifests.
    RewriteManifests::new(&table, ctx.catalog.as_ref())
        .target_size_bytes(u64::MAX)
        .execute()
        .await
        .expect("rewrite should succeed");

    // Collect all file paths after rewrite and verify they match.
    let table = ctx.load_table().await;
    let file_paths_after = collect_data_file_paths(&table).await;
    assert_eq!(file_paths_after.len(), 3);
    assert_eq!(file_paths_before, file_paths_after);
}

/// Helper: collect all alive data file paths from the current snapshot.
async fn collect_data_file_paths(table: &iceberg::table::Table) -> HashSet<String> {
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .unwrap();

    let mut paths = HashSet::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        for entry in manifest.entries() {
            if entry.is_alive() {
                paths.insert(entry.file_path().to_string());
            }
        }
    }
    paths
}
