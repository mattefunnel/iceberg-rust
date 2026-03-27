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

//! Integration tests for the RewriteDataFiles action.

mod common;

use async_trait::async_trait;
use common::TestContext;
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Error, ErrorKind, Result};
use iceberg_actions::{FileRewriter, RewriteDataFiles};

/// A passthrough rewriter that returns the input files unchanged.
/// This validates the selection/grouping logic without actually
/// rewriting any data.
struct PassthroughFileRewriter;

#[async_trait]
impl FileRewriter for PassthroughFileRewriter {
    async fn rewrite_files(&self, _table: &Table, group: Vec<DataFile>) -> Result<Vec<DataFile>> {
        Ok(group)
    }
}

/// A rewriter that fails on the first call and succeeds on subsequent calls.
struct FailOnceFileRewriter {
    call_count: std::sync::atomic::AtomicU32,
}

impl FailOnceFileRewriter {
    fn new() -> Self {
        Self {
            call_count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl FileRewriter for FailOnceFileRewriter {
    async fn rewrite_files(&self, _table: &Table, group: Vec<DataFile>) -> Result<Vec<DataFile>> {
        let call = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call == 0 {
            Err(Error::new(
                ErrorKind::Unexpected,
                "simulated first-call failure",
            ))
        } else {
            Ok(group)
        }
    }
}

/// Helper: build a synthetic data file with the given path, size, and partition.
fn make_data_file(path: &str, file_size: u64, partition: Struct) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(100)
        .partition(partition)
        .partition_spec_id(0)
        .build()
        .expect("failed to build DataFile")
}

/// Helper: append a list of data files to a table via fast-append,
/// returning the updated table.
async fn append_files(ctx: &TestContext, files: Vec<DataFile>) -> Table {
    let table = ctx.load_table().await;
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(files);
    let tx = action.apply(tx).expect("failed to apply fast_append");
    tx.commit(ctx.catalog.as_ref())
        .await
        .expect("failed to commit fast_append");
    ctx.load_table().await
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

/// Empty table (no snapshots) should produce a zero result.
#[tokio::test]
async fn test_rewrite_empty_table() {
    let ctx = TestContext::new("rdf_empty").await;
    let table = ctx.load_table().await;

    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), PassthroughFileRewriter)
        .execute()
        .await
        .expect("rewrite on empty table should succeed");

    assert_eq!(result.rewritten_data_files_count, 0);
    assert_eq!(result.added_data_files_count, 0);
    assert_eq!(result.rewritten_bytes_count, 0);
    assert_eq!(result.failed_data_files_count, 0);
    assert_eq!(result.removed_delete_files_count, 0);
}

/// When there are fewer files than `min_input_files` and all files are
/// within the acceptable size range, no compaction should occur.
#[tokio::test]
async fn test_rewrite_below_min_input_files_noop() {
    let ctx = TestContext::new("rdf_below_min").await;

    // Create 3 files that are within the size range.
    // Use min_input_files = 5 (default), so 3 < 5 means no compaction
    // as long as file sizes are within range.
    let files: Vec<DataFile> = (0..3)
        .map(|i| {
            make_data_file(
                &format!("data/file-{i}.parquet"),
                500 * 1024 * 1024, // 500 MiB — within default [384 MiB, 921 MiB]
                Struct::empty(),
            )
        })
        .collect();
    let table = append_files(&ctx, files).await;

    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), PassthroughFileRewriter)
        .execute()
        .await
        .expect("rewrite should succeed");

    assert_eq!(
        result.rewritten_data_files_count, 0,
        "no compaction should occur when file count < min_input_files and sizes are in range"
    );
    assert_eq!(result.added_data_files_count, 0);
}

/// Files below `min_file_size_bytes` should be selected as candidates
/// even when the total count is below `min_input_files`.
#[tokio::test]
async fn test_rewrite_small_files_selected() {
    let ctx = TestContext::new("rdf_small_files").await;

    // Create 2 small files (below min_file_size_bytes).
    let files: Vec<DataFile> = (0..2)
        .map(|i| {
            make_data_file(
                &format!("data/small-{i}.parquet"),
                1024, // 1 KiB — well below default 384 MiB
                Struct::empty(),
            )
        })
        .collect();
    let table = append_files(&ctx, files).await;

    // Even with min_input_files=5 (default), small files are candidates.
    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), PassthroughFileRewriter)
        .min_input_files(5)
        .execute()
        .await
        .expect("rewrite should succeed");

    // PassthroughFileRewriter returns the same files, so the rewrite
    // replaces 2 files with 2 files.
    assert_eq!(
        result.rewritten_data_files_count, 2,
        "small files should be selected for compaction"
    );
    assert_eq!(result.added_data_files_count, 2);
    assert_eq!(result.rewritten_bytes_count, 2 * 1024);
}

/// For an unpartitioned table (all files share Struct::empty()),
/// all files land in one partition group. When there are enough files
/// (>= min_input_files), all are selected for compaction.
#[tokio::test]
async fn test_rewrite_groups_by_partition() {
    let ctx = TestContext::new("rdf_partitions").await;

    // Create 6 files, all unpartitioned (Struct::empty()).
    // With min_input_files=5, all 6 should be selected because
    // they form a single partition group with count >= 5.
    let files: Vec<DataFile> = (0..6)
        .map(|i| {
            make_data_file(
                &format!("data/file-{i}.parquet"),
                500 * 1024 * 1024, // 500 MiB, within default range
                Struct::empty(),
            )
        })
        .collect();
    let table = append_files(&ctx, files).await;

    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), PassthroughFileRewriter)
        .min_input_files(5)
        .execute()
        .await
        .expect("rewrite should succeed");

    // All 6 files are in one partition group (unpartitioned) with count >= 5,
    // so all are candidates.
    assert_eq!(
        result.rewritten_data_files_count, 6,
        "all files in the unpartitioned group should be compacted"
    );
    assert_eq!(result.added_data_files_count, 6);

    // Now test with min_input_files=10: no compaction because 6 < 10
    // and all files are within the size range.
    let table = ctx.load_table().await;
    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), PassthroughFileRewriter)
        .min_input_files(10)
        .execute()
        .await
        .expect("rewrite should succeed");

    assert_eq!(
        result.rewritten_data_files_count, 0,
        "no compaction when count < min_input_files and sizes in range"
    );
}

/// With `partial_progress=true`, a failing rewriter for one group should
/// not abort the entire operation. Successful groups should still commit.
#[tokio::test]
async fn test_rewrite_partial_progress_on_failure() {
    let ctx = TestContext::new("rdf_partial").await;

    // Create enough small files to form at least 2 groups.
    // We use max_file_group_size_bytes to force splitting into groups.
    let mut files = Vec::new();
    for i in 0..6 {
        files.push(make_data_file(
            &format!("data/small-{i}.parquet"),
            1024, // 1 KiB
            Struct::empty(),
        ));
    }
    let table = append_files(&ctx, files).await;

    // Use FailOnceFileRewriter: first group fails, second succeeds.
    // Set max_file_group_size_bytes very small to force multiple groups.
    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), FailOnceFileRewriter::new())
        .min_input_files(3)
        .max_file_group_size_bytes(3 * 1024) // Force groups of ~3 files
        .partial_progress(true)
        .execute()
        .await
        .expect("partial progress should not return an error");

    // One group should have failed, one should have succeeded.
    assert!(
        result.failed_data_files_count > 0,
        "at least one group should have failed"
    );
    assert!(
        result.rewritten_data_files_count > 0,
        "at least one group should have succeeded"
    );
    assert_eq!(
        result.rewritten_data_files_count + result.failed_data_files_count,
        6,
        "total should account for all files"
    );
}
