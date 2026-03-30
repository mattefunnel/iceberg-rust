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
use iceberg_actions::{FileRewriter, RewriteDataFiles, RewriteFileGroup};

/// A passthrough rewriter that returns DataFile stubs from the scan tasks
/// unchanged. This validates the selection/grouping logic without actually
/// rewriting any data.
struct PassthroughFileRewriter;

#[async_trait]
impl FileRewriter for PassthroughFileRewriter {
    async fn rewrite(&self, _table: &Table, group: RewriteFileGroup) -> Result<Vec<DataFile>> {
        // Return the DataFile stubs derived from the scan tasks
        Ok(group.data_files_for_delete())
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
    async fn rewrite(&self, _table: &Table, group: RewriteFileGroup) -> Result<Vec<DataFile>> {
        let call = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call == 0 {
            Err(Error::new(
                ErrorKind::Unexpected,
                "simulated first-call failure",
            ))
        } else {
            Ok(group.data_files_for_delete())
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

/// Files below `min_file_size_bytes` should be selected as candidates.
/// When there are enough candidates (>= min_input_files), compaction
/// proceeds.
#[tokio::test]
async fn test_rewrite_small_files_selected() {
    let ctx = TestContext::new("rdf_small_files").await;

    let files: Vec<DataFile> = (0..6)
        .map(|i| {
            make_data_file(
                &format!("data/small-{i}.parquet"),
                1024, // 1 KiB — well below default 384 MiB
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

    assert_eq!(
        result.rewritten_data_files_count, 6,
        "small files should be selected for compaction"
    );
    assert_eq!(result.added_data_files_count, 6);
    assert_eq!(result.rewritten_bytes_count, 6 * 1024);
}

/// In-range files should NOT be selected even when the partition has many
/// files. Only undersized/oversized files are candidates.
#[tokio::test]
async fn test_rewrite_inrange_files_not_selected() {
    let ctx = TestContext::new("rdf_inrange").await;

    let files: Vec<DataFile> = (0..6)
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
        .min_input_files(5)
        .execute()
        .await
        .expect("rewrite should succeed");

    assert_eq!(
        result.rewritten_data_files_count, 0,
        "in-range files should not be selected even with many files in partition"
    );
}

/// With `partial_progress=true`, a failing rewriter for one group should
/// not abort the entire operation. Successful groups should still commit.
#[tokio::test]
async fn test_rewrite_partial_progress_on_failure() {
    let ctx = TestContext::new("rdf_partial").await;

    let mut files = Vec::new();
    for i in 0..6 {
        files.push(make_data_file(
            &format!("data/small-{i}.parquet"),
            1024, // 1 KiB
            Struct::empty(),
        ));
    }
    let table = append_files(&ctx, files).await;

    let result = RewriteDataFiles::new(&table, ctx.catalog.as_ref(), FailOnceFileRewriter::new())
        .min_input_files(3)
        .max_file_group_size_bytes(3 * 1024) // Force groups of ~3 files
        .partial_progress(true)
        .execute()
        .await
        .expect("partial progress should not return an error");

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
