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

//! Rewrite data files action — compacts small data files into larger ones
//! using a pluggable [`FileRewriter`] trait. This is the bin-pack compaction
//! action analogous to Spark's `RewriteDataFiles`.
//!
//! Planning uses [`TableScan::plan_files()`] to get [`FileScanTask`] objects
//! that carry associated delete files and partition context. The
//! [`FileRewriter`] can then read through Iceberg's delete-aware
//! [`ArrowReaderBuilder`] to produce correct output.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, PartitionSpec, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result};

/// Default target file size: 512 MiB.
const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;

/// Default minimum file size (75% of target): 384 MiB.
const DEFAULT_MIN_FILE_SIZE_BYTES: u64 = 384 * 1024 * 1024;

/// Default maximum file size (180% of target): ~922 MiB.
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 921 * 1024 * 1024;

/// Default minimum number of candidate files in a partition to trigger
/// compaction.
const DEFAULT_MIN_INPUT_FILES: usize = 5;

/// Default maximum size of a single file group: 100 GiB.
const DEFAULT_MAX_FILE_GROUP_SIZE_BYTES: u64 = 100 * 1024 * 1024 * 1024;

/// A group of files to be rewritten together.
///
/// Each group carries [`FileScanTask`] objects that include associated delete
/// files and partition context. The [`FileRewriter`] should use Iceberg's
/// delete-aware reader (e.g. [`ArrowReaderBuilder`]) to read these tasks so
/// that equality/position deletes are applied during compaction.
#[derive(Debug, Clone)]
pub struct RewriteFileGroup {
    /// The scan tasks for files in this group. Each task carries the data
    /// file path, associated delete files, partition, and schema context.
    pub tasks: Vec<FileScanTask>,
    /// Target output file size in bytes.
    pub target_file_size_bytes: u64,
    /// The partition spec for this group's files.
    pub partition_spec: Arc<PartitionSpec>,
    /// The partition value shared by all files in this group.
    pub partition_value: Option<Struct>,
}

impl RewriteFileGroup {
    /// Build minimal [`DataFile`] stubs from the scan tasks, suitable for
    /// passing to [`RewriteFilesAction::delete_files()`]. The commit only
    /// matches by path; the real manifest entry data comes from the manifest.
    pub fn data_files_for_delete(&self) -> Vec<DataFile> {
        self.tasks
            .iter()
            .map(|task| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(task.data_file_path.clone())
                    .file_format(task.data_file_format)
                    .file_size_in_bytes(task.file_size_in_bytes)
                    .record_count(task.record_count.unwrap_or(0))
                    .partition(task.partition.clone().unwrap_or_else(Struct::empty))
                    .partition_spec_id(
                        task.partition_spec
                            .as_ref()
                            .map(|s| s.spec_id())
                            .unwrap_or(0),
                    )
                    .build()
                    .expect("failed to build DataFile stub from FileScanTask")
            })
            .collect()
    }
}

/// Pluggable trait for rewriting data files. Any compute engine (DataFusion,
/// Spark, etc.) implements this to provide the actual file compaction logic.
///
/// The rewriter receives a [`RewriteFileGroup`] containing [`FileScanTask`]
/// objects with delete-file associations, target file size, and partition
/// context. Implementations should use Iceberg's delete-aware reader (e.g.
/// `ArrowReaderBuilder`) to read the tasks so that logically deleted rows
/// are excluded from the compacted output.
#[async_trait]
pub trait FileRewriter: Send + Sync {
    /// Rewrite a group of data files, producing a new set of (typically fewer,
    /// larger) data files with deletes applied.
    async fn rewrite(&self, table: &Table, group: RewriteFileGroup) -> Result<Vec<DataFile>>;
}

/// Result of executing the rewrite data files action.
#[derive(Debug, Default, Clone)]
pub struct RewriteDataFilesResult {
    /// Number of data files that were removed (rewritten).
    pub rewritten_data_files_count: u32,
    /// Number of new data files that were added.
    pub added_data_files_count: u32,
    /// Total bytes of rewritten input files.
    pub rewritten_bytes_count: u64,
    /// Number of data files that failed to rewrite (only relevant with
    /// `partial_progress` enabled).
    pub failed_data_files_count: u32,
    /// Number of delete files removed. Always 0 in the current implementation.
    pub removed_delete_files_count: u32,
}

/// Action that compacts data files by selecting groups of small files,
/// rewriting them via a [`FileRewriter`], and committing the replacement
/// atomically using the transaction API.
pub struct RewriteDataFiles<'a, R: FileRewriter> {
    table: &'a Table,
    catalog: &'a dyn Catalog,
    rewriter: R,
    target_file_size_bytes: u64,
    min_file_size_bytes: u64,
    max_file_size_bytes: u64,
    min_input_files: usize,
    max_file_group_size_bytes: u64,
    partial_progress: bool,
}

impl<'a, R: FileRewriter> RewriteDataFiles<'a, R> {
    /// Create a new RewriteDataFiles action.
    pub fn new(table: &'a Table, catalog: &'a dyn Catalog, rewriter: R) -> Self {
        Self {
            table,
            catalog,
            rewriter,
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE_BYTES,
            min_file_size_bytes: DEFAULT_MIN_FILE_SIZE_BYTES,
            max_file_size_bytes: DEFAULT_MAX_FILE_SIZE_BYTES,
            min_input_files: DEFAULT_MIN_INPUT_FILES,
            max_file_group_size_bytes: DEFAULT_MAX_FILE_GROUP_SIZE_BYTES,
            partial_progress: false,
        }
    }

    /// Set the target file size in bytes. Defaults to 512 MiB.
    pub fn target_file_size_bytes(mut self, size: u64) -> Self {
        self.target_file_size_bytes = size;
        self
    }

    /// Set the minimum file size in bytes. Files smaller than this are
    /// candidates for compaction. Defaults to 384 MiB (75% of target).
    pub fn min_file_size_bytes(mut self, size: u64) -> Self {
        self.min_file_size_bytes = size;
        self
    }

    /// Set the maximum file size in bytes. Files larger than this are
    /// candidates for compaction. Defaults to ~922 MiB (180% of target).
    pub fn max_file_size_bytes(mut self, size: u64) -> Self {
        self.max_file_size_bytes = size;
        self
    }

    /// Set the minimum number of candidate files in a partition to trigger
    /// compaction. Defaults to 5.
    pub fn min_input_files(mut self, n: usize) -> Self {
        self.min_input_files = n;
        self
    }

    /// Set the maximum size (in bytes) of a single file group passed to the
    /// [`FileRewriter`]. Defaults to 100 GiB.
    pub fn max_file_group_size_bytes(mut self, size: u64) -> Self {
        self.max_file_group_size_bytes = size;
        self
    }

    /// Enable partial progress mode. When enabled, each file group is
    /// committed independently so that a failure in one group does not
    /// abort the entire operation. Defaults to `false`.
    pub fn partial_progress(mut self, val: bool) -> Self {
        self.partial_progress = val;
        self
    }

    /// Execute the rewrite data files action.
    pub async fn execute(self) -> Result<RewriteDataFilesResult> {
        let metadata = self.table.metadata();

        // 1. If no current snapshot, nothing to compact.
        let current_snapshot = match metadata.current_snapshot() {
            Some(s) => s,
            None => return Ok(RewriteDataFilesResult::default()),
        };

        // Record the planning snapshot for concurrent-delete validation.
        let planning_snapshot_id = current_snapshot.snapshot_id();
        let planning_sequence_number = current_snapshot.sequence_number();

        // 2. Use TableScan::plan_files() to get FileScanTask objects.
        //    Each task carries the data file, associated delete files,
        //    partition and schema context.
        let scan = self
            .table
            .scan()
            .snapshot_id(planning_snapshot_id)
            .select_all()
            .build()?;

        let task_stream = scan.plan_files().await?;
        let tasks: Vec<FileScanTask> = task_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // 3. Group tasks by partition value.
        let mut partition_tasks: HashMap<Struct, Vec<FileScanTask>> = HashMap::new();
        for task in tasks {
            let key = task.partition.clone().unwrap_or_else(Struct::empty);
            partition_tasks.entry(key).or_default().push(task);
        }

        // 4. For each partition, select candidates and bin-pack into groups.
        let mut file_groups: Vec<RewriteFileGroup> = Vec::new();

        for (partition_value, tasks) in &partition_tasks {
            let candidates: Vec<&FileScanTask> = tasks
                .iter()
                .filter(|t| {
                    t.file_size_in_bytes < self.min_file_size_bytes
                        || t.file_size_in_bytes > self.max_file_size_bytes
                })
                .collect();

            if candidates.len() < self.min_input_files {
                continue;
            }

            // Use the partition spec from the first candidate. All tasks in
            // this group share the same partition value; the spec may differ
            // on spec-evolved tables but bin-pack groups within one partition.
            let spec = candidates
                .first()
                .and_then(|t| t.partition_spec.clone())
                .unwrap_or_else(|| metadata.default_partition_spec().clone());

            let groups = self.bin_pack(candidates, partition_value, spec);
            file_groups.extend(groups);
        }

        if file_groups.is_empty() {
            return Ok(RewriteDataFilesResult::default());
        }

        // 5. Rewrite each group and commit.
        if self.partial_progress {
            self.execute_partial_progress(
                file_groups,
                planning_snapshot_id,
                planning_sequence_number,
            )
            .await
        } else {
            self.execute_all_at_once(file_groups, planning_snapshot_id, planning_sequence_number)
                .await
        }
    }

    /// Bin-pack candidate tasks into groups, each not exceeding
    /// `max_file_group_size_bytes`.
    fn bin_pack(
        &self,
        candidates: Vec<&FileScanTask>,
        partition_value: &Struct,
        partition_spec: Arc<PartitionSpec>,
    ) -> Vec<RewriteFileGroup> {
        let mut groups: Vec<RewriteFileGroup> = Vec::new();
        let mut current_tasks: Vec<FileScanTask> = Vec::new();
        let mut current_size: u64 = 0;

        for task in candidates {
            let file_size = task.file_size_in_bytes;

            if !current_tasks.is_empty()
                && current_size + file_size > self.max_file_group_size_bytes
            {
                groups.push(RewriteFileGroup {
                    tasks: std::mem::take(&mut current_tasks),
                    target_file_size_bytes: self.target_file_size_bytes,
                    partition_spec: partition_spec.clone(),
                    partition_value: Some(partition_value.clone()),
                });
                current_size = 0;
            }

            current_tasks.push(task.clone());
            current_size += file_size;
        }

        if !current_tasks.is_empty() {
            groups.push(RewriteFileGroup {
                tasks: current_tasks,
                target_file_size_bytes: self.target_file_size_bytes,
                partition_spec: partition_spec.clone(),
                partition_value: Some(partition_value.clone()),
            });
        }

        groups
    }

    /// Execute all groups as a single atomic commit with commit-failure cleanup.
    async fn execute_all_at_once(
        &self,
        file_groups: Vec<RewriteFileGroup>,
        planning_snapshot_id: i64,
        planning_sequence_number: i64,
    ) -> Result<RewriteDataFilesResult> {
        let mut all_files_to_delete: Vec<DataFile> = Vec::new();
        let mut all_files_to_add: Vec<DataFile> = Vec::new();
        let mut rewritten_bytes: u64 = 0;

        for group in file_groups {
            rewritten_bytes += group
                .tasks
                .iter()
                .map(|t| t.file_size_in_bytes)
                .sum::<u64>();
            let files_to_delete = group.data_files_for_delete();
            let new_files = self.rewriter.rewrite(self.table, group).await?;
            all_files_to_delete.extend(files_to_delete);
            all_files_to_add.extend(new_files);
        }

        let rewritten_count = all_files_to_delete.len() as u32;
        let added_count = all_files_to_add.len() as u32;

        // Track output file paths for commit-failure cleanup
        let output_file_paths: Vec<String> = all_files_to_add
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();

        let tx = Transaction::new(self.table);
        let action = tx
            .rewrite_files()
            .delete_files(all_files_to_delete)
            .add_files(all_files_to_add)
            .validate_from_snapshot(planning_snapshot_id)
            .data_sequence_number(planning_sequence_number);
        let tx = action.apply(tx).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to apply rewrite files action: {e}"),
            )
        })?;

        match tx.commit(self.catalog).await {
            Ok(_) => Ok(RewriteDataFilesResult {
                rewritten_data_files_count: rewritten_count,
                added_data_files_count: added_count,
                rewritten_bytes_count: rewritten_bytes,
                failed_data_files_count: 0,
                removed_delete_files_count: 0,
            }),
            Err(e) => {
                // Best-effort cleanup of output files on commit failure
                let file_io = self.table.file_io();
                for path in &output_file_paths {
                    let _ = file_io.delete(path).await;
                }
                Err(e)
            }
        }
    }

    /// Execute each group as an independent commit, allowing partial progress.
    async fn execute_partial_progress(
        &self,
        file_groups: Vec<RewriteFileGroup>,
        planning_snapshot_id: i64,
        planning_sequence_number: i64,
    ) -> Result<RewriteDataFilesResult> {
        let mut result = RewriteDataFilesResult::default();

        for group in file_groups {
            let group_file_count = group.tasks.len() as u32;
            let group_bytes: u64 = group.tasks.iter().map(|t| t.file_size_in_bytes).sum();
            let files_to_delete = group.data_files_for_delete();

            match self.rewriter.rewrite(self.table, group).await {
                Ok(new_files) => {
                    let added_count = new_files.len() as u32;
                    let output_paths: Vec<String> = new_files
                        .iter()
                        .map(|f| f.file_path().to_string())
                        .collect();

                    let table = self.catalog.load_table(self.table.identifier()).await?;

                    let tx = Transaction::new(&table);
                    let action = tx
                        .rewrite_files()
                        .delete_files(files_to_delete)
                        .add_files(new_files)
                        .validate_from_snapshot(planning_snapshot_id)
                        .data_sequence_number(planning_sequence_number);
                    let tx = match action.apply(tx) {
                        Ok(tx) => tx,
                        Err(_) => {
                            Self::cleanup_files(self.table.file_io(), &output_paths).await;
                            result.failed_data_files_count += group_file_count;
                            continue;
                        }
                    };

                    match tx.commit(self.catalog).await {
                        Ok(_) => {
                            result.rewritten_data_files_count += group_file_count;
                            result.added_data_files_count += added_count;
                            result.rewritten_bytes_count += group_bytes;
                        }
                        Err(_) => {
                            Self::cleanup_files(self.table.file_io(), &output_paths).await;
                            result.failed_data_files_count += group_file_count;
                        }
                    }
                }
                Err(_) => {
                    result.failed_data_files_count += group_file_count;
                }
            }
        }

        Ok(result)
    }

    /// Best-effort cleanup of output files.
    async fn cleanup_files(file_io: &iceberg::io::FileIO, paths: &[String]) {
        for path in paths {
            let _ = file_io.delete(path).await;
        }
    }
}
