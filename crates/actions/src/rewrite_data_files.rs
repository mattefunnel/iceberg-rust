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

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use iceberg::spec::{DataContentType, DataFile, ManifestContentType, ManifestStatus, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result};

/// Default target file size: 512 MiB.
const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;

/// Default minimum file size (75% of target): 384 MiB.
const DEFAULT_MIN_FILE_SIZE_BYTES: u64 = 384 * 1024 * 1024;

/// Default maximum file size (180% of target): ~922 MiB.
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 921 * 1024 * 1024;

/// Default minimum number of input files in a partition to trigger compaction
/// even when all files are within the size range.
const DEFAULT_MIN_INPUT_FILES: usize = 5;

/// Default maximum size of a single file group: 100 GiB.
const DEFAULT_MAX_FILE_GROUP_SIZE_BYTES: u64 = 100 * 1024 * 1024 * 1024;

/// Pluggable trait for rewriting data files. Any compute engine (DataFusion,
/// Spark, etc.) implements this to provide the actual file compaction logic.
#[async_trait]
pub trait FileRewriter: Send + Sync {
    /// Rewrite a group of data files, producing a new set of (typically fewer,
    /// larger) data files.
    async fn rewrite_files(&self, table: &Table, group: Vec<DataFile>) -> Result<Vec<DataFile>>;
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
    #[allow(dead_code)]
    use_starting_sequence_number: bool,
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
            use_starting_sequence_number: true,
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

    /// Set the minimum number of files in a partition to trigger compaction
    /// of all files in that partition, even when each file individually
    /// falls within the size range. Defaults to 5.
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

    /// Whether to use the starting sequence number for new data files.
    /// Defaults to `true`.
    pub fn use_starting_sequence_number(mut self, val: bool) -> Self {
        self.use_starting_sequence_number = val;
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

        let file_io = self.table.file_io();

        // 2. Walk all DATA manifests to collect live data files.
        let manifest_list = current_snapshot
            .load_manifest_list(file_io, metadata)
            .await?;

        let mut partition_files: HashMap<Struct, Vec<DataFile>> = HashMap::new();

        // Track file paths we have already processed. Manifests in the
        // manifest list are ordered newest-first, so we process delete
        // entries before their corresponding older alive entries. A file
        // path that first appears as Deleted must not be counted as alive
        // from an older manifest.
        let mut seen_paths: HashSet<String> = HashSet::new();

        for manifest_file in manifest_list.entries() {
            // Skip delete manifests — we only care about data files.
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                if entry.content_type() != DataContentType::Data {
                    continue;
                }
                let path = entry.file_path().to_string();

                // If we have already seen this file path (from a newer
                // manifest), skip it regardless of status.
                if !seen_paths.insert(path) {
                    continue;
                }

                // Only collect alive entries (Added or Existing).
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }

                let partition = entry.data_file().partition().clone();
                partition_files
                    .entry(partition)
                    .or_default()
                    .push(entry.data_file().clone());
            }
        }

        // 3. For each partition group, select candidate files.
        let mut file_groups: Vec<Vec<DataFile>> = Vec::new();

        for files in partition_files.values() {
            let candidates = self.select_candidates(files);
            if candidates.is_empty() {
                continue;
            }

            // 4. Bin-pack candidates into groups not exceeding max_file_group_size_bytes.
            let groups = self.bin_pack(candidates);
            file_groups.extend(groups);
        }

        if file_groups.is_empty() {
            return Ok(RewriteDataFilesResult::default());
        }

        // 5. Rewrite each group and commit.
        if self.partial_progress {
            self.execute_partial_progress(file_groups).await
        } else {
            self.execute_all_at_once(file_groups).await
        }
    }

    /// Select candidate files from a single partition group.
    ///
    /// A file is a candidate if it is too small or too large. Additionally,
    /// if the partition has at least `min_input_files` files, ALL files in
    /// that partition are candidates (the bin-pack heuristic for many
    /// individually-acceptable files that are collectively suboptimal).
    fn select_candidates<'f>(&self, files: &'f [DataFile]) -> Vec<&'f DataFile> {
        // If there are enough files in the partition, all are candidates.
        if files.len() >= self.min_input_files {
            return files.iter().collect();
        }

        // Otherwise, select only undersized or oversized files.
        files
            .iter()
            .filter(|f| {
                f.file_size_in_bytes() < self.min_file_size_bytes
                    || f.file_size_in_bytes() > self.max_file_size_bytes
            })
            .collect()
    }

    /// Bin-pack candidate files into groups, each not exceeding
    /// `max_file_group_size_bytes` in total size.
    fn bin_pack(&self, candidates: Vec<&DataFile>) -> Vec<Vec<DataFile>> {
        let mut groups: Vec<Vec<DataFile>> = Vec::new();
        let mut current_group: Vec<DataFile> = Vec::new();
        let mut current_size: u64 = 0;

        for file in candidates {
            let file_size = file.file_size_in_bytes();

            if !current_group.is_empty()
                && current_size + file_size > self.max_file_group_size_bytes
            {
                groups.push(std::mem::take(&mut current_group));
                current_size = 0;
            }

            current_group.push(file.clone());
            current_size += file_size;
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }

    /// Execute all groups as a single atomic commit.
    async fn execute_all_at_once(
        &self,
        file_groups: Vec<Vec<DataFile>>,
    ) -> Result<RewriteDataFilesResult> {
        let mut all_files_to_delete: Vec<DataFile> = Vec::new();
        let mut all_files_to_add: Vec<DataFile> = Vec::new();
        let mut rewritten_bytes: u64 = 0;

        for group in file_groups {
            for f in &group {
                rewritten_bytes += f.file_size_in_bytes();
            }
            let new_files = self
                .rewriter
                .rewrite_files(self.table, group.clone())
                .await?;
            all_files_to_delete.extend(group);
            all_files_to_add.extend(new_files);
        }

        let rewritten_count = all_files_to_delete.len() as u32;
        let added_count = all_files_to_add.len() as u32;

        // Commit via Transaction + RewriteFilesAction.
        let tx = Transaction::new(self.table);
        let action = tx
            .rewrite_files()
            .delete_files(all_files_to_delete)
            .add_files(all_files_to_add);
        let tx = action.apply(tx).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to apply rewrite files action: {e}"),
            )
        })?;
        tx.commit(self.catalog).await?;

        Ok(RewriteDataFilesResult {
            rewritten_data_files_count: rewritten_count,
            added_data_files_count: added_count,
            rewritten_bytes_count: rewritten_bytes,
            failed_data_files_count: 0,
            removed_delete_files_count: 0,
        })
    }

    /// Execute each group as an independent commit, allowing partial
    /// progress even when some groups fail.
    async fn execute_partial_progress(
        &self,
        file_groups: Vec<Vec<DataFile>>,
    ) -> Result<RewriteDataFilesResult> {
        let mut result = RewriteDataFilesResult::default();

        for group in file_groups {
            let group_file_count = group.len() as u32;
            let group_bytes: u64 = group.iter().map(|f| f.file_size_in_bytes()).sum();

            match self.rewriter.rewrite_files(self.table, group.clone()).await {
                Ok(new_files) => {
                    let added_count = new_files.len() as u32;

                    // Reload table to get latest metadata for each partial commit.
                    let table = self.catalog.load_table(self.table.identifier()).await?;

                    let tx = Transaction::new(&table);
                    let action = tx.rewrite_files().delete_files(group).add_files(new_files);
                    let tx = match action.apply(tx) {
                        Ok(tx) => tx,
                        Err(_) => {
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
}
