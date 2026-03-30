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

//! Rewrite manifests action — merges small manifest files into larger ones
//! to speed up query planning. No data files are changed.
//!
//! Key correctness properties:
//! - Data and delete manifests are never merged across content types.
//! - Manifests with different partition spec IDs are never merged.
//! - Output shaping produces multiple output manifests when the merged
//!   result would exceed `target_size_bytes`.
//! - Commits through `RewriteManifestsAction` / `SnapshotProducer` for
//!   correct summary computation and sequence numbering.

use std::collections::HashMap;

use iceberg::spec::{ManifestContentType, ManifestFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, MergedManifestGroup, Transaction};
use iceberg::{Catalog, Error, ErrorKind, Result};

/// Default target manifest file size: 8 MiB.
const DEFAULT_TARGET_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Result of executing the rewrite manifests action.
#[derive(Debug, Default, Clone)]
pub struct RewriteManifestsResult {
    /// Number of manifests that were rewritten (removed).
    pub rewritten_manifests_count: u32,
    /// Number of new manifests that were added.
    pub added_manifests_count: u32,
}

/// Action that merges small manifest files into larger ones.
///
/// This action reads the manifest list of the current snapshot, identifies
/// manifests below the target size threshold, and rewrites them into fewer,
/// larger manifest files. A new snapshot is committed via the shared
/// `SnapshotProducer` through `RewriteManifestsAction`.
///
/// Manifests are grouped by (content_type, partition_spec_id) before merging
/// so that data and delete manifests are never mixed, and manifests with
/// different partition specs are never merged.
pub struct RewriteManifests<'a> {
    table: &'a Table,
    catalog: &'a dyn Catalog,
    target_size_bytes: u64,
}

impl<'a> RewriteManifests<'a> {
    /// Create a new RewriteManifests action for the given table and catalog.
    pub fn new(table: &'a Table, catalog: &'a dyn Catalog) -> Self {
        Self {
            table,
            catalog,
            target_size_bytes: DEFAULT_TARGET_SIZE_BYTES,
        }
    }

    /// Set the target manifest file size in bytes. Manifests smaller than
    /// this threshold are candidates for merging. Defaults to 8 MiB.
    pub fn target_size_bytes(mut self, target_size_bytes: u64) -> Self {
        self.target_size_bytes = target_size_bytes;
        self
    }

    /// Execute the rewrite manifests action.
    pub async fn execute(self) -> Result<RewriteManifestsResult> {
        let metadata = self.table.metadata();

        // If no current snapshot, nothing to do.
        let current_snapshot = match metadata.current_snapshot() {
            Some(s) => s,
            None => return Ok(RewriteManifestsResult::default()),
        };

        let file_io = self.table.file_io();

        // Load the current manifest list.
        let manifest_list = current_snapshot
            .load_manifest_list(file_io, metadata)
            .await?;

        // Group manifests by (content_type, spec_id). Within each group,
        // separate "small" candidates from "kept" manifests.
        type GroupKey = (ManifestContentType, i32);
        let mut small_by_group: HashMap<GroupKey, Vec<&ManifestFile>> = HashMap::new();
        let mut kept_manifests: Vec<ManifestFile> = Vec::new();

        for manifest_file in manifest_list.entries() {
            let key: GroupKey = (manifest_file.content, manifest_file.partition_spec_id);

            if (manifest_file.manifest_length as u64) < self.target_size_bytes {
                small_by_group.entry(key).or_default().push(manifest_file);
            } else {
                kept_manifests.push(manifest_file.clone());
            }
        }

        // For groups with 0 or 1 small manifests, nothing to merge — keep them.
        let mut groups_to_merge: Vec<(GroupKey, Vec<&ManifestFile>)> = Vec::new();
        for (key, manifests) in small_by_group {
            if manifests.len() <= 1 {
                for m in manifests {
                    kept_manifests.push(m.clone());
                }
            } else {
                groups_to_merge.push((key, manifests));
            }
        }

        if groups_to_merge.is_empty() {
            return Ok(RewriteManifestsResult::default());
        }

        let mut rewritten_count = 0u32;
        let mut added_count = 0u32;
        let mut merge_groups: Vec<MergedManifestGroup> = Vec::new();

        for ((content_type, spec_id), small_manifests) in &groups_to_merge {
            rewritten_count += small_manifests.len() as u32;

            let partition_spec = metadata
                .partition_spec_by_id(*spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Partition spec {} not found in table metadata", spec_id),
                    )
                })?
                .as_ref()
                .clone();

            // Read all entries from small manifests in this group
            let mut all_entries = Vec::new();
            for manifest_file in small_manifests {
                let manifest = manifest_file.load_manifest(file_io).await?;
                for entry in manifest.entries() {
                    if entry.is_alive() {
                        all_entries.push(entry.clone());
                    }
                }
            }

            if all_entries.is_empty() {
                continue;
            }

            // Output shaping: compute how many output manifests to produce.
            let total_manifest_size: u64 = small_manifests
                .iter()
                .map(|m| m.manifest_length as u64)
                .sum();
            let target_count = total_manifest_size
                .saturating_add(self.target_size_bytes.saturating_sub(1))
                .checked_div(self.target_size_bytes)
                .unwrap_or(1)
                .max(1) as usize;
            let entries_per_manifest = all_entries.len().div_ceil(target_count);

            // Split entries into groups for the action to write
            for chunk in all_entries.chunks(entries_per_manifest.max(1)) {
                merge_groups.push(MergedManifestGroup {
                    entries: chunk.to_vec(),
                    content_type: *content_type,
                    partition_spec: partition_spec.clone(),
                });
                added_count += 1;
            }
        }

        // Commit via Transaction + RewriteManifestsAction. The action writes
        // merged manifests inside SnapshotProducer where the snapshot_id is
        // known, ensuring correct manifest metadata.
        let tx = Transaction::new(self.table);
        let mut action = tx.rewrite_manifests().with_kept_manifests(kept_manifests);
        for group in merge_groups {
            action = action.add_merge_group(group);
        }
        let tx = action.apply(tx).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to apply rewrite manifests action: {e}"),
            )
        })?;
        tx.commit(self.catalog).await?;

        Ok(RewriteManifestsResult {
            rewritten_manifests_count: rewritten_count,
            added_manifests_count: added_count,
        })
    }
}
