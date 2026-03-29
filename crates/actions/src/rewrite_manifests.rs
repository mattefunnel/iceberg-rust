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

use std::collections::HashMap;
use std::time::SystemTime;

use iceberg::spec::{
    FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, Result, TableCommit, TableRequirement, TableUpdate};
use uuid::Uuid;

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
/// larger manifest files. A new snapshot is committed with the merged
/// manifest list.
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

        // Generate a unique snapshot ID for the new snapshot.
        let snapshot_id = generate_unique_snapshot_id(self.table);
        let commit_uuid = Uuid::now_v7();
        let schema = metadata.current_schema().clone();
        let mut manifest_counter = 0u64;
        let mut rewritten_count = 0u32;
        let mut added_count = 0u32;
        let mut new_merged_manifests: Vec<ManifestFile> = Vec::new();

        for ((content_type, spec_id), small_manifests) in &groups_to_merge {
            rewritten_count += small_manifests.len() as u32;

            // Look up the partition spec for this group
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

            // Write output manifests, splitting entries across them
            for chunk in all_entries.chunks(entries_per_manifest.max(1)) {
                let manifest_path = format!(
                    "{}/metadata/{}-m{}.avro",
                    metadata.location(),
                    commit_uuid,
                    manifest_counter
                );
                manifest_counter += 1;

                let output_file = file_io.new_output(&manifest_path)?;
                let builder = ManifestWriterBuilder::new(
                    output_file,
                    Some(snapshot_id),
                    None,
                    schema.clone(),
                    partition_spec.clone(),
                );

                let mut writer = match metadata.format_version() {
                    FormatVersion::V1 => builder.build_v1(),
                    FormatVersion::V2 => match content_type {
                        ManifestContentType::Data => builder.build_v2_data(),
                        ManifestContentType::Deletes => builder.build_v2_deletes(),
                    },
                    FormatVersion::V3 => match content_type {
                        ManifestContentType::Data => builder.build_v3_data(),
                        ManifestContentType::Deletes => builder.build_v3_deletes(),
                    },
                };

                for entry in chunk {
                    let entry_snapshot_id = entry.snapshot_id().ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Manifest entry missing snapshot_id")
                    })?;
                    let sequence_number = entry.sequence_number().unwrap_or(0);
                    let file_sequence_number = entry.file_sequence_number;
                    writer.add_existing_file(
                        entry.data_file().clone(),
                        entry_snapshot_id,
                        sequence_number,
                        file_sequence_number,
                    )?;
                }

                let merged_manifest = writer.write_manifest_file().await?;
                new_merged_manifests.push(merged_manifest);
                added_count += 1;
            }
        }

        // Build the final manifest list: kept manifests + new merged manifests
        let mut final_manifests = kept_manifests;
        final_manifests.extend(new_merged_manifests);

        // Write the new manifest list
        let next_seq_num = metadata.next_sequence_number();
        let manifest_list_path = format!(
            "{}/metadata/snap-{}-0-{}.avro",
            metadata.location(),
            snapshot_id,
            commit_uuid
        );
        let manifest_list_output = file_io.new_output(&manifest_list_path)?;

        let mut manifest_list_writer = match metadata.format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                manifest_list_output,
                snapshot_id,
                metadata.current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                manifest_list_output,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => {
                let first_row_id = Some(metadata.next_row_id());
                ManifestListWriter::v3(
                    manifest_list_output,
                    snapshot_id,
                    metadata.current_snapshot_id(),
                    next_seq_num,
                    first_row_id,
                )
            }
        };

        manifest_list_writer.add_manifests(final_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        // Build the new snapshot — inherit summary from parent since no data
        // files changed.
        let commit_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let summary = Summary {
            operation: Operation::Replace,
            additional_properties: Default::default(),
        };

        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(metadata.current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        // Commit via catalog
        // TODO: Migrate to shared snapshot-production commit path
        // (RewriteManifestsAction / SnapshotProducer) once the producer
        // supports pre-written manifests with correct snapshot_id assignment.
        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().clone())
            .updates(vec![
                TableUpdate::AddSnapshot {
                    snapshot: new_snapshot,
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ])
            .requirements(vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: metadata.current_snapshot_id(),
                },
            ])
            .build();

        self.catalog.update_table(table_commit).await?;

        Ok(RewriteManifestsResult {
            rewritten_manifests_count: rewritten_count,
            added_manifests_count: added_count,
        })
    }
}

/// Generate a unique snapshot ID that does not collide with existing snapshots.
fn generate_unique_snapshot_id(table: &Table) -> i64 {
    let generate_random_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        if snapshot_id < 0 {
            -snapshot_id
        } else {
            snapshot_id
        }
    };
    let mut snapshot_id = generate_random_id();
    while table
        .metadata()
        .snapshots()
        .any(|s| s.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_random_id();
    }
    snapshot_id
}
