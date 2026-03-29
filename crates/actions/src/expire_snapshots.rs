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

//! Expire snapshots action — removes old snapshots from table metadata
//! and deletes files (data files, manifests, manifest lists) exclusively
//! referenced by the expired snapshots.

use std::collections::HashSet;
use std::time::SystemTime;

use iceberg::spec::DataContentType;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, Result, TableCommit, TableRequirement, TableUpdate};

/// The property key for garbage-collection enabled flag.
const GC_ENABLED: &str = "gc.enabled";

/// Result of executing the expire snapshots action.
#[derive(Debug, Default, Clone)]
pub struct ExpireSnapshotsResult {
    /// Number of data files deleted.
    pub deleted_data_files_count: u64,
    /// Number of equality delete files deleted.
    pub deleted_equality_delete_files_count: u64,
    /// Number of position delete files deleted.
    pub deleted_position_delete_files_count: u64,
    /// Number of manifest files deleted.
    pub deleted_manifest_files_count: u64,
    /// Number of manifest list files deleted.
    pub deleted_manifest_lists_count: u64,
    /// Number of statistics files deleted.
    pub deleted_statistics_files_count: u64,
}

/// Action that expires (removes) old snapshots from an Iceberg table.
///
/// After computing which snapshots to expire, this action:
/// 1. Commits a `RemoveSnapshots` metadata update via the catalog
/// 2. Walks expired snapshot manifest lists to find orphaned files
/// 3. Deletes data files, manifests, and manifest lists exclusively
///    referenced by expired snapshots
pub struct ExpireSnapshots<'a> {
    table: &'a Table,
    catalog: &'a dyn Catalog,
    older_than: Option<SystemTime>,
    retain_last: usize,
    snapshot_ids: Vec<i64>,
    clean_expired_metadata: bool,
}

impl<'a> ExpireSnapshots<'a> {
    /// Create a new ExpireSnapshots action for the given table and catalog.
    pub fn new(table: &'a Table, catalog: &'a dyn Catalog) -> Self {
        Self {
            table,
            catalog,
            older_than: None,
            retain_last: 1,
            snapshot_ids: Vec::new(),
            clean_expired_metadata: true,
        }
    }

    /// Set the timestamp before which snapshots should be expired.
    /// Snapshots older than this time are candidates for expiration.
    pub fn older_than(mut self, older_than: SystemTime) -> Self {
        self.older_than = Some(older_than);
        self
    }

    /// Set the minimum number of snapshots to retain on the main branch.
    /// Defaults to 1.
    pub fn retain_last(mut self, n: usize) -> Self {
        self.retain_last = n;
        self
    }

    /// Explicitly request expiration of a specific snapshot ID.
    /// This snapshot will be expired regardless of `older_than` or `retain_last`.
    pub fn expire_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_ids.push(snapshot_id);
        self
    }

    /// Whether to delete orphaned data files, manifests, manifest lists,
    /// and statistics files after removing snapshots from metadata.
    /// Defaults to `true`.
    pub fn clean_expired_metadata(mut self, clean: bool) -> Self {
        self.clean_expired_metadata = clean;
        self
    }

    /// Execute the expire-snapshots action.
    pub async fn execute(self) -> Result<ExpireSnapshotsResult> {
        let metadata = self.table.metadata();

        // 1. Check gc.enabled — refuse if false
        if let Some(value) = metadata.properties().get(GC_ENABLED)
            && value.eq_ignore_ascii_case("false")
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Cannot expire snapshots: gc.enabled is set to false",
            ));
        }

        // If table has no snapshots, nothing to do
        if metadata.snapshots().len() == 0 {
            return Ok(ExpireSnapshotsResult::default());
        }

        // 2. Compute the set of retained snapshot IDs
        //
        // NOTE: Branch/tag-aware retention is not yet implemented.
        // Current behavior: retain ref heads, retain_last ancestors of main,
        // and snapshots newer than older_than.
        // Missing vs Java: per-branch minSnapshotsToKeep, per-branch
        // maxSnapshotAgeMs, maxRefAgeMs for ref expiration, and independent
        // branch history traversal.
        // See: plans/address-initial-review-claude-no-branches/07-out-of-scope-branches-tags.md

        // 2a. Collect all snapshot IDs referenced by branches/tags
        let mut retained_ids: HashSet<i64> = HashSet::new();
        for reference in metadata.refs().values() {
            retained_ids.insert(reference.snapshot_id);
        }

        // 2b. retain_last most recent ancestors of the main branch
        if let Some(current_snapshot_id) = metadata.current_snapshot_id() {
            let mut count = 0usize;
            let mut cursor = Some(current_snapshot_id);
            while let Some(sid) = cursor {
                if count >= self.retain_last {
                    break;
                }
                retained_ids.insert(sid);
                count += 1;
                cursor = metadata
                    .snapshot_by_id(sid)
                    .and_then(|s| s.parent_snapshot_id());
            }
        }

        // 2c. All snapshots newer than `older_than`
        if let Some(older_than) = self.older_than {
            let older_than_ms = older_than
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            for snapshot in metadata.snapshots() {
                if snapshot.timestamp_ms() >= older_than_ms {
                    retained_ids.insert(snapshot.snapshot_id());
                }
            }
        }

        // 2d. Explicitly requested snapshot IDs are always expired
        // TODO: When branch/tag support is added, validate that explicitly
        // targeted snapshot IDs are not the head of any live branch or tag.
        // Java's RemoveSnapshots throws if this is attempted.
        // See: plans/address-initial-review-claude-no-branches/07-out-of-scope-branches-tags.md
        for &sid in &self.snapshot_ids {
            retained_ids.remove(&sid);
        }

        // Compute the IDs to expire
        let expired_ids: Vec<i64> = metadata
            .snapshots()
            .filter(|s| !retained_ids.contains(&s.snapshot_id()))
            .map(|s| s.snapshot_id())
            .collect();

        if expired_ids.is_empty() {
            return Ok(ExpireSnapshotsResult::default());
        }

        let expired_id_set: HashSet<i64> = expired_ids.iter().copied().collect();

        // 3. Commit RemoveSnapshots + RemoveStatistics + RemovePartitionStatistics
        let mut updates: Vec<TableUpdate> = vec![TableUpdate::RemoveSnapshots {
            snapshot_ids: expired_ids.clone(),
        }];

        // Remove statistics files for expired snapshots
        for stat in metadata.statistics_iter() {
            if expired_id_set.contains(&stat.snapshot_id) {
                updates.push(TableUpdate::RemoveStatistics {
                    snapshot_id: stat.snapshot_id,
                });
            }
        }

        // Remove partition statistics for expired snapshots
        for pstat in metadata.partition_statistics_iter() {
            if expired_id_set.contains(&pstat.snapshot_id) {
                updates.push(TableUpdate::RemovePartitionStatistics {
                    snapshot_id: pstat.snapshot_id,
                });
            }
        }

        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().clone())
            .updates(updates)
            .requirements(vec![TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            }])
            .build();

        self.catalog.update_table(table_commit).await?;

        // 4. If clean_expired_metadata, walk expired snapshots and delete orphaned files
        if !self.clean_expired_metadata {
            return Ok(ExpireSnapshotsResult::default());
        }

        let file_io = self.table.file_io();
        let mut result = ExpireSnapshotsResult::default();

        // ── Pass 1: Discovery (before any deletes) ──────────────────────
        //
        // Build the set of live files from retained snapshots. This now
        // includes BOTH data and delete manifests (the previous code only
        // walked DATA manifests, so delete files from retained snapshots
        // could be incorrectly identified as candidates for deletion).

        let mut live_data_files: HashSet<String> = HashSet::new();
        let mut live_manifest_paths: HashSet<String> = HashSet::new();
        let mut live_manifest_list_paths: HashSet<String> = HashSet::new();

        for snapshot in metadata.snapshots() {
            if !retained_ids.contains(&snapshot.snapshot_id()) {
                continue;
            }
            live_manifest_list_paths.insert(snapshot.manifest_list().to_string());
            let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
            for manifest_file in manifest_list.entries() {
                // Include ALL manifests (data AND delete) in the live set
                live_manifest_paths.insert(manifest_file.manifest_path.clone());
                let manifest_bytes = file_io
                    .new_input(&manifest_file.manifest_path)?
                    .read()
                    .await?;
                let (_meta, entries) =
                    iceberg::spec::Manifest::try_from_avro_bytes(&manifest_bytes)?;
                for entry in &entries {
                    if entry.is_alive() {
                        live_data_files.insert(entry.data_file.file_path().to_string());
                    }
                }
            }
        }

        // Discover candidate files from expired snapshots. We read all
        // manifests and manifest lists BEFORE deleting anything. This fixes
        // the prior bug where a manifest list was deleted and then the code
        // tried to read it to discover what else to clean up.
        struct CandidateFile {
            path: String,
            content_type: DataContentType,
        }

        let mut candidate_data_files: Vec<CandidateFile> = Vec::new();
        let mut candidate_manifests: HashSet<String> = HashSet::new();
        let mut candidate_manifest_lists: HashSet<String> = HashSet::new();

        for snapshot in metadata.snapshots() {
            if !expired_id_set.contains(&snapshot.snapshot_id()) {
                continue;
            }

            // Collect manifest list path
            let manifest_list_path = snapshot.manifest_list().to_string();
            candidate_manifest_lists.insert(manifest_list_path);

            // Load the manifest list to discover manifests and data/delete files
            let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;

            for manifest_file in manifest_list.entries() {
                // Include ALL manifests (data AND delete) in candidates
                if candidate_manifests.insert(manifest_file.manifest_path.clone()) {
                    let manifest_bytes = file_io
                        .new_input(&manifest_file.manifest_path)?
                        .read()
                        .await?;
                    let (_meta, entries) =
                        iceberg::spec::Manifest::try_from_avro_bytes(&manifest_bytes)?;
                    for entry in &entries {
                        candidate_data_files.push(CandidateFile {
                            path: entry.data_file.file_path().to_string(),
                            content_type: entry.data_file.content_type(),
                        });
                    }
                }
            }
        }

        // ── Between passes: Subtract live files ─────────────────────────

        // Remove any path that appears in the live sets
        candidate_data_files.retain(|f| !live_data_files.contains(&f.path));
        candidate_manifests.retain(|p| !live_manifest_paths.contains(p));
        candidate_manifest_lists.retain(|p| !live_manifest_list_paths.contains(p));

        // Deduplicate data/delete file paths (a file can appear in multiple
        // expired snapshots)
        let mut seen_data_paths: HashSet<String> = HashSet::new();
        candidate_data_files.retain(|f| seen_data_paths.insert(f.path.clone()));

        // ── Pass 2: Deletion (leaf-to-root ordering) ────────────────────
        //
        // Delete order: data/delete files → manifests → manifest lists →
        // statistics. A crash at any point leaves only already-unreferenced
        // files behind, not dangling references to deleted files.

        // 1. Data files and delete files (leaf nodes)
        for file in &candidate_data_files {
            let _ = file_io.delete(&file.path).await;
            match file.content_type {
                DataContentType::Data => result.deleted_data_files_count += 1,
                DataContentType::PositionDeletes => {
                    result.deleted_position_delete_files_count += 1;
                }
                DataContentType::EqualityDeletes => {
                    result.deleted_equality_delete_files_count += 1;
                }
            }
        }

        // 2. Manifests (reference data files, but data files already deleted)
        for manifest_path in &candidate_manifests {
            let _ = file_io.delete(manifest_path).await;
            result.deleted_manifest_files_count += 1;
        }

        // 3. Manifest lists (reference manifests, but manifests already deleted)
        for manifest_list_path in &candidate_manifest_lists {
            let _ = file_io.delete(manifest_list_path).await;
            result.deleted_manifest_lists_count += 1;
        }

        // 4. Statistics and partition statistics files
        for stat in metadata.statistics_iter() {
            if expired_id_set.contains(&stat.snapshot_id) {
                let _ = file_io.delete(&stat.statistics_path).await;
                result.deleted_statistics_files_count += 1;
            }
        }
        for pstat in metadata.partition_statistics_iter() {
            if expired_id_set.contains(&pstat.snapshot_id) {
                let _ = file_io.delete(&pstat.statistics_path).await;
                result.deleted_statistics_files_count += 1;
            }
        }

        Ok(result)
    }
}
