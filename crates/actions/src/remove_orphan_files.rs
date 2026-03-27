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

//! Remove orphan files action — lists all files under the table location,
//! compares against files referenced by any snapshot in the metadata,
//! and deletes unreferenced (orphan) files.

use std::collections::HashSet;
use std::time::SystemTime;

use futures::StreamExt;
use iceberg::table::Table;
use iceberg::{Error, ErrorKind, Result};

/// The property key for garbage-collection enabled flag.
const GC_ENABLED: &str = "gc.enabled";

/// Result of executing the remove orphan files action.
#[derive(Debug, Default, Clone)]
pub struct RemoveOrphanFilesResult {
    /// Locations of orphan files that were found (and deleted unless dry_run was set).
    pub orphan_file_locations: Vec<String>,
}

/// Action that removes orphan files from an Iceberg table's storage location.
///
/// Orphan files are files present in the table's storage directory that are
/// not referenced by any snapshot in the current table metadata. These can
/// accumulate from failed writes, interrupted compactions, or other incomplete
/// operations.
pub struct RemoveOrphanFiles<'a> {
    table: &'a Table,
    // TODO: implement mtime filtering when FileIO::list() returns metadata.
    // Currently FileIO::list() returns only file paths, not modification
    // timestamps. The `older_than` parameter is accepted for forward
    // compatibility but mtime-based filtering is NOT implemented. All
    // unreferenced files are treated as orphan candidates regardless of age.
    #[allow(dead_code)]
    older_than: SystemTime,
    location: Option<String>,
    dry_run: bool,
}

impl<'a> RemoveOrphanFiles<'a> {
    /// Create a new RemoveOrphanFiles action for the given table.
    ///
    /// Defaults:
    /// - `older_than`: now minus 3 days (not currently enforced; see struct docs)
    /// - `location`: table metadata location
    /// - `dry_run`: false
    pub fn new(table: &'a Table) -> Self {
        let three_days = std::time::Duration::from_secs(3 * 24 * 60 * 60);
        let default_older_than = SystemTime::now()
            .checked_sub(three_days)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        Self {
            table,
            older_than: default_older_than,
            location: None,
            dry_run: false,
        }
    }

    /// Set the timestamp threshold. Files modified after this time would be
    /// excluded from orphan detection.
    ///
    /// **Note:** This is a no-op today because `FileIO::list()` does not
    /// return modification timestamps. The parameter is accepted for forward
    /// compatibility.
    pub fn older_than(mut self, older_than: SystemTime) -> Self {
        self.older_than = older_than;
        self
    }

    /// Override the storage location to scan for orphan files.
    /// Defaults to `table.metadata().location()`.
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// When true, orphan files are reported but not deleted.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Execute the remove-orphan-files action.
    pub async fn execute(self) -> Result<RemoveOrphanFilesResult> {
        let metadata = self.table.metadata();

        // 1. Check gc.enabled — refuse if false
        if let Some(value) = metadata.properties().get(GC_ENABLED)
            && value.eq_ignore_ascii_case("false")
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Cannot remove orphan files: gc.enabled is set to false",
            ));
        }

        let file_io = self.table.file_io();

        // 2. Build "referenced files" set from ALL snapshots in metadata.
        let mut referenced_files: HashSet<String> = HashSet::new();

        // Current metadata file path
        if let Some(metadata_location) = self.table.metadata_location() {
            referenced_files.insert(metadata_location.to_string());
        }

        // All metadata log entries
        for log_entry in metadata.metadata_log() {
            referenced_files.insert(log_entry.metadata_file.clone());
        }

        // All statistics files
        for stat in metadata.statistics_iter() {
            referenced_files.insert(stat.statistics_path.clone());
        }

        // All partition statistics files
        for pstat in metadata.partition_statistics_iter() {
            referenced_files.insert(pstat.statistics_path.clone());
        }

        // Walk ALL snapshots to collect manifest lists, manifests, and data files
        for snapshot in metadata.snapshots() {
            // Manifest list path
            let manifest_list_path = snapshot.manifest_list().to_string();
            referenced_files.insert(manifest_list_path);

            // Load the manifest list to get manifest file paths
            let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
            for manifest_file in manifest_list.entries() {
                // Manifest file path
                referenced_files.insert(manifest_file.manifest_path.clone());

                // Load the manifest to get data/delete file paths
                let manifest_bytes = file_io
                    .new_input(&manifest_file.manifest_path)?
                    .read()
                    .await?;
                let (_meta, entries) =
                    iceberg::spec::Manifest::try_from_avro_bytes(&manifest_bytes)?;
                for entry in &entries {
                    referenced_files.insert(entry.data_file.file_path().to_string());
                }
            }
        }

        // 3. List all files under the location
        let scan_location = self
            .location
            .as_deref()
            .unwrap_or_else(|| metadata.location());

        let mut file_stream = file_io.list(scan_location).await?;

        // 4. Orphans = files in storage NOT in referenced set
        let mut orphan_locations: Vec<String> = Vec::new();
        while let Some(file_result) = file_stream.next().await {
            let file_path = file_result?;
            if !referenced_files.contains(&file_path) {
                orphan_locations.push(file_path);
            }
        }

        // 5. Delete orphans (unless dry_run)
        if !self.dry_run {
            for orphan in &orphan_locations {
                let _ = file_io.delete(orphan).await;
            }
        }

        Ok(RemoveOrphanFilesResult {
            orphan_file_locations: orphan_locations,
        })
    }
}
