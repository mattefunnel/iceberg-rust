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

use std::collections::{HashMap, HashSet};
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

/// Behavior when a file's URI scheme/authority does not match any known
/// equivalence configured via [`RemoveOrphanFiles::equal_schemes`] or
/// [`RemoveOrphanFiles::equal_authorities`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixMismatchMode {
    /// Fail with an error (default). The safest option because it surfaces
    /// configuration problems rather than silently skipping or deleting.
    Error,
    /// Skip the file — do not treat it as an orphan.
    Ignore,
}

/// Action that removes orphan files from an Iceberg table's storage location.
///
/// Orphan files are files present in the table's storage directory that are
/// not referenced by any snapshot in the current table metadata. These can
/// accumulate from failed writes, interrupted compactions, or other incomplete
/// operations.
pub struct RemoveOrphanFiles<'a> {
    table: &'a Table,
    older_than: SystemTime,
    location: Option<String>,
    dry_run: bool,
    equal_schemes: HashMap<String, String>,
    equal_authorities: HashMap<String, String>,
    prefix_mismatch_mode: PrefixMismatchMode,
}

impl<'a> RemoveOrphanFiles<'a> {
    /// Create a new RemoveOrphanFiles action for the given table.
    ///
    /// Defaults:
    /// - `older_than`: now minus 3 days
    /// - `location`: table metadata location
    /// - `dry_run`: false
    /// - `prefix_mismatch_mode`: `Error`
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
            equal_schemes: HashMap::new(),
            equal_authorities: HashMap::new(),
            prefix_mismatch_mode: PrefixMismatchMode::Error,
        }
    }

    /// Set the timestamp threshold. Files modified after this time are
    /// excluded from orphan detection. When modification time is not
    /// available from the storage backend, the file is still treated as
    /// an orphan candidate (the guard only applies when mtime is known).
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

    /// Declare that two URI schemes should be treated as equivalent.
    /// For example, `equal_schemes("s3a", "s3")` means `s3a://bucket/path`
    /// and `s3://bucket/path` refer to the same file.
    pub fn equal_schemes(
        mut self,
        scheme_alias: impl Into<String>,
        canonical: impl Into<String>,
    ) -> Self {
        self.equal_schemes.insert(
            scheme_alias.into().to_lowercase(),
            canonical.into().to_lowercase(),
        );
        self
    }

    /// Declare that two URI authorities should be treated as equivalent.
    pub fn equal_authorities(
        mut self,
        authority_alias: impl Into<String>,
        canonical: impl Into<String>,
    ) -> Self {
        self.equal_authorities
            .insert(authority_alias.into(), canonical.into());
        self
    }

    /// Set the behavior when a file's URI scheme/authority does not match
    /// any configured equivalence. Defaults to [`PrefixMismatchMode::Error`].
    pub fn prefix_mismatch_mode(mut self, mode: PrefixMismatchMode) -> Self {
        self.prefix_mismatch_mode = mode;
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
        //    Normalize all paths for consistent comparison.
        let mut referenced_files: HashSet<String> = HashSet::new();

        // Current metadata file path
        if let Some(metadata_location) = self.table.metadata_location() {
            referenced_files.insert(normalize_uri(metadata_location));
        }

        // All metadata log entries
        for log_entry in metadata.metadata_log() {
            referenced_files.insert(normalize_uri(&log_entry.metadata_file));
        }

        // All statistics files
        for stat in metadata.statistics_iter() {
            referenced_files.insert(normalize_uri(&stat.statistics_path));
        }

        // All partition statistics files
        for pstat in metadata.partition_statistics_iter() {
            referenced_files.insert(normalize_uri(&pstat.statistics_path));
        }

        // Walk ALL snapshots to collect manifest lists, manifests, and data files
        for snapshot in metadata.snapshots() {
            // Manifest list path
            referenced_files.insert(normalize_uri(snapshot.manifest_list()));

            // Load the manifest list to get manifest file paths
            let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
            for manifest_file in manifest_list.entries() {
                // Manifest file path
                referenced_files.insert(normalize_uri(&manifest_file.manifest_path));

                // Load the manifest to get data/delete file paths
                let manifest_bytes = file_io
                    .new_input(&manifest_file.manifest_path)?
                    .read()
                    .await?;
                let (_meta, entries) =
                    iceberg::spec::Manifest::try_from_avro_bytes(&manifest_bytes)?;
                for entry in &entries {
                    referenced_files.insert(normalize_uri(entry.data_file.file_path()));
                }
            }
        }

        // Apply scheme equivalences to the referenced set
        if !self.equal_schemes.is_empty() || !self.equal_authorities.is_empty() {
            let original: Vec<String> = referenced_files.iter().cloned().collect();
            for path in &original {
                let canonical = self.apply_scheme_and_authority_equivalences(path);
                if canonical != *path {
                    referenced_files.insert(canonical);
                }
            }
        }

        // 3. List all files under the location using list_with_metadata
        let scan_location = self
            .location
            .as_deref()
            .unwrap_or_else(|| metadata.location());

        let mut file_stream = file_io.list_with_metadata(scan_location).await?;

        // Extract the scheme://authority prefix from the table location to
        // detect when a listed file uses a different prefix.
        let table_prefix = extract_scheme_authority(scan_location);

        // 4. Orphans = files in storage NOT in referenced set, filtered by mtime
        let mut orphan_locations: Vec<String> = Vec::new();
        while let Some(file_result) = file_stream.next().await {
            let file_entry = file_result?;
            let normalized = normalize_uri(&file_entry.path);

            // Apply scheme/authority equivalences for comparison
            let canonical = if !self.equal_schemes.is_empty() || !self.equal_authorities.is_empty()
            {
                self.apply_scheme_and_authority_equivalences(&normalized)
            } else {
                normalized.clone()
            };

            if referenced_files.contains(&canonical) || referenced_files.contains(&normalized) {
                continue;
            }

            // Prefix mismatch check: if this file's scheme://authority differs
            // from the table location's prefix and no equivalence resolved it,
            // consult prefix_mismatch_mode.
            let file_prefix = extract_scheme_authority(&normalized);
            if let Some(ref table_pfx) = table_prefix
                && let Some(ref file_pfx) = file_prefix
                && file_pfx != table_pfx
            {
                // Check if the canonical form matches the table prefix
                let canonical_prefix = extract_scheme_authority(&canonical);
                let prefix_matches = canonical_prefix.as_ref() == Some(table_pfx);
                if !prefix_matches {
                    match self.prefix_mismatch_mode {
                        PrefixMismatchMode::Error => {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Prefix mismatch: file '{}' has scheme/authority '{}' \
                                     which differs from the table location '{}'. \
                                     Configure equal_schemes() or equal_authorities() \
                                     to declare equivalences, or set \
                                     prefix_mismatch_mode(Ignore) to skip such files.",
                                    file_entry.path, file_pfx, table_pfx,
                                ),
                            ));
                        }
                        PrefixMismatchMode::Ignore => {
                            continue; // Skip this file
                        }
                    }
                }
            }

            // Mtime filtering: when mtime is available, skip files newer than
            // `older_than`. When mtime is NOT available (the current default
            // since most Storage backends don't return it from list), the file
            // is treated as an orphan candidate — preserving the previous
            // behavior where all unreferenced files were candidates.
            if let Some(last_modified) = file_entry.last_modified
                && last_modified >= self.older_than
            {
                continue; // Too recent, skip
            }

            orphan_locations.push(file_entry.path);
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

    /// Apply configured scheme and authority equivalences to a URI.
    fn apply_scheme_and_authority_equivalences(&self, uri: &str) -> String {
        let Some((scheme_authority, rest)) = uri.split_once("://") else {
            return uri.to_string();
        };

        let scheme_lower = scheme_authority.to_lowercase();
        let canonical_scheme = self
            .equal_schemes
            .get(&scheme_lower)
            .map(|s| s.as_str())
            .unwrap_or(&scheme_lower);

        // Split rest into authority and path
        let (authority, path) = if let Some(idx) = rest.find('/') {
            (&rest[..idx], &rest[idx..])
        } else {
            (rest, "")
        };

        let canonical_authority = self
            .equal_authorities
            .get(authority)
            .map(|s| s.as_str())
            .unwrap_or(authority);

        format!("{canonical_scheme}://{canonical_authority}{path}")
    }
}

/// Extract the "scheme://authority" prefix from a URI, if present.
/// Returns `None` for bare paths like `/tmp/foo`.
fn extract_scheme_authority(uri: &str) -> Option<String> {
    let (scheme, rest) = uri.split_once("://")?;
    let authority = rest.split('/').next().unwrap_or("");
    Some(format!("{}://{}", scheme.to_lowercase(), authority))
}

/// Normalize a URI for consistent comparison: lowercase scheme, strip trailing
/// slashes from the path component.
fn normalize_uri(uri: &str) -> String {
    if let Some((scheme, rest)) = uri.split_once("://") {
        let scheme_lower = scheme.to_lowercase();
        let rest_trimmed = rest.trim_end_matches('/');
        format!("{scheme_lower}://{rest_trimmed}")
    } else {
        uri.trim_end_matches('/').to_string()
    }
}
