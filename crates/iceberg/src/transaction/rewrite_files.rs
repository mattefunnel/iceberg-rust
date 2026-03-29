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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation, Struct};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// `RewriteFilesAction` is a transaction action that atomically replaces a set
/// of data files with a new set. This is the low-level primitive used by
/// compaction (RewriteDataFiles): it deletes the old files and adds the new
/// files in a single atomic snapshot.
///
/// **Critical safety property:** if any file-to-delete is not present in the
/// current snapshot, the commit fails immediately. This prevents silent data
/// loss from double-deleting files.
pub struct RewriteFilesAction {
    files_to_delete: Vec<DataFile>,
    files_to_add: Vec<DataFile>,
    starting_snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            files_to_delete: Vec::new(),
            files_to_add: Vec::new(),
            starting_snapshot_id: None,
            data_sequence_number: None,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
        }
    }

    /// Set the data files that should be removed from the current snapshot.
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files_to_delete.extend(files);
        self
    }

    /// Set the data files that should be added in place of the deleted files.
    pub fn add_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files_to_add.extend(files);
        self
    }

    /// Set the snapshot id to validate against. If set, the commit will check
    /// that the files to delete are present in this specific snapshot.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }

    /// Set a specific data sequence number for the rewrite operation.
    pub fn data_sequence_number(mut self, seq_num: i64) -> Self {
        self.data_sequence_number = Some(seq_num);
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.files_to_delete.is_empty() && self.files_to_add.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite files action requires at least one file to delete or add",
            ));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            None,
            self.snapshot_properties.clone(),
            self.files_to_add.clone(),
        );

        // Validate added files
        if !self.files_to_add.is_empty() {
            snapshot_producer.validate_added_data_files()?;
        }

        let operation = RewriteFilesOperation {
            files_to_delete: self.files_to_delete.clone(),
            starting_snapshot_id: self.starting_snapshot_id,
            data_sequence_number: self.data_sequence_number,
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

struct RewriteFilesOperation {
    files_to_delete: Vec<DataFile>,
    /// If set, the snapshot from which the rewrite was planned. Used to
    /// validate that the table has not changed incompatibly since planning.
    starting_snapshot_id: Option<i64>,
    /// If set, validate that no new delete files have been added since this
    /// sequence number for partitions containing files being rewritten.
    /// Mirrors Java's `validateNoNewDeletesForDataFiles()`.
    data_sequence_number: Option<i64>,
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            if !self.files_to_delete.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot delete files from a table with no current snapshot",
                ));
            }
            return Ok(vec![]);
        };

        // Validate that starting_snapshot_id is an ancestor of the current
        // snapshot. If the table was rolled back or the snapshot was expired,
        // the rewrite is operating on stale data.
        if let Some(starting_id) = self.starting_snapshot_id {
            let mut found = false;
            let mut cursor = Some(snapshot.snapshot_id());
            while let Some(sid) = cursor {
                if sid == starting_id {
                    found = true;
                    break;
                }
                cursor = snapshot_produce
                    .table
                    .metadata()
                    .snapshot_by_id(sid)
                    .and_then(|s| s.parent_snapshot_id());
            }
            if !found {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot commit rewrite: starting snapshot {} is not an ancestor \
                         of the current snapshot {}. The table may have been rolled back \
                         or the snapshot expired.",
                        starting_id,
                        snapshot.snapshot_id(),
                    ),
                ));
            }
        }

        let delete_paths: HashSet<&str> =
            self.files_to_delete.iter().map(|f| f.file_path()).collect();

        if delete_paths.is_empty() {
            return Ok(vec![]);
        }

        let mut found_paths: HashSet<String> = HashSet::new();
        let mut delete_entries = Vec::new();

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            for entry in manifest.entries() {
                if entry.is_alive() && delete_paths.contains(entry.file_path()) {
                    found_paths.insert(entry.file_path().to_string());

                    // Create a deleted manifest entry preserving the original
                    // sequence numbers.
                    let deleted_entry = ManifestEntry::builder()
                        .status(crate::spec::ManifestStatus::Deleted)
                        .snapshot_id(entry.snapshot_id().unwrap_or(0))
                        .sequence_number(entry.sequence_number().unwrap_or(0))
                        .file_sequence_number(entry.sequence_number().unwrap_or(0))
                        .data_file(entry.data_file().clone())
                        .build();

                    delete_entries.push(deleted_entry);
                }
            }
        }

        // Safety check: every file-to-delete MUST have been found
        let missing: Vec<String> = delete_paths
            .iter()
            .filter(|p| !found_paths.contains(**p))
            .map(|p| p.to_string())
            .collect();

        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot commit rewrite: the following files to delete were not found \
                     in the current snapshot: {}",
                    missing.join(", ")
                ),
            ));
        }

        // Concurrent-delete validation: if data_sequence_number is set, check
        // that no new delete files have been added for partitions containing
        // files being rewritten. This mirrors Java's
        // `validateNoNewDeletesForDataFiles()` in MergingSnapshotProducer.
        if let Some(data_seq_num) = self.data_sequence_number {
            let rewrite_partitions: HashSet<&Struct> =
                self.files_to_delete.iter().map(|f| f.partition()).collect();

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Deletes {
                    continue;
                }

                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    let entry_seq = entry.sequence_number().unwrap_or(0);
                    if entry_seq > data_seq_num
                        && entry.is_alive()
                        && rewrite_partitions.contains(entry.data_file().partition())
                    {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Cannot commit rewrite: found new delete file {} \
                                 (sequence_number={}) added after the planning snapshot \
                                 (data_sequence_number={}). A concurrent operation added \
                                 deletes for data files being rewritten.",
                                entry.file_path(),
                                entry_seq,
                                data_seq_num,
                            ),
                        ));
                    }
                }
            }
        }

        Ok(delete_entries)
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Carry forward all existing manifests
        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    fn make_data_file(path: &str, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_rewrite_files_success() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Step 1: Append a data file so there is something to rewrite.
        let original_file = make_data_file("test/original.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Verify the file was appended.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(!manifest_list.entries().is_empty());

        // Step 2: Rewrite: delete the original file, add a replacement.
        let replacement_file = make_data_file("test/replacement.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![original_file])
            .add_files(vec![replacement_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Step 3: Verify the snapshot uses the Replace operation and
        // the replacement file is present.
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation.as_str(), "replace");

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Collect all alive file paths across all manifests.
        let mut alive_paths = Vec::new();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    alive_paths.push(entry.file_path().to_string());
                }
            }
        }

        assert!(
            alive_paths.contains(&"test/replacement.parquet".to_string()),
            "Replacement file should be present in snapshot"
        );
    }

    #[tokio::test]
    async fn test_rewrite_files_fails_if_deleted_file_missing() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Step 1: Append a data file so the table has a snapshot.
        let existing_file = make_data_file("test/existing.parquet", 5);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![existing_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Step 2: Try to delete a file that does not exist in the snapshot.
        let ghost_file = make_data_file("test/ghost.parquet", 5);
        let replacement = make_data_file("test/replacement2.parquet", 5);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![ghost_file])
            .add_files(vec![replacement]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(
            result.is_err(),
            "Commit should fail when deleting a file not in the snapshot"
        );
        let err = result.unwrap_err();
        assert!(
            err.message().contains("not found in the current snapshot"),
            "Error message should mention missing files, got: {}",
            err.message()
        );
    }
}
