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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry, ManifestEntryRef,
    ManifestFile, ManifestWriterBuilder, Operation, PartitionSpec,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A group of manifest entries to be written as a new merged manifest.
#[derive(Debug, Clone)]
pub struct MergedManifestGroup {
    /// The entries to write.
    pub entries: Vec<ManifestEntryRef>,
    /// The content type for the output manifest.
    pub content_type: ManifestContentType,
    /// The partition spec for the output manifest.
    pub partition_spec: PartitionSpec,
}

/// `RewriteManifestsAction` is a transaction action that replaces a set of
/// manifest files with new merged manifests, without changing any data files.
///
/// Unlike pre-writing manifests before commit, this action writes merged
/// manifests inside the `SnapshotProduceOperation` where the snapshot ID is
/// known, ensuring correct metadata on the output manifest files.
pub struct RewriteManifestsAction {
    /// Manifests that are kept unchanged (not rewritten).
    kept_manifests: Vec<ManifestFile>,
    /// Groups of entries to be written as new merged manifests.
    merge_groups: Vec<MergedManifestGroup>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            kept_manifests: Vec::new(),
            merge_groups: Vec::new(),
            snapshot_properties: HashMap::new(),
        }
    }

    /// Set the manifests that should be kept unchanged.
    pub fn with_kept_manifests(mut self, manifests: Vec<ManifestFile>) -> Self {
        self.kept_manifests = manifests;
        self
    }

    /// Add a group of entries to be written as a merged manifest.
    pub fn add_merge_group(mut self, group: MergedManifestGroup) -> Self {
        self.merge_groups.push(group);
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.kept_manifests.is_empty() && self.merge_groups.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite manifests action requires at least one manifest",
            ));
        }

        // Pass a snapshot property to satisfy SnapshotProducer's precondition.
        let mut props = self.snapshot_properties.clone();
        props
            .entry("iceberg.action".to_string())
            .or_insert_with(|| "rewrite-manifests".to_string());

        let snapshot_producer = SnapshotProducer::new(
            table,
            Uuid::now_v7(),
            None,
            props,
            vec![], // No added data files
        );

        let operation = RewriteManifestsOperation {
            kept_manifests: self.kept_manifests.clone(),
            merge_groups: self.merge_groups.clone(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

struct RewriteManifestsOperation {
    kept_manifests: Vec<ManifestFile>,
    merge_groups: Vec<MergedManifestGroup>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let mut result = self.kept_manifests.clone();

        // Write each merge group as a new manifest, using the producer's
        // snapshot_id so the manifest metadata is correct.
        let snapshot_id = snapshot_produce.snapshot_id();
        let commit_uuid = snapshot_produce.commit_uuid();
        let table = snapshot_produce.table;
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let file_io = table.file_io();

        for (idx, group) in self.merge_groups.iter().enumerate() {
            if group.entries.is_empty() {
                continue;
            }

            let manifest_path = format!(
                "{}/metadata/{}-m{}.{}",
                metadata.location(),
                commit_uuid,
                idx,
                DataFileFormat::Avro,
            );
            let output_file = file_io.new_output(&manifest_path)?;
            let builder = ManifestWriterBuilder::new(
                output_file,
                Some(snapshot_id),
                None,
                schema.clone(),
                group.partition_spec.clone(),
            );

            let mut writer = match metadata.format_version() {
                FormatVersion::V1 => builder.build_v1(),
                FormatVersion::V2 => match group.content_type {
                    ManifestContentType::Data => builder.build_v2_data(),
                    ManifestContentType::Deletes => builder.build_v2_deletes(),
                },
                FormatVersion::V3 => match group.content_type {
                    ManifestContentType::Data => builder.build_v3_data(),
                    ManifestContentType::Deletes => builder.build_v3_deletes(),
                },
            };

            for entry in &group.entries {
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

            let manifest_file = writer.write_manifest_file().await?;
            result.push(manifest_file);
        }

        Ok(result)
    }
}
