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
use crate::spec::{ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// `RewriteManifestsAction` is a transaction action that replaces a set of
/// manifest files with a new set, without changing any data files. This is
/// used by manifest compaction to merge small manifests into larger ones.
///
/// The action produces a `Replace` snapshot through `SnapshotProducer`,
/// ensuring correct summary computation and sequence numbering.
pub struct RewriteManifestsAction {
    /// The final manifest list: kept (unmodified) manifests plus new merged
    /// manifests, minus the old manifests that were rewritten.
    new_manifest_list: Vec<ManifestFile>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            new_manifest_list: Vec::new(),
            snapshot_properties: HashMap::new(),
        }
    }

    /// Set the complete new manifest list (kept manifests + merged manifests).
    pub fn with_manifest_list(mut self, manifests: Vec<ManifestFile>) -> Self {
        self.new_manifest_list = manifests;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.new_manifest_list.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite manifests action requires a non-empty manifest list",
            ));
        }

        // Pass a snapshot property to satisfy SnapshotProducer's precondition
        // that either added_data_files or snapshot_properties is non-empty.
        // Manifest rewrites don't add data files.
        let mut props = self.snapshot_properties.clone();
        props
            .entry("iceberg.action".to_string())
            .or_insert_with(|| "rewrite-manifests".to_string());

        let snapshot_producer = SnapshotProducer::new(
            table,
            Uuid::now_v7(),
            None,
            props,
            vec![], // No added data files — manifest rewrite only
        );

        let operation = RewriteManifestsOperation {
            new_manifest_list: self.new_manifest_list.clone(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

struct RewriteManifestsOperation {
    new_manifest_list: Vec<ManifestFile>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // No data files are being changed in a manifest rewrite
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Return the pre-computed manifest list (kept + merged manifests)
        Ok(self.new_manifest_list.clone())
    }
}
