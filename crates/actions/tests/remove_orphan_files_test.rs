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

//! Integration tests for the RemoveOrphanFiles action.

mod common;

use std::collections::HashMap;

use bytes::Bytes;
use common::TestContext;
use iceberg::TableCommit;
use iceberg_actions::RemoveOrphanFiles;

#[tokio::test]
async fn test_remove_orphan_files_empty_table() {
    let ctx = TestContext::new("orphan_empty").await;
    let table = ctx.load_table().await;

    let result = RemoveOrphanFiles::new(&table)
        .execute()
        .await
        .expect("remove orphan files on empty table should succeed");

    assert!(
        result.orphan_file_locations.is_empty(),
        "empty table should have no orphan files"
    );
}

#[tokio::test]
async fn test_remove_orphan_files_detects_orphan() {
    let ctx = TestContext::new("orphan_detect").await;

    // Create a snapshot so the table has real data
    ctx.append_data_file().await;
    let table = ctx.load_table().await;

    // Write an orphan file directly to the table location
    let table_location = table.metadata().location().to_string();
    let orphan_path = format!("{table_location}/data/orphan-file.parquet");
    table
        .file_io()
        .new_output(&orphan_path)
        .unwrap()
        .write(Bytes::from("orphan data"))
        .await
        .unwrap();

    let result = RemoveOrphanFiles::new(&table)
        .execute()
        .await
        .expect("remove orphan files should succeed");

    assert!(
        result.orphan_file_locations.contains(&orphan_path),
        "orphan file should be detected; found: {:?}",
        result.orphan_file_locations
    );

    // Verify the orphan file was actually deleted
    let exists = table
        .file_io()
        .new_input(&orphan_path)
        .unwrap()
        .exists()
        .await
        .unwrap();
    assert!(!exists, "orphan file should have been deleted");
}

#[tokio::test]
async fn test_remove_orphan_files_dry_run() {
    let ctx = TestContext::new("orphan_dryrun").await;

    ctx.append_data_file().await;
    let table = ctx.load_table().await;

    // Write an orphan file
    let table_location = table.metadata().location().to_string();
    let orphan_path = format!("{table_location}/data/dry-run-orphan.parquet");
    table
        .file_io()
        .new_output(&orphan_path)
        .unwrap()
        .write(Bytes::from("dry run data"))
        .await
        .unwrap();

    let result = RemoveOrphanFiles::new(&table)
        .dry_run(true)
        .execute()
        .await
        .expect("dry run should succeed");

    assert!(
        result.orphan_file_locations.contains(&orphan_path),
        "orphan should be reported in dry run"
    );

    // Verify the orphan file still exists (not deleted in dry run)
    let exists = table
        .file_io()
        .new_input(&orphan_path)
        .unwrap()
        .exists()
        .await
        .unwrap();
    assert!(
        exists,
        "orphan file should NOT have been deleted in dry run"
    );
}

#[tokio::test]
async fn test_remove_orphan_files_gc_disabled() {
    let ctx = TestContext::new("orphan_gc_disabled").await;
    let table = ctx.load_table().await;

    // Set gc.enabled=false
    let table_commit = TableCommit::builder()
        .ident(ctx.table_ident.clone())
        .updates(vec![iceberg::TableUpdate::SetProperties {
            updates: HashMap::from([("gc.enabled".to_string(), "false".to_string())]),
        }])
        .requirements(vec![iceberg::TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        }])
        .build();
    ctx.catalog
        .update_table(table_commit)
        .await
        .expect("should set gc.enabled=false");

    let table = ctx.load_table().await;

    let err = RemoveOrphanFiles::new(&table).execute().await;

    assert!(
        err.is_err(),
        "remove orphan files should fail when gc.enabled=false"
    );
    let err_msg = format!("{}", err.unwrap_err());
    assert!(
        err_msg.contains("gc.enabled"),
        "error message should mention gc.enabled, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_remove_orphan_files_preserves_live_files() {
    let ctx = TestContext::new("orphan_preserve_live").await;

    // Create snapshots with real data files
    ctx.append_data_file().await;
    ctx.append_data_file().await;
    let table = ctx.load_table().await;

    // Collect all referenced data files before the action
    let file_io = table.file_io();
    let metadata = table.metadata();
    let mut referenced_data_files: Vec<String> = Vec::new();
    for snapshot in metadata.snapshots() {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest_bytes = file_io
                .new_input(&manifest_file.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            let (_meta, entries) =
                iceberg::spec::Manifest::try_from_avro_bytes(&manifest_bytes).unwrap();
            for entry in &entries {
                referenced_data_files.push(entry.data_file.file_path().to_string());
            }
        }
    }

    assert!(
        !referenced_data_files.is_empty(),
        "should have at least one referenced data file"
    );

    // Run remove orphan files — should NOT delete any live files
    let result = RemoveOrphanFiles::new(&table)
        .execute()
        .await
        .expect("remove orphan files should succeed");

    // None of the referenced data files should appear in orphan_file_locations
    for data_file in &referenced_data_files {
        assert!(
            !result.orphan_file_locations.contains(data_file),
            "referenced data file should not be reported as orphan: {data_file}"
        );
    }
}

#[tokio::test]
async fn test_remove_orphan_files_location_scoped() {
    let ctx = TestContext::new("orphan_location_scoped").await;

    ctx.append_data_file().await;
    let table = ctx.load_table().await;

    let table_location = table.metadata().location().to_string();

    // Write orphan files in two different subdirectories
    let orphan_in_data = format!("{table_location}/data/scoped-orphan.parquet");
    let orphan_in_other = format!("{table_location}/other/scoped-orphan.parquet");

    table
        .file_io()
        .new_output(&orphan_in_data)
        .unwrap()
        .write(Bytes::from("data orphan"))
        .await
        .unwrap();
    table
        .file_io()
        .new_output(&orphan_in_other)
        .unwrap()
        .write(Bytes::from("other orphan"))
        .await
        .unwrap();

    // Scan only the "other" subdirectory
    let scoped_location = format!("{table_location}/other");
    let result = RemoveOrphanFiles::new(&table)
        .location(&scoped_location)
        .execute()
        .await
        .expect("scoped remove orphan files should succeed");

    // Only the orphan in /other should be detected
    assert!(
        result.orphan_file_locations.contains(&orphan_in_other),
        "orphan in /other should be detected"
    );
    assert!(
        !result.orphan_file_locations.contains(&orphan_in_data),
        "orphan in /data should NOT be detected when scanning /other only"
    );

    // The orphan in /data should still exist (was not in scan scope)
    let exists = table
        .file_io()
        .new_input(&orphan_in_data)
        .unwrap()
        .exists()
        .await
        .unwrap();
    assert!(exists, "orphan in /data should still exist");
}
