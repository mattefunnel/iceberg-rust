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

//! Shared test helpers for iceberg-actions integration tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct,
    Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

/// A test context that owns a MemoryCatalog, namespace, and table
/// suitable for iceberg-actions unit and integration tests.
pub struct TestContext {
    pub catalog: Arc<dyn Catalog>,
    pub table_ident: TableIdent,
}

impl TestContext {
    /// Create a new test context with a fresh MemoryCatalog, namespace, and
    /// unpartitioned table. The `prefix` is incorporated into the namespace
    /// name to avoid collisions when multiple tests run in parallel.
    pub async fn new(prefix: &str) -> Self {
        let warehouse = format!("/tmp/iceberg-actions-test-{prefix}");
        let props = HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]);

        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .load("test_catalog", props)
                .await
                .expect("failed to create MemoryCatalog"),
        );

        let ns_name = format!("ns_{prefix}");
        let namespace = NamespaceIdent::new(ns_name.clone());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("failed to create namespace");

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("failed to build schema");

        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .expect("failed to create table");

        let table_ident =
            TableIdent::from_strs([&ns_name, "test_table"]).expect("failed to build TableIdent");

        TestContext {
            catalog,
            table_ident,
        }
    }

    /// Reload the table from the catalog to pick up the latest metadata.
    pub async fn load_table(&self) -> Table {
        self.catalog
            .load_table(&self.table_ident)
            .await
            .expect("failed to load table")
    }

    /// Append a single synthetic data file to the table via fast-append.
    /// The data file is a metadata-only entry (no physical parquet is written);
    /// this is sufficient for testing maintenance actions that operate on
    /// metadata rather than file contents.
    pub async fn append_data_file(&self) {
        self.append_data_files(1).await;
    }

    /// Append `n` synthetic data files to the table in a single snapshot.
    pub async fn append_data_files(&self, n: usize) {
        let table = self.load_table().await;

        let data_files: Vec<_> = (0..n)
            .map(|i| {
                let file_path = format!("data/{}-{i}.parquet", uuid::Uuid::now_v7());
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(file_path)
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(1024)
                    .record_count(100)
                    .partition(Struct::empty())
                    .partition_spec_id(0)
                    .build()
                    .expect("failed to build DataFile")
            })
            .collect();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx).expect("failed to apply fast_append");
        tx.commit(self.catalog.as_ref())
            .await
            .expect("failed to commit fast_append");
    }
}
