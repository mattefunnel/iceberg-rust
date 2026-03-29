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

//! [`DataFusionFileRewriter`] implements the [`FileRewriter`] trait from
//! `iceberg-actions` using DataFusion's execution engine to read and write
//! parquet files during bin-pack compaction.

use async_trait::async_trait;
use futures::StreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Error, ErrorKind, Result};
use iceberg_actions::{FileRewriter, RewriteFileGroup};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

/// A [`FileRewriter`] implementation that uses the iceberg writer
/// infrastructure to read and rewrite parquet data files.
///
/// This rewriter reads each input file through the table's [`FileIO`],
/// decodes parquet record batches, and writes them back out through the
/// standard iceberg writer stack (`DataFileWriterBuilder` ->
/// `RollingFileWriterBuilder` -> `ParquetWriterBuilder`).
///
/// The result is typically fewer, larger data files suitable for bin-pack
/// compaction.
pub struct DataFusionFileRewriter;

impl DataFusionFileRewriter {
    /// Create a new `DataFusionFileRewriter`.
    pub fn new() -> Self {
        Self
    }
}

impl Default for DataFusionFileRewriter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FileRewriter for DataFusionFileRewriter {
    /// Rewrite a group of data files into new, compacted data files.
    ///
    /// This method:
    /// 1. Reads through Iceberg's [`ArrowReaderBuilder`] which automatically
    ///    applies position and equality deletes
    /// 2. Writes record batches through the iceberg writer stack
    /// 3. Returns new `DataFile` metadata for the compacted output
    async fn rewrite(&self, table: &Table, group: RewriteFileGroup) -> Result<Vec<DataFile>> {
        if group.tasks.is_empty() {
            return Ok(Vec::new());
        }

        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let file_io = table.file_io().clone();

        // Use target file size from the group (provided by RewriteDataFiles)
        let target_file_size = group.target_file_size_bytes;

        // Set up the writer stack
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), schema.clone());
        let location_generator = DefaultLocationGenerator::new(metadata.clone()).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to create location generator: {e}"),
            )
        })?;
        let file_name_generator = DefaultFileNameGenerator::new(
            Uuid::now_v7().to_string(),
            None,
            DataFileFormat::Parquet,
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size as usize,
            file_io.clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let partition_key = group.partition_value.as_ref().map(|pv| {
            iceberg::spec::PartitionKey::new(
                group.partition_spec.as_ref().clone(),
                schema,
                pv.clone(),
            )
        });
        let mut writer = data_file_writer_builder.build(partition_key).await?;

        // Read through Iceberg's delete-aware ArrowReader. This applies
        // position deletes and equality deletes automatically so that
        // logically deleted rows do not appear in the compacted output.
        let task_stream = Box::pin(futures::stream::iter(group.tasks.into_iter().map(Ok)));
        let arrow_reader = ArrowReaderBuilder::new(file_io).build();
        let mut record_batch_stream = arrow_reader.read(task_stream)?;

        while let Some(batch_result) = record_batch_stream.next().await {
            let batch = batch_result?;
            writer.write(batch).await?;
        }

        let data_files = writer.close().await?;
        Ok(data_files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that DataFusionFileRewriter implements the FileRewriter trait
    /// and can be constructed.
    #[test]
    fn test_implements_file_rewriter_trait() {
        let rewriter = DataFusionFileRewriter::new();
        // Verify it can be used as a trait object.
        let _trait_obj: &dyn FileRewriter = &rewriter;
    }

    /// Verify that default construction works.
    #[test]
    fn test_default_construction() {
        let _rewriter = DataFusionFileRewriter;
    }

    /// Verify that rewrite_files returns an empty vec for an empty group.
    #[tokio::test]
    async fn test_rewrite_empty_group() {
        use std::collections::HashMap;

        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .location(warehouse_path)
            .name("test_table".to_string())
            .properties(HashMap::new())
            .schema(schema)
            .build();

        let table = catalog.create_table(&namespace, creation).await.unwrap();

        let rewriter = DataFusionFileRewriter::new();
        let empty_group = iceberg_actions::RewriteFileGroup {
            tasks: Vec::new(),
            target_file_size_bytes: 512 * 1024 * 1024,
            partition_spec: table.metadata().default_partition_spec().clone(),
            partition_value: None,
        };
        let result = rewriter.rewrite(&table, empty_group).await.unwrap();
        assert!(result.is_empty());
    }

    /// End-to-end test: write parquet files, then rewrite them via
    /// DataFusionFileRewriter and verify the output.
    #[tokio::test]
    async fn test_rewrite_files_round_trip() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use iceberg::io::LocalFsStorageFactory;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Struct, Type};
        use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
        use iceberg::writer::file_writer::ParquetWriterBuilder;
        use iceberg::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
        use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
        use parquet::file::properties::WriterProperties;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        // Create catalog with LocalFsStorageFactory so the table's FileIO
        // can read/write real files on the local filesystem.
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let iceberg_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .location(warehouse_path.clone())
            .name("rewrite_test".to_string())
            .properties(HashMap::new())
            .schema(iceberg_schema)
            .build();

        let table = catalog.create_table(&namespace, creation).await.unwrap();
        let schema = table.metadata().current_schema().clone();
        let file_io = table.file_io().clone();

        // Write two small parquet files using the iceberg writer stack,
        // using the same FileIO as the table so the rewriter can read them.
        let location_gen =
            DefaultLocationGenerator::with_data_location(format!("{warehouse_path}/data"));
        let file_name_gen =
            DefaultFileNameGenerator::new("original".to_string(), None, DataFileFormat::Parquet);

        let pw = ParquetWriterBuilder::new(WriterProperties::default(), schema.clone());
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen.clone(),
            file_name_gen,
        );
        let dfwb = DataFileWriterBuilder::new(rolling);

        // Build arrow schema with field IDs matching iceberg schema.
        let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "id",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            )
            .with_metadata(HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            datafusion::arrow::datatypes::Field::new(
                "name",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            )
            .with_metadata(HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        // Write file 1: rows (1, "Alice"), (2, "Bob")
        let mut w1 = dfwb.build(None).await.unwrap();
        let batch1 = datafusion::arrow::array::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    "Alice", "Bob",
                ])),
            ],
        )
        .unwrap();
        w1.write(batch1).await.unwrap();
        let files1 = w1.close().await.unwrap();

        // Write file 2: rows (3, "Charlie")
        let file_name_gen2 =
            DefaultFileNameGenerator::new("original2".to_string(), None, DataFileFormat::Parquet);
        let pw2 = ParquetWriterBuilder::new(WriterProperties::default(), schema.clone());
        let rolling2 = RollingFileWriterBuilder::new_with_default_file_size(
            pw2,
            file_io.clone(),
            location_gen,
            file_name_gen2,
        );
        let dfwb2 = DataFileWriterBuilder::new(rolling2);
        let mut w2 = dfwb2.build(None).await.unwrap();
        let batch2 = datafusion::arrow::array::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![3])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["Charlie"])),
            ],
        )
        .unwrap();
        w2.write(batch2).await.unwrap();
        let files2 = w2.close().await.unwrap();

        // Commit the data files via fast-append so plan_files() can find them.
        let mut input_files: Vec<DataFile> = Vec::new();
        input_files.extend(files1);
        input_files.extend(files2);
        assert_eq!(input_files.len(), 2);

        let total_input_records: u64 = input_files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total_input_records, 3);

        let tx = iceberg::transaction::Transaction::new(&table);
        let action = tx.fast_append().add_data_files(input_files);
        let tx = iceberg::transaction::ApplyTransactionAction::apply(action, tx).unwrap();
        let table = tx.commit(&*catalog).await.unwrap();

        // Use plan_files() to get proper FileScanTask objects with delete
        // association and partition context.
        let scan = table.scan().select_all().build().unwrap();
        let tasks: Vec<iceberg::scan::FileScanTask> =
            futures::StreamExt::collect::<Vec<_>>(scan.plan_files().await.unwrap())
                .await
                .into_iter()
                .collect::<iceberg::Result<Vec<_>>>()
                .unwrap();
        assert_eq!(tasks.len(), 2);

        let rewriter = DataFusionFileRewriter::new();
        let rewrite_group = iceberg_actions::RewriteFileGroup {
            tasks,
            target_file_size_bytes: 512 * 1024 * 1024,
            partition_spec: table.metadata().default_partition_spec().clone(),
            partition_value: Some(Struct::empty()),
        };
        let new_files = rewriter.rewrite(&table, rewrite_group).await.unwrap();

        // Verify output: should have at least one file, total records = 3.
        assert!(!new_files.is_empty(), "Expected at least one output file");
        let total_output_records: u64 = new_files.iter().map(|f| f.record_count()).sum();
        assert_eq!(
            total_output_records, 3,
            "Expected 3 total records in output"
        );

        // All output files should be parquet.
        for f in &new_files {
            assert_eq!(f.file_format(), DataFileFormat::Parquet);
            assert!(f.file_size_in_bytes() > 0);
            // Verify the output file exists.
            assert!(
                file_io.exists(f.file_path()).await.unwrap(),
                "Output file should exist: {}",
                f.file_path()
            );
            // Verify the partition is empty (unpartitioned table).
            assert_eq!(*f.partition(), Struct::empty());
        }
    }
}
