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

//! Integration tests for hadoop catalog.

use iceberg::Catalog;
use iceberg::io::FileIOBuilder;
use iceberg_catalog_hadoop::{HadoopCatalog, HadoopCatalogBuilder};

async fn get_hadoop_catalog() -> HadoopCatalog {
    let file_io = FileIOBuilder::new_fs_io()
        .build()
        .expect("Should build FS FileIO");
    let cargo_manifest_dir = env!("CARGO_MANIFEST_DIR");
    let test_data_path = format!("file://{}/testdata/hadoop_warehouse", cargo_manifest_dir);
    // let catalog = HadoopCatalog::new(test_data_path, file_io);
    let catalog = HadoopCatalogBuilder::default()
        .with_file_io(file_io)
        .with_warehouse_root(test_data_path)
        .build()
        .await
        .expect("Should build HadoopCatalog");

    catalog
}

mod tests {
    use arrow_array::RecordBatch;
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_hadoop_catalog_list_tables() {
        let catalog = get_hadoop_catalog().await;

        // List tables in the test namespace
        let mut tables = catalog
            .list_tables(&iceberg::NamespaceIdent::new("test".to_string()))
            .await
            .expect("Should list tables");
        tables.sort_by(|a, b| a.name().cmp(b.name()));

        assert!(
            tables.len() == 2,
            "Should have exactly two tables in the test namespace, found: {:?}",
            tables
        );

        assert!(
            tables[0].name() == "my_table_1",
            "The table name should be 'my_table_1', found: {}",
            tables[0].name()
        );

        assert!(
            tables[1].name() == "my_table_2",
            "The table name should be 'my_table_2', found: {}",
            tables[1].name()
        );

        // List tables in the nested namespace
        let mut tables = catalog
            .list_tables(
                &iceberg::NamespaceIdent::from_strs(&["nested", "test"])
                    .expect("Should create namespace"),
            )
            .await
            .expect("Should list tables");
        tables.sort_by(|a, b| a.name().cmp(b.name()));

        assert!(
            tables.len() == 1,
            "Should have exactly one tables in the test namespace, found: {:?}",
            tables
        );

        assert!(
            tables[0].name() == "my_table_3",
            "The table name should be 'my_table_3', found: {}",
            tables[0].name()
        );
    }

    #[tokio::test]
    async fn test_hadoop_catalog_list_namespaces() {
        let catalog = get_hadoop_catalog().await;

        // List namespaces
        let mut namespaces = catalog
            .list_namespaces(None)
            .await
            .expect("Should list namespaces");

        assert!(
            namespaces.len() == 2,
            "Should have exactly one namespace, found: {:?}",
            namespaces
        );

        namespaces.sort_by(|a, b| a.to_string().cmp(&b.to_string()));

        assert!(
            namespaces[0].to_string() == "nested",
            "The namespace should be 'nested', found: {}",
            namespaces[0].to_string()
        );

        assert!(
            namespaces[1].to_string() == "test",
            "The namespace should be 'test', found: {}",
            namespaces[1].to_string()
        );

        // List namespaces in the nested namespace
        let nested_namespaces = catalog
            .list_namespaces(Some(&iceberg::NamespaceIdent::new("nested".to_string())))
            .await
            .expect("Should list nested namespaces");

        assert!(
            nested_namespaces.len() == 1,
            "Should have exactly one nested namespace, found: {:?}",
            nested_namespaces
        );

        assert!(
            nested_namespaces[0].to_string() == "nested.test",
            "The nested namespace should be 'nested.test', found: {}",
            nested_namespaces[0].to_string()
        );
    }

    #[tokio::test]
    async fn test_hadoop_catalog_namespace_exists() {
        let catalog = get_hadoop_catalog().await;

        // Check if the namespace exists
        let exists = catalog
            .namespace_exists(&iceberg::NamespaceIdent::new("test".to_string()))
            .await
            .expect("Should check namespace existence");

        assert!(exists, "The namespace 'test' should exist");

        // Check a non-existing namespace
        let non_existing = catalog
            .namespace_exists(&iceberg::NamespaceIdent::new("non_existing".to_string()))
            .await
            .expect("Should check non-existing namespace");

        assert!(
            !non_existing,
            "The namespace 'non_existing' should not exist"
        );

        // Check a nested namespace
        let nested_exists = catalog
            .namespace_exists(
                &iceberg::NamespaceIdent::from_strs(&["nested", "test"])
                    .expect("Should create nested namespace"),
            )
            .await
            .expect("Should check nested namespace existence");

        assert!(nested_exists, "The namespace 'nested.test' should exist");
    }

    #[tokio::test]
    async fn test_hadoop_catalog_table_exists() {
        let catalog = get_hadoop_catalog().await;

        // Check if a table exists
        let exists = catalog
            .table_exists(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new("test".to_string()),
                "my_table_1".to_string(),
            ))
            .await
            .expect("Should check table existence");

        assert!(exists, "The table 'my_table_1' should exist");

        // Check if a nested table exists
        let exists = catalog
            .table_exists(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::from_strs(&["nested", "test"])
                    .expect("Should create nested namespace"),
                "my_table_3".to_string(),
            ))
            .await
            .expect("Should check table existence");

        assert!(exists, "The table 'my_table_3' should exist");

        // Check a non-existing table
        let non_existing = catalog
            .table_exists(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new("test".to_string()),
                "non_existing_table".to_string(),
            ))
            .await
            .expect("Should check non-existing table");

        assert!(
            !non_existing,
            "The table 'non_existing_table' should not exist"
        );
    }

    #[tokio::test]
    async fn test_hadoop_load_table() {
        let catalog = get_hadoop_catalog().await;

        // Load a table - my_table_1
        {
            let table = catalog
                .load_table(&iceberg::TableIdent::new(
                    iceberg::NamespaceIdent::new("test".to_string()),
                    "my_table_1".to_string(),
                ))
                .await
                .expect("Should load table");

            assert!(
                table.identifier().name() == "my_table_1",
                "The loaded table name should be 'my_table_1', found: {}",
                table.identifier().name()
            );

            // Check if the table is readonly
            assert!(table.readonly(), "The table should be readonly");

            // Read rows from the table
            let table_scan = table
                .scan()
                .select_all()
                .build()
                .expect("Should build scan");
            let mut row_stream = table_scan
                .to_arrow()
                .await
                .expect("Should convert to Arrow");

            let mut row_count = 0;
            let mut row_data: Vec<RecordBatch> = Vec::new();
            while let Some(row) = row_stream.try_next().await.expect("Should read next row") {
                row_count += row.num_rows();
                row_data.push(row);
            }

            assert!(
                row_count == 2,
                "Should have exactly 2 rows in the table, found: {}",
                row_count
            );

            let snapshot_content = arrow::util::pretty::pretty_format_batches(&row_data)
                .expect("Should print batches");

            insta::assert_snapshot!(snapshot_content);
        }

        // Load another table - my_table_2
        {
            let table = catalog
                .load_table(&iceberg::TableIdent::new(
                    iceberg::NamespaceIdent::new("test".to_string()),
                    "my_table_2".to_string(),
                ))
                .await
                .expect("Should load table");

            assert!(
                table.identifier().name() == "my_table_2",
                "The loaded table name should be 'my_table_2', found: {}",
                table.identifier().name()
            );

            // Check if the table is readonly
            assert!(table.readonly(), "The table should be readonly");

            // Read rows from the table
            let table_scan = table
                .scan()
                .select_all()
                .build()
                .expect("Should build scan");
            let mut row_stream = table_scan
                .to_arrow()
                .await
                .expect("Should convert to Arrow");

            let mut row_count = 0;
            let mut row_data: Vec<RecordBatch> = Vec::new();
            while let Some(row) = row_stream.try_next().await.expect("Should read next row") {
                row_count += row.num_rows();
                row_data.push(row);
            }

            assert!(
                row_count == 2,
                "Should have exactly 2 rows in the table, found: {}",
                row_count
            );

            let snapshot_content = arrow::util::pretty::pretty_format_batches(&row_data)
                .expect("Should print batches");

            insta::assert_snapshot!(snapshot_content);
        }

        // Load another table - my_table_3
        {
            let table = catalog
                .load_table(&iceberg::TableIdent::new(
                    iceberg::NamespaceIdent::from_strs(&["nested", "test"])
                        .expect("Should create nested namespace"),
                    "my_table_3".to_string(),
                ))
                .await
                .expect("Should load table");

            assert!(
                table.identifier().name() == "my_table_3",
                "The loaded table name should be 'my_table_3', found: {}",
                table.identifier().name()
            );

            // Check if the table is readonly
            assert!(table.readonly(), "The table should be readonly");

            // Read rows from the table
            let table_scan = table
                .scan()
                .select_all()
                .build()
                .expect("Should build scan");
            let mut row_stream = table_scan
                .to_arrow()
                .await
                .expect("Should convert to Arrow");

            let mut row_count = 0;
            let mut row_data: Vec<RecordBatch> = Vec::new();
            while let Some(row) = row_stream.try_next().await.expect("Should read next row") {
                row_count += row.num_rows();
                row_data.push(row);
            }

            assert!(
                row_count == 2,
                "Should have exactly 2 rows in the table, found: {}",
                row_count
            );

            let snapshot_content = arrow::util::pretty::pretty_format_batches(&row_data)
                .expect("Should print batches");

            insta::assert_snapshot!(snapshot_content);
        }
    }
}
