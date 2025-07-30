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
use iceberg_catalog_hadoop::HadoopCatalog;

#[tokio::test]
async fn test_hadoop_catalog_list_tables() {
    let file_io = FileIOBuilder::new_fs_io()
        .build()
        .expect("Should build FS FileIO");
    let cargo_manifest_dir = env!("CARGO_MANIFEST_DIR");
    let test_data_path = format!("file://{}/testdata/hadoop_warehouse", cargo_manifest_dir);
    let catalog = HadoopCatalog::new(test_data_path, file_io);

    // List tables in the root namespace
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
}
