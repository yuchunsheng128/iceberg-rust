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

//! Iceberg Hadoop Catalog implementation.

use std::collections::HashMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use opendal::EntryMode;

/// Represents a hadoop catalog backed by storage from a `FileIO`
#[derive(Debug)]
pub struct HadoopCatalog {
    file_io: FileIO,
    warehouse_root: String,
}

impl HadoopCatalog {
    /// Creates a new instance of a `HadoopCatalog`
    /// The `warehouse_root` should be the absolute path to the warehouse directory, including the scheme prefix for the FileIO
    pub fn new(warehouse_root: String, file_io: FileIO) -> Self {
        // TODO: validate the warehouse_root starts with the same scheme as the FileIO
        Self {
            file_io,
            warehouse_root,
        }
    }
}

#[async_trait]
impl Catalog for HadoopCatalog {
    // Unsupported operations in Hadoop Catalog
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating namespaces is not supported in hadoop catalog",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespaces is not supported in hadoop catalog",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Dropping namespaces is not supported in hadoop catalog",
        ))
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating tables is not supported in hadoop catalog",
        ))
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Dropping tables is not supported in hadoop catalog",
        ))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Renaming tables is not supported in hadoop catalog",
        ))
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating tables is not supported in hadoop catalog",
        ))
    }

    // Supported operations in Hadoop Catalog
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Not implemented yet",
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Not implemented yet",
        ))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Not implemented yet",
        ))
    }

    async fn load_table(&self, _table_identifier: &TableIdent) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Not implemented yet",
        ))
    }

    async fn table_exists(&self, _table: &TableIdent) -> Result<bool> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Not implemented yet",
        ))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        // List the tables in the specified namespace
        let path = format!("{}/{}/", self.warehouse_root, namespace.to_string());
        let mut tables = Vec::new();

        let mut lister = self.file_io.lister(&path).await?;
        while let Some(entry) = lister.try_next().await? {
            if matches!(entry.metadata().mode(), EntryMode::DIR) {
                if path.ends_with(entry.path()) {
                    // Skip the root directory itself
                    continue;
                }

                let table_name = entry
                    .name()
                    .strip_suffix("/")
                    .unwrap_or(entry.name())
                    .to_string();

                let table_ident = TableIdent {
                    namespace: namespace.clone(),
                    name: table_name,
                };

                // TODO: validate the directory contains metadata files
                tables.push(table_ident);
            }
        }

        Ok(tables)
    }
}
