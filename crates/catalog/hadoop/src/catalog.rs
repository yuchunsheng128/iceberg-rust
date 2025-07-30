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
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use opendal::{Entry, EntryMode};

/// Specifies the mode for identifying metadata files in a Hadoop catalog
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataMode {
    /// Infer the latest metadata file from the Hadoop structure
    Infer,
    /// Use the exact metadata file specified, or infer it if the file does not exist
    ExactOrInfer(String),
    /// Use the exact metadata file specified
    Exact(String),
}

/// Builder for creating a new `HadoopCatalog`
#[derive(Debug, Default)]
pub struct HadoopCatalogBuilder {
    warehouse_root: Option<String>,
    file_io: Option<FileIO>,
}

impl HadoopCatalogBuilder {
    /// Sets the warehouse root for the Hadoop catalog.
    /// The warehouse root should be the absolute path to the warehouse directory, including the scheme prefix for the FileIO.
    pub fn with_warehouse_root(mut self, warehouse_root: String) -> Self {
        self.warehouse_root = Some(warehouse_root);
        self
    }

    /// Sets the FileIO instance for the Hadoop catalog.
    pub fn with_file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    /// Builds the `HadoopCatalog` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the warehouse root is not specified, if the FileIO is not specified,
    /// if the warehouse root is not a directory, or if the warehouse root does not start with the FileIO scheme prefix.
    pub async fn build(self) -> Result<HadoopCatalog> {
        let mut warehouse_root = self.warehouse_root.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                "Warehouse root must be specified",
            )
        })?;

        let file_io = self
            .file_io
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "FileIO must be specified"))?;

        let root_input = file_io.new_input(&warehouse_root).map_err(|e| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("Invalid warehouse root: {}", e),
            )
        })?;

        if !matches!(root_input.metadata().await?.mode, EntryMode::DIR) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "Warehouse root must be a directory",
            ));
        }

        if !warehouse_root.starts_with(file_io.scheme()) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "Warehouse root must start with the FileIO scheme prefix",
            ));
        }

        if !warehouse_root.ends_with("/") {
            warehouse_root.push_str("/");
        }

        Ok(HadoopCatalog {
            warehouse_root,
            file_io,
            metadata_mode: MetadataMode::Infer,
        })
    }
}

/// Represents a hadoop catalog backed by storage from a `FileIO`
#[derive(Debug)]
pub struct HadoopCatalog {
    file_io: FileIO,
    warehouse_root: String,
    metadata_mode: MetadataMode,
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
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let path = if let Some(namespace) = parent {
            format!(
                "{warehouse_root}{namespace}/",
                warehouse_root = self.warehouse_root,
                namespace = namespace.join("/")
            )
        } else {
            self.warehouse_root.clone()
        };

        let mut namespaces = Vec::new();
        let directories = self.get_directories(&path).await?;

        for entry in directories {
            let path = format!("{path}{entry}/", path = path, entry = entry.name());
            if self
                .directory_has_metadata_and_data(&path, self.metadata_mode.clone())
                .await?
            {
                // This is a table, skip it
                continue;
            }

            let namespace_name = entry
                .name()
                .strip_suffix("/")
                .unwrap_or(entry.name())
                .to_string();

            let namespace = if let Some(parent) = parent.cloned() {
                let mut namespace = parent.inner();
                namespace.push(namespace_name);
                NamespaceIdent::from_vec(namespace)?
            } else {
                NamespaceIdent::from_vec(vec![namespace_name])?
            };

            namespaces.push(namespace);
        }

        Ok(namespaces)
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let path = format!(
            "{warehouse_root}{namespace}/",
            warehouse_root = self.warehouse_root,
            namespace = namespace.join("/")
        );

        self.directory_exists(&path).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        Ok(Namespace::new(namespace.clone()))
    }

    async fn load_table(&self, table_identifier: &TableIdent) -> Result<Table> {
        if !self.table_exists(table_identifier).await? {
            if let MetadataMode::Exact(ref metadata_file) = self.metadata_mode {
                let input_file = self.file_io.new_input(&metadata_file)?;
                if !input_file.exists().await? {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Expected metadata file '{}' does not exist", metadata_file),
                    ));
                }
            }

            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!("Table {} does not exist", table_identifier),
            ));
        }

        let metadata_file_path = match self.metadata_mode {
            MetadataMode::Infer => None,
            MetadataMode::ExactOrInfer(ref metadata_file) => {
                let input_file = self.file_io.new_input(metadata_file)?;
                if input_file.exists().await? {
                    Some(metadata_file.clone())
                } else {
                    // If the exact metadata file does not exist, infer the latest metadata file
                    None
                }
            }
            MetadataMode::Exact(ref metadata_file) => Some(metadata_file.clone()),
        };

        let metadata_file = if let Some(metadata_file) = metadata_file_path {
            self.file_io.new_input(&metadata_file)?
        } else {
            // Infer if there is a version hint
            let version_hint_path = format!(
                // TODO: version hint could be .txt or .text. refactor this to support both later
                "{warehouse_root}{namespace}/{table}/metadata/version-hint.txt",
                warehouse_root = self.warehouse_root,
                namespace = table_identifier.namespace.join("/"),
                table = table_identifier.name
            );

            let input = self.file_io.new_input(&version_hint_path)?;
            if input.exists().await? {
                // Load the version hint file to get the latest metadata file
                let metadata_version = input.read().await?;
                let metadata_version = std::str::from_utf8(&metadata_version).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid UTF-8 in version hint file: {}", e),
                    )
                })?;
                let metadata_file = format!(
                    "{warehouse_root}{namespace}/{table}/metadata/v{version}.metadata.json",
                    warehouse_root = self.warehouse_root,
                    namespace = table_identifier.namespace.join("/"),
                    table = table_identifier.name,
                    version = metadata_version.trim()
                );

                self.file_io.new_input(&metadata_file)?
            } else {
                // If there is no version hint, list the metadata files and get the latest one
                let metadata_directory = format!(
                    "{warehouse_root}{namespace}/{table}/metadata/",
                    warehouse_root = self.warehouse_root,
                    namespace = table_identifier.namespace.join("/"),
                    table = table_identifier.name
                );

                let mut lister = self.file_io.lister(&metadata_directory).await?;
                let mut latest_metadata_file: Option<Entry> = None;
                while let Some(entry) = lister.try_next().await? {
                    if matches!(entry.metadata().mode(), EntryMode::FILE)
                        && entry.name().ends_with(".metadata.json")
                    {
                        if let Some(latest_file) = &latest_metadata_file {
                            match (
                                latest_file.metadata().last_modified(),
                                entry.metadata().last_modified(),
                            ) {
                                (Some(latest_modified), Some(entry_modified)) => {
                                    // Compare last modified times
                                    if entry_modified > latest_modified {
                                        latest_metadata_file = Some(entry);
                                    }
                                }
                                _ => {
                                    // compare by name if last modified times are not available
                                    if entry.name() > latest_file.name() {
                                        latest_metadata_file = Some(entry);
                                    }
                                }
                            }
                        } else {
                            latest_metadata_file = Some(entry);
                        }
                    }
                }

                if let Some(latest_file) = latest_metadata_file {
                    let path = format!(
                        "{warehouse_root}{namespace}/{table}/metadata/{latest_file}",
                        warehouse_root = self.warehouse_root,
                        namespace = table_identifier.namespace.join("/"),
                        table = table_identifier.name,
                        latest_file = latest_file.name()
                    );

                    self.file_io.new_input(path)?
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "No metadata file found in the table directory",
                    ));
                }
            }
        };

        let metadata_file_content = metadata_file.read().await?;
        let table_metadata = serde_json::from_slice::<TableMetadata>(&metadata_file_content)?;

        Table::builder()
            .metadata(table_metadata)
            .identifier(table_identifier.clone())
            .file_io(self.file_io.clone())
            .readonly(true)
            .build()
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let path = format!(
            "{warehouse_root}{namespace}/{table}/",
            warehouse_root = self.warehouse_root,
            namespace = table.namespace.join("/"),
            table = table.name
        );

        if !self.directory_exists(&path).await? {
            return Ok(false);
        }

        // Check if the table has metadata
        self.directory_has_metadata_and_data(&path, MetadataMode::Infer)
            .await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        // List the tables in the specified namespace
        let path = format!(
            "{warehouse_root}{namespace}/",
            warehouse_root = self.warehouse_root,
            namespace = namespace.join("/")
        );
        let mut tables = Vec::new();

        let directories = self.get_directories(&path).await?;
        for entry in directories {
            let table_name = entry
                .name()
                .strip_suffix("/")
                .unwrap_or(entry.name())
                .to_string();

            let table_ident = TableIdent {
                namespace: namespace.clone(),
                name: table_name,
            };

            if self
                .directory_has_metadata_and_data(
                    &format!("{path}/{table_name}", table_name = table_ident.name),
                    self.metadata_mode.clone(),
                )
                .await?
            {
                tables.push(table_ident);
            } else {
                // TODO: Add something like a `MetadataMissingBehavior` to choose whether to fail or not
                tracing::warn!("Table {} does not have metadata, skipping", table_ident);
            }
        }

        Ok(tables)
    }
}

impl HadoopCatalog {
    async fn get_directories(&self, root: &str) -> Result<Vec<Entry>> {
        let mut directories = Vec::new();
        let mut lister = self.file_io.lister(root).await?;

        while let Some(entry) = lister.try_next().await? {
            if matches!(entry.metadata().mode(), EntryMode::DIR) && !root.ends_with(entry.path()) {
                directories.push(entry);
            }
        }

        Ok(directories)
    }

    async fn directory_has_metadata_and_data(
        &self,
        path: &str,
        metadata_mode: MetadataMode,
    ) -> Result<bool> {
        let data_dir = format!("{path}/data/");
        let input_data = self.file_io.new_input(&data_dir)?;
        return Ok(input_data.exists().await?
            && matches!(input_data.metadata().await?.mode, EntryMode::DIR)
            && self.directory_has_metadata(&path, metadata_mode).await?);
    }

    async fn directory_exists(&self, path: &str) -> Result<bool> {
        let input = self.file_io.new_input(path)?;
        if !input.exists().await? {
            return Ok(false);
        }

        let metadata = input.metadata().await?;
        Ok(matches!(metadata.mode, EntryMode::DIR))
    }

    async fn directory_has_metadata(
        &self,
        path: &str,
        metadata_mode: MetadataMode,
    ) -> Result<bool> {
        let metadata_directory = format!("{path}/metadata/");
        let input = self.file_io.new_input(&metadata_directory)?;
        if !input.exists().await? {
            return Ok(false);
        }

        let mut lister = self.file_io.lister(&metadata_directory).await?;
        while let Some(entry) = lister.try_next().await? {
            if matches!(entry.metadata().mode(), EntryMode::FILE) {
                let (metadata_file, fail_if_exact_missing) = match metadata_mode {
                    MetadataMode::Infer => (None, false),
                    MetadataMode::ExactOrInfer(ref metadata_file) => (Some(metadata_file), false),
                    MetadataMode::Exact(ref metadata_file) => (Some(metadata_file), true),
                };

                if let Some(metadata_file) = metadata_file {
                    if entry.name() == metadata_file {
                        return Ok(true);
                    } else if fail_if_exact_missing {
                        return Ok(false);
                    }
                }

                // Naive check if the file is a metadata file
                if entry.name().ends_with(".metadata.json") {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}
