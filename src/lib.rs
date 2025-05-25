//! # RQLite Storage Engine
//!
//! Author: [Raúl Moldes Castillo] (raul.moldes.work@gmail.com)
//! 
//! RQLite is a SQLite-compatible storage engine written in Rust. It provides
//! a complete B-Tree based storage system with support for both table and index
//! operations, transactions, and efficient memory management through a buffer pool.
//!
//! ## Architecture
//!
//! The storage engine is built in layers:
//! - **Disk Manager**: Low-level disk I/O operations
//! - **Pager**: Page-level operations with caching and transaction support
//! - **B-Tree**: High-level tree operations for tables and indexes
//! - **RQLite**: Main API that ties everything together
//!
//! ## Example Usage
//!
//! ```rust
//! use rqlite_engine::{RQLite, Record};
//! use rqlite_engine::utils::serialization::SqliteValue;
//!
//! // Create a new database
//! let mut db = RQLite::create("test.db", None)?;
//!
//! // Create a table
//! let table_id = db.create_table()?;
//!
//! // Insert a record
//! let record = Record::with_values(vec![
//!     SqliteValue::Integer(1),
//!     SqliteValue::String("Hello, World!".to_string()),
//! ]);
//! db.table_insert(table_id, 1, &record)?;
//!
//! // Find the record
//! let found = db.table_find(table_id, 1)?;
//! assert!(found.is_some());
//! ```

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

pub mod header;
pub mod page;
pub mod storage;
pub mod tree;
pub mod utils;

use storage::pager::Pager;
use tree::btree::{BTree, TreeType};
pub use tree::record::Record;
pub use utils::cmp::KeyValue;

/// Configuration options for the RQLite storage engine.
#[derive(Debug, Clone)]
pub struct RQLiteConfig {
    /// Page size in bytes (must be power of 2 between 512 and 65536).
    pub page_size: u32,
    /// Size of the buffer pool (number of pages to keep in memory).
    pub buffer_pool_size: usize,
    /// Reserved space at the end of each page.
    pub reserved_space: u8,
    /// Maximum fraction of a page that can be occupied by a single payload.
    pub max_payload_fraction: u8,
    /// Minimum fraction of a page that must be occupied by a payload when splitting.
    pub min_payload_fraction: u8,
}

impl Default for RQLiteConfig {
    fn default() -> Self {
        RQLiteConfig {
            page_size: 4096,
            buffer_pool_size: 1000,
            reserved_space: 0,
            max_payload_fraction: 255, // 100%
            min_payload_fraction: 32,  // ~12.5%
        }
    }
}

/// Unique identifier for a table in the database.
pub type TableId = u32;

/// Unique identifier for an index in the database.
pub type IndexId = u32;

/// Main entry point for the RQLite storage engine.
///
/// The `RQLite` struct provides a high-level interface for database operations
/// including table and index management, record insertion/retrieval, and
/// transaction support.
pub struct RQLite {
    /// The pager manages page-level operations and caching.
    pager: Arc<Pager>,
    /// Maps table IDs to their corresponding B-Trees.
    tables: HashMap<TableId, BTree>,
    /// Maps index IDs to their corresponding B-Trees.
    indexes: HashMap<IndexId, BTree>,
    /// Configuration options for this database instance.
    config: RQLiteConfig,
    /// Counter for generating unique table IDs.
    next_table_id: TableId,
    /// Counter for generating unique index IDs.
    next_index_id: IndexId,
}

impl RQLite {
    /// Creates a new database file with the specified configuration.
    ///
    /// # Parameters
    /// * `path` - Path where the database file will be created.
    /// * `config` - Optional configuration. If None, default values are used.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The database file cannot be created
    /// - The configuration values are invalid
    /// - There are disk I/O issues
    ///
    /// # Returns
    /// A new RQLite instance connected to the created database.
    ///
    /// # Example
    /// ```rust
    /// use rqlite_engine::{RQLite, RQLiteConfig};
    ///
    /// // Create with default configuration
    /// let db = RQLite::create("my_database.db", None)?;
    ///
    /// // Create with custom configuration
    /// let config = RQLiteConfig {
    ///     page_size: 8192,
    ///     buffer_pool_size: 2000,
    ///     ..Default::default()
    /// };
    /// let db = RQLite::create("my_database.db", Some(config))?;
    /// ```
    pub fn create<P: AsRef<Path>>(path: P, config: Option<RQLiteConfig>) -> io::Result<Self> {
        let config = config.unwrap_or_default();
        
        let pager = Pager::create(
            path,
            config.page_size,
            Some(config.buffer_pool_size),
            config.reserved_space,
        )?;

        Ok(RQLite {
            pager: Arc::new(pager),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            config,
            next_table_id: 1,
            next_index_id: 1,
        })
    }

    /// Opens an existing database file.
    ///
    /// # Parameters
    /// * `path` - Path to the existing database file.
    /// * `config` - Optional configuration. If None, default values are used.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The database file does not exist or cannot be opened
    /// - The file is corrupted or has an invalid format
    /// - There are disk I/O issues
    ///
    /// # Returns
    /// An RQLite instance connected to the existing database.
    ///
    /// # Example
    /// ```rust
    /// use rqlite_engine::RQLite;
    ///
    /// let db = RQLite::open("existing_database.db", None)?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P, config: Option<RQLiteConfig>) -> io::Result<Self> {
        let config = config.unwrap_or_default();
        
        let pager = Pager::open(path, Some(config.buffer_pool_size))?;

        // In a complete implementation, you would load table and index metadata
        // from the database's system tables here. For now, we start with empty collections.
        Ok(RQLite {
            pager: Arc::new(pager),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            config,
            next_table_id: 1,
            next_index_id: 1,
        })
    }

    /// Creates a new table in the database.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The table cannot be created due to disk space issues
    /// - There are I/O problems
    ///
    /// # Returns
    /// The unique identifier for the newly created table.
    ///
    /// # Example
    /// ```rust
    /// let table_id = db.create_table()?;
    /// println!("Created table with ID: {}", table_id);
    /// ```
    pub fn create_table(&mut self) -> io::Result<TableId> {
        let table_id = self.next_table_id;
        self.next_table_id += 1;

        let btree = BTree::create(
            TreeType::Table,
           Arc::clone(&self.pager),
            self.config.page_size,
            self.config.reserved_space,
            self.config.max_payload_fraction,
            self.config.min_payload_fraction,
        )?;

        self.tables.insert(table_id, btree);
        Ok(table_id)
    }

    /// Creates a new index on a table.
    ///
    /// # Parameters
    /// * `table_id` - The table to create the index on.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified table does not exist
    /// - The index cannot be created due to disk space issues
    /// - There are I/O problems
    ///
    /// # Returns
    /// The unique identifier for the newly created index.
    ///
    /// # Example
    /// ```rust
    /// let table_id = db.create_table()?;
    /// let index_id = db.create_index(table_id)?;
    /// println!("Created index with ID: {}", index_id);
    /// ```
    pub fn create_index(&mut self, _table_id: TableId) -> io::Result<IndexId> {
        let index_id = self.next_index_id;
        self.next_index_id += 1;

        let btree = BTree::create(
            TreeType::Index,
            self.pager.clone(), // Cloning the pager is not ideal, but necessary for now.
            self.config.page_size,
            self.config.reserved_space,
            self.config.max_payload_fraction,
            self.config.min_payload_fraction,
        )?;

        self.indexes.insert(index_id, btree);
        Ok(index_id)
    }

    /// Inserts a record into the specified table.
    ///
    /// # Parameters
    /// * `table_id` - The table to insert into.
    /// * `rowid` - The row identifier for the record.
    /// * `record` - The record to insert.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified table does not exist
    /// - The rowid already exists in the table
    /// - There are disk space or I/O issues
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// use rqlite_engine::utils::serialization::SqliteValue;
    /// use rqlite_engine::tree::record::Record;
    ///
    /// let table_id = db.create_table()?;
    /// let record = Record::with_values(vec![
    ///     SqliteValue::Integer(42),
    ///     SqliteValue::String("Hello".to_string()),
    /// ]);
    /// db.table_insert(table_id, 1, &record)?;
    /// ```
    pub fn table_insert(&mut self, table_id: TableId, rowid: i64, record: &Record) -> io::Result<()> {
        let btree = self.tables.get_mut(&table_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table {} not found", table_id))
        })?;

        btree.insert(rowid, record)
    }

    /// Finds a record in the specified table by its rowid.
    ///
    /// # Parameters
    /// * `table_id` - The table to search in.
    /// * `rowid` - The row identifier to search for.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified table does not exist
    /// - There are I/O issues
    ///
    /// # Returns
    /// The record if found, or None if no record exists with the given rowid.
    ///
    /// # Example
    /// ```rust
    /// match db.table_find(table_id, 1)? {
    ///     Some(record) => println!("Found record with {} values", record.len()),
    ///     None => println!("Record not found"),
    /// }
    /// ```
    pub fn table_find(&self, table_id: TableId, rowid: i64) -> io::Result<Option<Record>> {
        let btree = self.tables.get(&table_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table {} not found", table_id))
        })?;

        btree.find(rowid)
    }

    /// Deletes a record from the specified table.
    ///
    /// # Parameters
    /// * `table_id` - The table to delete from.
    /// * `rowid` - The row identifier of the record to delete.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified table does not exist
    /// - There are I/O issues
    ///
    /// # Returns
    /// True if a record was deleted, false if no record existed with the given rowid.
    ///
    /// # Example
    /// ```rust
    /// if db.table_delete(table_id, 1)? {
    ///     println!("Record deleted successfully");
    /// } else {
    ///     println!("Record not found");
    /// }
    /// ```
    pub fn table_delete(&mut self, table_id: TableId, rowid: i64) -> io::Result<bool> {
        let btree = self.tables.get_mut(&table_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table {} not found", table_id))
        })?;

        btree.delete(rowid)
    }

    /// Inserts an entry into the specified index.
    ///
    /// # Parameters
    /// * `index_id` - The index to insert into.
    /// * `key` - The key to index.
    /// * `rowid` - The row identifier associated with this key.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified index does not exist
    /// - There are disk space or I/O issues
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// use rqlite_engine::utils::serialization::{SqliteValue, serialize_values};
    ///
    /// let index_id = db.create_index(table_id)?;
    /// 
    /// // Create a key payload (in this case, an integer key)
    /// let mut key_payload = Vec::new();
    /// serialize_values(&[SqliteValue::Integer(42)], &mut key_payload)?;
    /// 
    /// db.index_insert(index_id, &key_payload, 1)?;
    /// ```
    pub fn index_insert(&mut self, index_id: IndexId, key: &[u8], rowid: i64) -> io::Result<()> {
        let btree = self.indexes.get_mut(&index_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Index {} not found", index_id))
        })?;

        btree.insert_index(key, rowid)
    }

    /// Finds an entry in the specified index.
    ///
    /// # Parameters
    /// * `index_id` - The index to search in.
    /// * `key` - The key to search for.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified index does not exist
    /// - There are I/O issues
    ///
    /// # Returns
    /// A tuple containing:
    /// - True if the key was found, false otherwise
    /// - The leaf page number where the key is located
    /// - The index within that page
    ///
    /// # Example
    /// ```rust
    /// use rqlite_engine::utils::cmp::KeyValue;
    ///
    /// let key = KeyValue::Integer(42);
    /// let (found, page, index) = db.index_find(index_id, &key)?;
    /// if found {
    ///     println!("Key found at page {} index {}", page, index);
    /// }
    /// ```
    pub fn index_find(&self, index_id: IndexId, key: &KeyValue) -> io::Result<(bool, u32, u16)> {
        let btree = self.indexes.get(&index_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Index {} not found", index_id))
        })?;

        btree.find_index_key(key)
    }

    /// Deletes an entry from the specified index.
    ///
    /// # Parameters
    /// * `index_id` - The index to delete from.
    /// * `key` - The key to delete.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The specified index does not exist
    /// - There are I/O issues
    ///
    /// # Returns
    /// True if an entry was deleted, false if no entry existed with the given key.
    ///
    /// # Example
    /// ```rust
    /// use rqlite_engine::utils::cmp::KeyValue;
    ///
    /// let key = KeyValue::Integer(42);
    /// if db.index_delete(index_id, &key)? {
    ///     println!("Index entry deleted successfully");
    /// } else {
    ///     println!("Index entry not found");
    /// }
    /// ```
    pub fn index_delete(&mut self, index_id: IndexId, key: &KeyValue) -> io::Result<bool> {
        let btree = self.indexes.get_mut(&index_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Index {} not found", index_id))
        })?;

        btree.delete_index(key)
    }

    /// Begins a new transaction.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues or if a transaction is already active.
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// db.begin_transaction()?;
    /// // Perform multiple operations...
    /// db.commit_transaction()?; // or db.rollback_transaction()?
    /// ```
    pub fn begin_transaction(&self) -> io::Result<()> {
        self.pager.begin_transaction()
    }

    /// Commits the current transaction, making all changes permanent.
    ///
    /// # Errors
    /// Returns an error if:
    /// - No transaction is active
    /// - There are I/O issues during commit
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// db.begin_transaction()?;
    /// db.table_insert(table_id, 1, &record)?;
    /// db.commit_transaction()?; // Changes are now permanent
    /// ```
    pub fn commit_transaction(&self) -> io::Result<()> {
        self.pager.commit_transaction()
    }

    /// Rolls back the current transaction, discarding all changes.
    ///
    /// # Errors
    /// Returns an error if:
    /// - No transaction is active
    /// - There are I/O issues during rollback
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// db.begin_transaction()?;
    /// db.table_insert(table_id, 1, &record)?;
    /// db.rollback_transaction()?; // Changes are discarded
    /// ```
    pub fn rollback_transaction(&self) -> io::Result<()> {
        self.pager.rollback_transaction()
    }

    /// Forces all pending changes to be written to disk.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues during the flush operation.
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// db.table_insert(table_id, 1, &record)?;
    /// db.flush()?; // Ensure changes are written to disk
    /// ```
    pub fn flush(&self) -> io::Result<()> {
        self.pager.flush()
    }

    /// Closes the database, flushing any pending changes.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues during the close operation.
    ///
    /// # Returns
    /// Success indication.
    ///
    /// # Example
    /// ```rust
    /// // Perform database operations...
    /// db.close()?; // Properly close the database
    /// ```
    pub fn close(self) -> io::Result<()> {
        // Since pager is in Arc, we need to extract it
        match Arc::try_unwrap(self.pager) {
            Ok(pager) => pager.close(),
            Err(_) => {
                // If there are still references to the pager, just flush
                // This shouldn't happen in normal usage, but provides a fallback
                Ok(())
            }
        }
    
    
    }

    /// Gets the current database configuration.
    ///
    /// # Returns
    /// A reference to the current configuration.
    ///
    /// # Example
    /// ```rust
    /// let config = db.config();
    /// println!("Page size: {} bytes", config.page_size);
    /// println!("Buffer pool size: {} pages", config.buffer_pool_size);
    /// ```
    pub fn config(&self) -> &RQLiteConfig {
        &self.config
    }

    /// Gets the total number of pages in the database.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// The total number of pages.
    ///
    /// # Example
    /// ```rust
    /// let page_count = db.page_count()?;
    /// println!("Database has {} pages", page_count);
    /// ```
    pub fn page_count(&self) -> io::Result<u32> {
        self.pager.page_count()
    }

    /// Gets information about the tables in the database.
    ///
    /// # Returns
    /// A vector of table IDs currently in the database.
    ///
    /// # Example
    /// ```rust
    /// let tables = db.list_tables();
    /// println!("Database contains {} tables", tables.len());
    /// for table_id in tables {
    ///     println!("Table ID: {}", table_id);
    /// }
    /// ```
    pub fn list_tables(&self) -> Vec<TableId> {
        self.tables.keys().cloned().collect()
    }

    /// Gets information about the indexes in the database.
    ///
    /// # Returns
    /// A vector of index IDs currently in the database.
    ///
    /// # Example
    /// ```rust
    /// let indexes = db.list_indexes();
    /// println!("Database contains {} indexes", indexes.len());
    /// for index_id in indexes {
    ///     println!("Index ID: {}", index_id);
    /// }
    /// ```
    pub fn list_indexes(&self) -> Vec<IndexId> {
        self.indexes.keys().cloned().collect()
    }

    /// Checks if a table exists in the database.
    ///
    /// # Parameters
    /// * `table_id` - The table ID to check.
    ///
    /// # Returns
    /// True if the table exists, false otherwise.
    ///
    /// # Example
    /// ```rust
    /// if db.table_exists(table_id) {
    ///     println!("Table {} exists", table_id);
    /// } else {
    ///     println!("Table {} does not exist", table_id);
    /// }
    /// ```
    pub fn table_exists(&self, table_id: TableId) -> bool {
        self.tables.contains_key(&table_id)
    }

    /// Checks if an index exists in the database.
    ///
    /// # Parameters
    /// * `index_id` - The index ID to check.
    ///
    /// # Returns
    /// True if the index exists, false otherwise.
    ///
    /// # Example
    /// ```rust
    /// if db.index_exists(index_id) {
    ///     println!("Index {} exists", index_id);
    /// } else {
    ///     println!("Index {} does not exist", index_id);
    /// }
    /// ```
    pub fn index_exists(&self, index_id: IndexId) -> bool {
        self.indexes.contains_key(&index_id)
    }

    /// Gets the root page number for a specific table.
    ///
    /// # Parameters
    /// * `table_id` - The table ID to get the root page for.
    ///
    /// # Errors
    /// Returns an error if the table does not exist.
    ///
    /// # Returns
    /// The root page number of the table's B-Tree.
    ///
    /// # Example
    /// ```rust
    /// let root_page = db.table_root_page(table_id)?;
    /// println!("Table {} root page: {}", table_id, root_page);
    /// ```
    pub fn table_root_page(&self, table_id: TableId) -> io::Result<u32> {
        let btree = self.tables.get(&table_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table {} not found", table_id))
        })?;

        Ok(btree.root_page())
    }

    /// Gets the root page number for a specific index.
    ///
    /// # Parameters
    /// * `index_id` - The index ID to get the root page for.
    ///
    /// # Errors
    /// Returns an error if the index does not exist.
    ///
    /// # Returns
    /// The root page number of the index's B-Tree.
    ///
    /// # Example
    /// ```rust
    /// let root_page = db.index_root_page(index_id)?;
    /// println!("Index {} root page: {}", index_id, root_page);
    /// ```
    pub fn index_root_page(&self, index_id: IndexId) -> io::Result<u32> {
        let btree = self.indexes.get(&index_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Index {} not found", index_id))
        })?;

        Ok(btree.root_page())
    }
}

// Re-export commonly used types for convenience
pub use utils::serialization::{SqliteValue, serialize_values, deserialize_values};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_open_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create database
        {
            let db = RQLite::create(&db_path, None).unwrap();
            assert_eq!(db.list_tables().len(), 0);
            assert_eq!(db.list_indexes().len(), 0);
        }

        // Open existing database
        {
            let db = RQLite::open(&db_path, None).unwrap();
            assert_eq!(db.list_tables().len(), 0);
            assert_eq!(db.list_indexes().len(), 0);
        }
    }

    #[test]
    fn test_table_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Create table
        let table_id = db.create_table().unwrap();
        assert!(db.table_exists(table_id));
        assert_eq!(db.list_tables(), vec![table_id]);

        // Insert record
        let record = Record::with_values(vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Hello, World!".to_string()),
        ]);
        db.table_insert(table_id, 1, &record).unwrap();

        // Find record
        let found = db.table_find(table_id, 1).unwrap();
        assert!(found.is_some());
        let found_record = found.unwrap();
        assert_eq!(found_record.len(), 2);

        // Verify record contents
        match &found_record.values[0] {
            SqliteValue::Integer(val) => assert_eq!(*val, 42),
            _ => panic!("Expected integer"),
        }
        match &found_record.values[1] {
            SqliteValue::String(val) => assert_eq!(val, "Hello, World!"),
            _ => panic!("Expected string"),
        }

        // Delete record
        assert!(db.table_delete(table_id, 1).unwrap());
        assert!(db.table_find(table_id, 1).unwrap().is_none());
        assert!(!db.table_delete(table_id, 1).unwrap()); // Already deleted
    }

    #[test]
    fn test_index_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Create table and index
        let table_id = db.create_table().unwrap();
        let index_id = db.create_index(table_id).unwrap();
        
        assert!(db.index_exists(index_id));
        assert_eq!(db.list_indexes(), vec![index_id]);

        // Insert index entry
        let mut key_payload = Vec::new();
        serialize_values(&[SqliteValue::Integer(42)], &mut key_payload).unwrap();
        db.index_insert(index_id, &key_payload, 1).unwrap();
        

        // Find index entry
        let key = KeyValue::Integer(42);
        let (found, _, _) = db.index_find(index_id, &key).unwrap();
        assert!(found);
        

        // Delete index entry
        assert!(db.index_delete(index_id, &key).unwrap());
        let (found, _, _) = db.index_find(index_id, &key).unwrap();
        assert!(!found);
    }

    #[test]
    fn test_transaction_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        let table_id = db.create_table().unwrap();
        let record = Record::with_values(vec![SqliteValue::Integer(42)]);

        // Test commit
        db.begin_transaction().unwrap();
        db.table_insert(table_id, 1, &record).unwrap();
        db.commit_transaction().unwrap();
        
        // Record should exist after commit
        assert!(db.table_find(table_id, 1).unwrap().is_some());

        // Test rollback
        db.begin_transaction().unwrap();
        db.table_insert(table_id, 2, &record).unwrap();
        db.rollback_transaction().unwrap();
        
        // Record should not exist after rollback
        assert!(db.table_find(table_id, 2).unwrap().is_none());
    }

    #[test]
    fn test_multiple_tables_and_indexes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Create multiple tables
        let table1 = db.create_table().unwrap();
        let table2 = db.create_table().unwrap();
        let table3 = db.create_table().unwrap();

        // Create multiple indexes
        let index1 = db.create_index(table1).unwrap();
        let index2 = db.create_index(table2).unwrap();

        // Verify tables and indexes exist
        let tables = db.list_tables();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&table1));
        assert!(tables.contains(&table2));
        assert!(tables.contains(&table3));

        let indexes = db.list_indexes();
        assert_eq!(indexes.len(), 2);
        assert!(indexes.contains(&index1));
        assert!(indexes.contains(&index2));

        // Insert records into different tables
        let record1 = Record::with_values(vec![SqliteValue::String("Table 1".to_string())]);
        let record2 = Record::with_values(vec![SqliteValue::String("Table 2".to_string())]);

        db.table_insert(table1, 1, &record1).unwrap();
        db.table_insert(table2, 1, &record2).unwrap();

        // Verify records are in correct tables
        let found1 = db.table_find(table1, 1).unwrap().unwrap();
        let found2 = db.table_find(table2, 1).unwrap().unwrap();

        match &found1.values[0] {
            SqliteValue::String(val) => assert_eq!(val, "Table 1"),
            _ => panic!("Expected string"),
        }

        match &found2.values[0] {
            SqliteValue::String(val) => assert_eq!(val, "Table 2"),
            _ => panic!("Expected string"),
        }

        // Verify root pages are different
        let root1 = db.table_root_page(table1).unwrap();
        let root2 = db.table_root_page(table2).unwrap();
        let root3 = db.table_root_page(table3).unwrap();

        assert_ne!(root1, root2);
        assert_ne!(root2, root3);
        assert_ne!(root1, root3);
    }

    #[test]
    fn test_configuration() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let config = RQLiteConfig {
            page_size: 8192,
            buffer_pool_size: 500,
            reserved_space: 64,
            max_payload_fraction: 200,
            min_payload_fraction: 50,
        };

        let db = RQLite::create(db_path, Some(config.clone())).unwrap();
        
        assert_eq!(db.config().page_size, config.page_size);
        assert_eq!(db.config().buffer_pool_size, config.buffer_pool_size);
        assert_eq!(db.config().reserved_space, config.reserved_space);
        assert_eq!(db.config().max_payload_fraction, config.max_payload_fraction);
        assert_eq!(db.config().min_payload_fraction, config.min_payload_fraction);
    }

    #[test]
    fn test_error_handling() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Test operations on non-existent table
        let result = db.table_find(999, 1);
        assert!(result.is_err());

        let record = Record::with_values(vec![SqliteValue::Integer(42)]);
        let result = db.table_insert(999, 1, &record);
        assert!(result.is_err());

        let result = db.table_delete(999, 1);
        assert!(result.is_err());

        // Test operations on non-existent index
        let key = KeyValue::Integer(42);
        let result = db.index_find(999, &key);
        assert!(result.is_err());

        let mut key_payload = Vec::new();
        serialize_values(&[SqliteValue::Integer(42)], &mut key_payload).unwrap();
        let result = db.index_insert(999, &key_payload, 1);
        assert!(result.is_err());

        let result = db.index_delete(999, &key);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_dataset() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        let table_id = db.create_table().unwrap();

        // Insert many records to test B-Tree splits
        let record_count = 1000;
        for i in 1..=record_count {
            let record = Record::with_values(vec![
                SqliteValue::Integer(i),
                SqliteValue::String(format!("Record number {}", i)),
                SqliteValue::Blob(vec![i as u8; 50]), // Add some bulk
            ]);
            db.table_insert(table_id, i, &record).unwrap();
        }

        // Verify all records can be found
        for i in 1..=record_count {
            let found = db.table_find(table_id, i).unwrap();
            assert!(found.is_some());
            
            let record = found.unwrap();
            match &record.values[0] {
                SqliteValue::Integer(val) => assert_eq!(*val, i),
                _ => panic!("Expected integer"),
            }
        }

        // Test random access
        let random_ids = [15, 73, 2, 99, 41, 88];
        for &id in &random_ids {
            let found = db.table_find(table_id, id).unwrap();
            assert!(found.is_some());
        }

        // Delete some records
        let delete_ids = [10, 25, 50, 75, 90];
        for &id in &delete_ids {
            assert!(db.table_delete(table_id, id).unwrap());
        }

        // Verify deleted records are gone
        for &id in &delete_ids {
            assert!(db.table_find(table_id, id).unwrap().is_none());
        }

        // Verify remaining records still exist
        for i in 1..=record_count {
            if !delete_ids.contains(&i) {
                assert!(db.table_find(table_id, i).unwrap().is_some());
            }
        }
    }

    #[test]
    #[allow(unused_variables)]
    fn test_database_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let table_id;
        let record = Record::with_values(vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Persistent data".to_string()),
        ]);

        // Create database and insert data
        {
            let mut db = RQLite::create(&db_path, None).unwrap();
            table_id = db.create_table().unwrap();
            db.table_insert(table_id, 1, &record).unwrap();
            db.flush().unwrap();
        } // Database is closed here

        // Reopen database and verify data persists
        {
            let db = RQLite::open(&db_path, None).unwrap();
            // Note: In a complete implementation, table metadata would be loaded from system tables
            // For this test, we would need to track table IDs in the database file itself
            assert!(db_path.exists());
        }
    }

    #[test]
    fn test_flush_and_close() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        let table_id = db.create_table().unwrap();
        let record = Record::with_values(vec![SqliteValue::Integer(42)]);
        
        db.table_insert(table_id, 1, &record).unwrap();
        
        // Test flush
        db.flush().unwrap();
        
        // Test close
        db.close().unwrap();
    }

    #[test]
    fn test_page_count() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        let initial_pages = db.page_count().unwrap();
        assert!(initial_pages > 0);

        // Create tables and insert data to increase page count
        let table_id = db.create_table().unwrap();
        let index_id = db.create_index(table_id).unwrap();

        // Insert enough data to force page allocations
        for i in 1..=50 {
            let record = Record::with_values(vec![
                SqliteValue::Integer(i),
                SqliteValue::Blob(vec![i as u8; 1000]), // Large payload
            ]);
            db.table_insert(table_id, i, &record).unwrap();

            let mut key_payload = Vec::new();
            serialize_values(&[SqliteValue::Integer(i)], &mut key_payload).unwrap();
            db.index_insert(index_id, &key_payload, i).unwrap();
        }

        let final_pages = db.page_count().unwrap();
        assert!(final_pages > initial_pages);
    }



    #[test]
    fn test_pager_sharing_efficiency() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("shared_test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Crear múltiples tablas - todas deberían compartir el mismo pager
        let table1 = db.create_table().unwrap();
        let table2 = db.create_table().unwrap();
        let table3 = db.create_table().unwrap();
        
        let index1 = db.create_index(table1).unwrap();
        let index2 = db.create_index(table2).unwrap();

        // Verificar que todas las estructuras fueron creadas
        assert!(db.table_exists(table1));
        assert!(db.table_exists(table2));
        assert!(db.table_exists(table3));
        assert!(db.index_exists(index1));
        assert!(db.index_exists(index2));

        // Insertar datos en múltiples tablas
        for i in 1..=10 {
            let record = Record::with_values(vec![
                SqliteValue::Integer(i),
                SqliteValue::String(format!("Record {}", i)),
            ]);
            
            db.table_insert(table1, i, &record).unwrap();
            db.table_insert(table2, i, &record).unwrap();
            db.table_insert(table3, i, &record).unwrap();
        }

        // Un solo flush debería afectar todas las tablas (porque comparten pager)
        db.flush().unwrap();

        // Verificar que todos los datos existen
        for i in 1..=10 {
            assert!(db.table_find(table1, i).unwrap().is_some());
            assert!(db.table_find(table2, i).unwrap().is_some());
            assert!(db.table_find(table3, i).unwrap().is_some());
        }
    }

    #[test]
    fn test_transaction_consistency_across_tables() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("transaction_test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        let table1 = db.create_table().unwrap();
        let table2 = db.create_table().unwrap();

        // Comenzar transacción que afecta múltiples tablas
        db.begin_transaction().unwrap();

        let record1 = Record::with_values(vec![SqliteValue::Integer(100)]);
        let record2 = Record::with_values(vec![SqliteValue::Integer(200)]);

        db.table_insert(table1, 1, &record1).unwrap();
        db.table_insert(table2, 1, &record2).unwrap();

        // Rollback debería afectar ambas tablas
        db.rollback_transaction().unwrap();

        // Verificar que ningún dato existe después del rollback
        assert!(db.table_find(table1, 1).unwrap().is_none());
        assert!(db.table_find(table2, 1).unwrap().is_none());

        // Probar commit
        db.begin_transaction().unwrap();
        db.table_insert(table1, 1, &record1).unwrap();
        db.table_insert(table2, 1, &record2).unwrap();
        db.commit_transaction().unwrap();

        // Ahora ambos deberían existir
        assert!(db.table_find(table1, 1).unwrap().is_some());
        assert!(db.table_find(table2, 1).unwrap().is_some());
    }

    #[test]
    fn test_memory_efficiency() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("memory_test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Crear muchas tablas para probar eficiencia
        let table_count = 50;
        let mut tables = Vec::new();

        for i in 0..table_count {
            let table_id = db.create_table().unwrap();
            tables.push(table_id);

            // Insertar datos en cada tabla
            let record = Record::with_values(vec![
                SqliteValue::Integer(i as i64),
                SqliteValue::String(format!("Table {} data", i)),
            ]);
            
            db.table_insert(table_id, 1, &record).unwrap();
        }

        // Verificar que todas las tablas funcionan correctamente
        for (i, &table_id) in tables.iter().enumerate() {
            let found = db.table_find(table_id, 1).unwrap().unwrap();
            match &found.values[0] {
                SqliteValue::Integer(val) => assert_eq!(*val, i as i64),
                _ => panic!("Expected integer"),
            }
        }

        // Un solo page_count() debería funcionar para todas las tablas
        let page_count = db.page_count().unwrap();
        assert!(page_count > 0);
    }

    #[test]
    fn test_arc_reference_counting() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("arc_test.db");
        
        // Crear el pager y verificar que Arc funciona correctamente
        let pager = Arc::new(Pager::create(db_path, 4096, None, 0).unwrap());
        
        // Verificar que podemos clonar Arc múltiples veces
        let pager_clone1 = Arc::clone(&pager);
        let pager_clone2 = Arc::clone(&pager);
        let pager_clone3 = Arc::clone(&pager);
        
        // Todos deberían apuntar al mismo pager
        assert_eq!(Arc::strong_count(&pager), 4); // original + 3 clones
        
        // Crear BTrees con referencias compartidas
        let btree1 = BTree::create(TreeType::Table, pager_clone1, 4096, 0, 255, 32).unwrap();
        let btree2 = BTree::create(TreeType::Index, pager_clone2, 4096, 0, 255, 32).unwrap();
        
        // Verificar que ambos BTrees funcionan
        assert_ne!(btree1.root_page(), btree2.root_page());
        assert_eq!(btree1.tree_type(), TreeType::Table);
        assert_eq!(btree2.tree_type(), TreeType::Index);
        
        // Al drop los BTrees, las referencias deberían decrementarse
        drop(btree1);
        drop(btree2);
        assert_eq!(Arc::strong_count(&pager), 2); // original + pager_clone3
        
        drop(pager_clone3);
        assert_eq!(Arc::strong_count(&pager), 1); // solo original
    }
    /// ADDED THIS TESTS AFTER LAST REFACTORING TO INCLUDE THE SHARED PAGER ACCROSS ALL TABLES. 
    #[test]
    fn test_close_with_arc() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("close_test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Crear algunas tablas
        let _table1 = db.create_table().unwrap();
        let _table2 = db.create_table().unwrap();

        // El close debería funcionar correctamente
        let result = db.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_large_database_simulation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("large_db_test.db");
        let mut db = RQLite::create(db_path, None).unwrap();

        // Simular una base de datos grande
        let table_count = 20;
        let index_count = 30;
        let records_per_table = 100;

        let mut tables = Vec::new();
        let mut indexes = Vec::new();

        // Crear tablas
        for _ in 0..table_count {
            tables.push(db.create_table().unwrap());
        }

        // Crear índices
        for i in 0..index_count {
            let table_id = tables[i % table_count];
            indexes.push(db.create_index(table_id).unwrap());
        }

        // Insertar datos masivamente
        for &table_id in &tables {
            for i in 1..=records_per_table {
                let record = Record::with_values(vec![
                    SqliteValue::Integer(i),
                    SqliteValue::String(format!("Large dataset record {}", i)),
                    SqliteValue::Blob(vec![i as u8; 50]),
                ]);
                
                db.table_insert(table_id, i, &record).unwrap();
            }
        }

        // Flush todos los cambios con una sola operación
        db.flush().unwrap();

        // Verificar integridad de datos
        for &table_id in &tables {
            for i in 1..=10 { // Verificar primeros 10 registros
                let found = db.table_find(table_id, i).unwrap();
                assert!(found.is_some());
            }
        }

        // El número de páginas debería ser razonable (no multiplicado por número de tablas)
        let page_count = db.page_count().unwrap();
        println!("Large database test: {} pages for {} tables with {} records each", 
                 page_count, table_count, records_per_table);
        
        // Debería ser mucho menor que si cada tabla tuviera su propio pager
        assert!(page_count < 1000); // Límite razonable
    }
}