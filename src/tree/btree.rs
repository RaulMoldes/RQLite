//! # B-Tree Implementation Module
//!
//! This module implements the complete B-Tree structure for SQLite,
//! providing operations for creating, searching, inserting, and deleting.
//! It supports both table B-trees (which store rows) and index B-trees
//! (which store indexed values).
//!
//! In the future, I would like to improve this implementation to avoid having a copy of the ẁhole pager for all the BTrees.
//! This is quite inefficient, but for now I do not want to refactor the whole thing.
//!
//! Apart from that I am quite happy with the current solution.

use std::io;
use std::sync::Arc;

use crate::page::{BTreeCell, PageType};
use crate::storage::pager::Pager;
use crate::tree::cell::BTreeCellFactory;
use crate::tree::node::{extract_key_from_payload, BTreeNode};
use crate::tree::record::Record;
use crate::utils::cmp::KeyValue;
use std::hash::Hasher;

/// Represents a B-Tree in SQLite.
///
/// A B-Tree is a self-balancing tree data structure that maintains
/// sorted data and allows searches, insertions, and deletions in
/// logarithmic time.
///
/// This implementation uses a callback-based approach for node access
/// to ensure safe memory management and proper cleanup.
pub struct BTree {
    /// Number of the root page of the tree
    root_page: u32,
    /// Type of B-Tree (table or index)
    tree_type: TreeType,
    /// Reference to the pager for I/O operations
    pager: Arc<Pager>, // Decided to use Arc for Pager to avoid cloning it everywhere
    /// Size of each page in bytes
    page_size: u32,
    /// Reserved space at the end of each page
    reserved_space: u8,
    /// Maximum fraction of page that can be used by a payload
    max_payload_fraction: u8,
    /// Minimum fraction of page that must be used by a payload
    min_payload_fraction: u8,
}

/// Type of B-Tree
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeType {
    /// B-Tree for a table (stores rows)
    Table,
    /// B-Tree for an index (stores index entries)
    Index,
}

impl BTree {
    /// Creates a new B-Tree instance.
    ///
    /// # Parameters
    /// * `root_page` - Number of the root page.
    /// * `tree_type` - Type of the tree (table or index).
    /// * `pager` - Pager for I/O operations.
    /// * `page_size` - Size of page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a payload.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a payload.
    ///
    /// # Returns
    /// A new B-Tree instance.
    pub fn new(
        root_page: u32,
        tree_type: TreeType,
        pager: Arc<Pager>,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> Self {
        BTree {
            root_page,
            tree_type,
            pager,
            page_size,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
        }
    }

    /// Creates a new empty B-Tree.
    ///
    /// # Parameters
    /// * `tree_type` - Type of tree (table or index).
    /// * `pager` - Pager for I/O operations.
    /// * `page_size` - Size of page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a payload.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a payload.
    ///
    /// # Errors
    /// Returns an error if the root page cannot be created.
    ///
    /// # Returns
    /// A new B-Tree instance.
    pub fn create(
        tree_type: TreeType,
        pager: Arc<Pager>,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> io::Result<Self> {
        // Create the root page (always a leaf)
        let page_type = match tree_type {
            TreeType::Table => PageType::TableLeaf,
            TreeType::Index => PageType::IndexLeaf,
        };

        // Create the root node
        let node = BTreeNode::create_leaf(page_type, &pager)?;
        let root_page = node.page_number;

        Ok(BTree {
            root_page,
            tree_type,
            pager,
            page_size,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
        })
    }

    /// Opens an existing B-Tree.
    ///
    /// # Parameters
    /// * `root_page` - Number of the root page.
    /// * `tree_type` - Type of tree (table or index).
    /// * `pager` - Pager for I/O operations.
    /// * `page_size` - Size of page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a payload.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a payload.
    ///
    /// # Errors
    /// Returns an error if the root page does not exist or is not valid.
    ///
    /// # Returns
    /// A B-Tree instance representing the existing tree.
    pub fn open(
        root_page: u32,
        tree_type: TreeType,
        pager: Arc<Pager>,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> io::Result<Self> {
        // Verify that the root page exists and is valid
        let expected_page_type = match tree_type {
            TreeType::Table => PageType::TableLeaf, // Could be TableInterior if tree has grown
            TreeType::Index => PageType::IndexLeaf, // Could be IndexInterior if tree has grown
        };

        // Try to open the root page - this will validate that it exists and is the right type
        let _root_node = pager.get_page_callback(root_page, Some(expected_page_type), |page| {
            let actual_type = page.page_type();

            // For table trees, accept both leaf and interior as valid root types
            let is_valid = match (tree_type, actual_type) {
                (TreeType::Table, PageType::TableLeaf)
                | (TreeType::Table, PageType::TableInterior)
                | (TreeType::Index, PageType::IndexLeaf)
                | (TreeType::Index, PageType::IndexInterior) => true,
                _ => false,
            };

            if !is_valid {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Root page type {:?} does not match tree type {:?}",
                        actual_type, tree_type
                    ),
                ));
            }

            Ok(())
        })?;

        Ok(BTree {
            root_page,
            tree_type,
            pager,
            page_size,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
        })
    }

    /// Gets the maximum payload size that can be stored locally in a page.
    ///
    /// # Returns
    /// Maximum payload size in bytes.
    fn max_local_payload(&self) -> usize {
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        BTreeCellFactory::max_local_payload(usable_size, self.max_payload_fraction)
    }

    /// Gets the minimum payload size that must be stored locally in a page.
    ///
    /// # Returns
    /// Minimum payload size in bytes.
    fn min_local_payload(&self) -> usize {
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        BTreeCellFactory::min_local_payload(usable_size, self.min_payload_fraction)
    }

    /// Gets the usable size of a page (excluding reserved space).
    ///
    /// # Returns
    /// Usable size in bytes.
    fn usable_page_size(&self) -> usize {
        self.page_size as usize - self.reserved_space as usize
    }

    /// Finds a record in a table B-Tree by its rowid.
    ///
    /// # Parameters
    /// * `rowid` - Row ID to search for.
    ///
    /// # Errors
    /// Returns an error if the tree is not a table tree or if there are I/O issues.
    ///
    /// # Returns
    /// The record found, or `None` if no record exists with the given rowid.
    pub fn find(&self, rowid: i64) -> io::Result<Option<Record>> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot find record in an index tree",
            ));
        }

        // Start at the root page
        let mut current_page = self.root_page;
        // println!("Finding rowid {} in B-Tree", rowid);
        // println!("Starting at root page {}", current_page);
        // Traverse the tree until we reach a leaf node
        loop {
            let current_type = self.get_page_type(current_page)?;

            if current_type.is_leaf() {
                // We're at a leaf node, look for the key
                let leaf_node = BTreeNode::new(current_page, current_type);
                let (found, idx) = leaf_node.find_table_rowid(rowid, &self.pager)?;

                if !found {
                    // Key not found
                    return Ok(None);
                }

                // Get the record from the cell
                let cell = leaf_node.get_cell_owned(idx, &self.pager)?;

                match cell {
                    BTreeCell::TableLeaf(leaf_cell) => {
                        // Get payload from the cell
                        let mut payload = leaf_cell.payload.clone();

                        // Handle overflow chain if present
                        if let Some(overflow_page) = leaf_cell.overflow_page {
                            payload.extend_from_slice(&self.read_overflow_chain(overflow_page)?);
                        }

                        // Deserialize the record
                        let (record, _) = Record::from_bytes(&payload)?;
                        return Ok(Some(record));
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Expected a table leaf cell",
                        ));
                    }
                }
            } else {
                // Interior node - find the child that may contain the key
                let interior_node = BTreeNode::new(current_page, current_type);
                let (_, child_page, _) = interior_node.find_table_key(rowid, &self.pager)?;

                current_page = child_page;
                // println!("Traversing to child page {}", current_page);
            }
        }
    }

    /// Finds a key in an index B-Tree.
    ///
    /// # Parameters
    /// * `key` - Key to search for.
    ///
    /// # Errors
    /// Returns an error if the tree is not an index tree or if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - `true` if the key was found, `false` otherwise
    /// - leaf page where the key is or should be
    /// - index of the cell in the leaf page
    pub fn find_index_key(&self, key: &KeyValue) -> io::Result<(bool, u32, u16)> {
        if self.tree_type != TreeType::Index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot find index key in a table tree",
            ));
        }

        // Start at the root page
        let mut current_page = self.root_page;

        // If we are empty, return immediately
        let root_type = self.get_page_type(current_page)?;
        if root_type.is_leaf() {
            let root_node = BTreeNode::new(current_page, root_type);
            let cell_count = root_node.cell_count(&self.pager)?;
            if cell_count == 0 {
                return Ok((false, current_page, 0));
            }
        }

        // Traverse the tree until we reach a leaf node
        loop {
            let current_type = self.get_page_type(current_page)?;

            if current_type.is_leaf() {
                // We're at a leaf node, look for the key
                let leaf_node = BTreeNode::new(current_page, current_type);
                let (found, idx) = leaf_node.find_index_key(key, &self.pager)?;
                return Ok((found, current_page, idx));
            } else {
                // Interior node - find the child that may contain the key
                let interior_node = BTreeNode::new(current_page, current_type);
                let (found, idx) = interior_node.find_index_key(key, &self.pager)?;

                let child_page = if found {
                    // If we found the key in an interior node, follow the left child
                    let cell = interior_node.get_cell_owned(idx, &self.pager)?;
                    match cell {
                        BTreeCell::IndexInterior(interior_cell) => interior_cell.left_child_page,
                        _ => unreachable!("Expected an index interior cell"),
                    }
                } else {
                    let cell_count = interior_node.cell_count(&self.pager)?;

                    if idx == 0 {
                        // The key is smaller than all keys in this node
                        let cell = interior_node.get_cell_owned(0, &self.pager)?;
                        match cell {
                            BTreeCell::IndexInterior(interior_cell) => {
                                interior_cell.left_child_page
                            }
                            _ => unreachable!("Expected an index interior cell"),
                        }
                    } else if idx >= cell_count {
                        // The key is larger than all keys in this node
                        interior_node.get_right_most_child(&self.pager)?
                    } else {
                        // The key falls between two keys
                        let cell = interior_node.get_cell_owned(idx, &self.pager)?;
                        match cell {
                            BTreeCell::IndexInterior(interior_cell) => {
                                interior_cell.left_child_page
                            }
                            _ => unreachable!("Expected an index interior cell"),
                        }
                    }
                };

                current_page = child_page;
            }
        }
    }

    /// Inserts a record into a table B-Tree.
    ///
    /// # Parameters
    /// * `rowid` - Row ID for the record.
    /// * `record` - Record to insert.
    ///
    /// # Errors
    /// Returns an error if the tree is not a table tree or if there are I/O issues.
    pub fn insert(&mut self, rowid: i64, record: &Record) -> io::Result<()> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert record into an index tree",
            ));
        }

        // Serialize the record
        let payload = record.to_bytes()?;

        // Create a table leaf cell
        let (cell, overflow_data) = BTreeCellFactory::create_table_leaf_cell(
            rowid,
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;

        // Handle overflow if needed
        let cell = if let Some(overflow_data) = overflow_data {
            match cell {
                BTreeCell::TableLeaf(mut leaf_cell) => {
                    // Create overflow pages for the overflow data
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    leaf_cell.overflow_page = Some(overflow_page);
                    BTreeCell::TableLeaf(leaf_cell)
                }
                _ => unreachable!("Expected a table leaf cell"),
            }
        } else {
            cell
        };

        // Find the leaf node where the record should be inserted
        let (leaf_page, path) = self.find_leaf_for_insert_table(rowid)?;
        let leaf_node = BTreeNode::new(leaf_page, PageType::TableLeaf);

        // Try to insert the cell
        let (split, median_key, new_node) = leaf_node.insert_cell_ordered(cell, &self.pager)?;
        // println!("Insert cell: split={}, median_key={:?}", split, median_key);
        
        if split {
            // Propagate the split up the tree
            self.propagate_split_table(leaf_node, new_node.unwrap(), median_key.unwrap(), path)?;
        }

        Ok(())
    }

    /// Inserts a key into an index B-Tree.
    ///
    /// # Parameters
    /// * `key` - Key to insert (usually a serialized form of the indexed column).
    /// * `rowid` - Row ID associated with the key.
    ///
    /// # Errors
    /// Returns an error if the tree is not an index tree or if there are I/O issues.
    pub fn insert_index(&mut self, key: &[u8], rowid: i64) -> io::Result<()> {
        if self.tree_type != TreeType::Index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert index entry into a table tree",
            ));
        }

        // Extract key value for comparison
        let key_value = extract_key_from_payload(key)?;

        // Create a payload containing the key and rowid
        let mut payload = key.to_vec();

        // Add the rowid at the end of the payload
        let rowid_record =
            Record::with_values(vec![crate::utils::serialization::SqliteValue::Integer(
                rowid,
            )]);
        let rowid_bytes = rowid_record.to_bytes()?;
        payload.extend_from_slice(&rowid_bytes);

        // Create an index leaf cell
        let (cell, overflow_data) = BTreeCellFactory::create_index_leaf_cell(
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;

        // Handle overflow if needed
        let cell = if let Some(overflow_data) = overflow_data {
            match cell {
                BTreeCell::IndexLeaf(mut leaf_cell) => {
                    // Create overflow pages for the overflow data
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    leaf_cell.overflow_page = Some(overflow_page);
                    BTreeCell::IndexLeaf(leaf_cell)
                }
                _ => unreachable!("Expected an index leaf cell"),
            }
        } else {
            cell
        };

        // Check if the root page is empty
        let root_type = self.get_page_type(self.root_page)?;
        match root_type {
            PageType::TableLeaf | PageType::IndexLeaf => {
                // If the root is a leaf, we can insert directly
                let root_node = BTreeNode::new(self.root_page, root_type);
                let root_cell_count = root_node.cell_count(&self.pager)?;
                if root_cell_count == 0 {
                    // If the root is empty, we can directly insert the cell
                    let _ = root_node.insert_cell(cell, &self.pager)?;
                    return Ok(());
                }
            }
            _ => {
                // If the root is not a leaf, we need to handle it differently
            }
        }
        // Find the leaf node where the key should be inserted
        let (_found, leaf_page, _index) = self.find_index_key(&key_value)?;
        let leaf_node = BTreeNode::new(leaf_page, PageType::IndexLeaf);

        // Try to insert the cell
        let (split, median_key, new_node) = leaf_node.insert_cell_ordered(cell, &self.pager)?;

        if split {
            // Propagate the split up the tree
            let path = self.get_path_to_leaf_index(leaf_page, &key_value)?;
            self.propagate_split_index(leaf_node, new_node.unwrap(), median_key.unwrap(), path)?;
        }

        Ok(())
    }

    /// Deletes a record from a table B-Tree.
    ///
    /// # Parameters
    /// * `rowid` - Row ID of the record to delete.
    ///
    /// # Errors
    /// Returns an error if the tree is not a table tree or if there are I/O issues.
    ///
    /// # Returns
    /// `true` if a record was deleted, `false` if the record was not found.
    pub fn delete(&mut self, rowid: i64) -> io::Result<bool> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete record from an index tree",
            ));
        }

        // Find the leaf node containing the record
        let (leaf_page, path) = self.find_leaf_for_insert_table(rowid)?;
        let leaf_node = BTreeNode::new(leaf_page, PageType::TableLeaf);

        // Find the record in the leaf
        let (found, idx) = leaf_node.find_table_rowid(rowid, &self.pager)?;

        if !found {
            // Record not found
            return Ok(false);
        }

        // Get the cell to handle overflow pages
        let cell = leaf_node.get_cell_owned(idx, &self.pager)?;
        if let BTreeCell::TableLeaf(leaf_cell) = &cell {
            if let Some(overflow_page) = leaf_cell.overflow_page {
                // Free overflow pages
                self.free_overflow_chain(overflow_page)?;
            }
        }

        // Delete the cell from the page
        leaf_node.remove_cell(idx, &self.pager)?;

        // Check if the node is underfilled and needs rebalancing
        self.rebalance_after_delete(leaf_page, path)?;

        Ok(true)
    }

    /// Deletes a key from an index B-Tree.
    ///
    /// # Parameters
    /// * `key` - Key to delete.
    ///
    /// # Errors
    /// Returns an error if the tree is not an index tree or if there are I/O issues.
    ///
    /// # Returns
    /// `true` if a key was deleted, `false` if the key was not found.
    pub fn delete_index(&mut self, key: &KeyValue) -> io::Result<bool> {
        if self.tree_type != TreeType::Index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete index entry from a table tree",
            ));
        }

        // Find the leaf node containing the key
        let (found, leaf_page, idx) = self.find_index_key(key)?;

        if !found {
            // Key not found
            return Ok(false);
        }

        // Get the cell to handle overflow pages
        let leaf_node = BTreeNode::new(leaf_page, PageType::IndexLeaf);
        let cell = leaf_node.get_cell_owned(idx, &self.pager)?;

        if let BTreeCell::IndexLeaf(leaf_cell) = &cell {
            if let Some(overflow_page) = leaf_cell.overflow_page {
                // Free overflow pages
                self.free_overflow_chain(overflow_page)?;
            }
        }

        // Delete the cell from the page
        leaf_node.remove_cell(idx, &self.pager)?;

        // Get the path to the leaf for rebalancing
        let path = self.get_path_to_leaf_index(leaf_page, key)?;

        // Check if the node is underfilled and needs rebalancing
        self.rebalance_after_delete(leaf_page, path)?;

        Ok(true)
    }

    /// Creates a chain of overflow pages to store additional data.
    ///
    /// # Parameters
    /// * `data` - Data to store in overflow pages.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Page number of the first overflow page.
    fn create_overflow_chain(&self, data: Vec<u8>) -> io::Result<u32> {
        // Calculate how much data can fit in each overflow page
        let data_per_page = self.page_size as usize - 13; // 4 bytes for next_page pointer, 4 bytes for page number, 4 bytes for reserved space

        // Split the data into chunks
        let chunks: Vec<_> = data.chunks(data_per_page).collect();
        let chunk_count = chunks.len();

        if chunk_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No data to store in overflow chain",
            ));
        }

        // Create the last page first (with next_page = 0)
        let last_chunk = chunks[chunk_count - 1];
        let last_page = self.pager.create_overflow_page(0, last_chunk.to_vec())?;

        if chunk_count == 1 {
            return Ok(last_page);
        }

        // Create the remaining pages in reverse order
        let mut next_page = last_page;

        for i in (0..chunk_count - 1).rev() {
            let page = self
                .pager
                .create_overflow_page(next_page, chunks[i].to_vec())?;
            next_page = page;
        }

        Ok(next_page)
    }

    /// Reads the data from a chain of overflow pages.
    ///
    /// # Parameters
    /// * `first_page` - Page number of the first overflow page.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Data stored in the overflow chain.
    fn read_overflow_chain(&self, first_page: u32) -> io::Result<Vec<u8>> {
        let mut result = Vec::new();
        let mut current_page = first_page;

        while current_page != 0 {
            let guard = self
                .pager
                .get_page(current_page, Some(PageType::Overflow))?;

            match guard.page() {
                crate::page::Page::Overflow(overflow) => {
                    result.extend_from_slice(&overflow.data);
                    current_page = overflow.next_page;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Expected overflow page, got something else: {}",
                            current_page
                        ),
                    ));
                }
            }
        }

        Ok(result)
    }

    /// Frees a chain of overflow pages.
    ///
    /// # Parameters
    /// * `first_page` - Page number of the first overflow page.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn free_overflow_chain(&self, first_page: u32) -> io::Result<()> {
        // Currently, we just mark the pages as free
        // In a complete implementation, you would add them to the freelist
        let mut current_page = first_page;

        while current_page != 0 {
            let guard = self
                .pager
                .get_page(current_page, Some(PageType::Overflow))?;

            let next_page = match guard.page() {
                crate::page::Page::Overflow(overflow) => overflow.next_page,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Expected overflow page, got something else: {}",
                            current_page
                        ),
                    ));
                }
            };

            // Mark the page as free (add to freelist)
            // This would be implemented differently in a full SQLite implementation
            // For now, we just create a free page
            self.pager.create_free_page(0)?;

            current_page = next_page;
        }

        Ok(())
    }

    /// Finds the leaf node where a key should be inserted for table trees.
    ///
    /// # Parameters
    /// * `key` - Key to insert (rowid for table trees).
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - Page number of the leaf node
    /// - Path from root to the leaf (excluding the leaf itself)
    fn find_leaf_for_insert_table(&self, key: i64) -> io::Result<(u32, Vec<u32>)> {
        let mut current_page = self.root_page;
        let mut path = Vec::new();

        // Traverse the tree until we reach a leaf
        loop {
            let current_type = self.get_page_type(current_page)?;

            if current_type.is_leaf() {
                return Ok((current_page, path));
            }

            path.push(current_page);
            let node = BTreeNode::new(current_page, current_type);
            let (_, child_page, _) = node.find_table_key(key, &self.pager)?;
            current_page = child_page;
        }
    }

    /// Gets the path from the root to a leaf node for a specific key in an index tree.
    ///
    /// # Parameters
    /// * `leaf_page` - Page number of the leaf node.
    /// * `key` - Key to find the path for.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Path from root to the leaf (excluding the leaf itself).
    fn get_path_to_leaf_index(&self, leaf_page: u32, key: &KeyValue) -> io::Result<Vec<u32>> {
        if leaf_page == self.root_page {
            return Ok(Vec::new());
        }

        let mut current_page = self.root_page;
        let mut path = Vec::new();

        // Traverse the tree until we reach the target leaf
        loop {
            let current_type = self.get_page_type(current_page)?;

            if current_type.is_leaf() {
                if current_page == leaf_page {
                    return Ok(path);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Could not find path to leaf",
                    ));
                }
            }

            path.push(current_page);
            let node = BTreeNode::new(current_page, current_type);
            let (found, idx) = node.find_index_key(key, &self.pager)?;

            let child_page = if found {
                let cell = node.get_cell_owned(idx, &self.pager)?;
                match cell {
                    BTreeCell::IndexInterior(cell) => cell.left_child_page,
                    _ => unreachable!("Expected an index interior cell"),
                }
            } else {
                let cell_count = node.cell_count(&self.pager)?;

                if idx == 0 {
                    let cell = node.get_cell_owned(0, &self.pager)?;
                    match cell {
                        BTreeCell::IndexInterior(cell) => cell.left_child_page,
                        _ => unreachable!("Expected an index interior cell"),
                    }
                } else if idx >= cell_count {
                    node.get_right_most_child(&self.pager)?
                } else {
                    let cell = node.get_cell_owned(idx, &self.pager)?;
                    match cell {
                        BTreeCell::IndexInterior(cell) => cell.left_child_page,
                        _ => unreachable!("Expected an index interior cell"),
                    }
                }
            };

            if child_page == leaf_page {
                return Ok(path);
            }

            current_page = child_page;
        }
    }

    /// Propagates a node split up the tree for table trees.
    ///
    /// # Parameters
    /// * `left_node` - Left node after the split (original node).
    /// * `right_node` - Right node after the split (new node).
    /// * `median_key` - Key that separates the two nodes.
    /// * `path` - Path from root to the split node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn propagate_split_table(
        &mut self,
        left_node: BTreeNode,
        right_node: BTreeNode,
        median_key: i64,
        mut path: Vec<u32>,
    ) -> io::Result<()> {
        // If path is empty, we're splitting the root
        if path.is_empty() {
            // println!("Creating new root for table tree");
            self.create_new_root_table(left_node, right_node, median_key)?;
            return Ok(());
        }

        // Get the parent node
        let parent_page = path.pop().unwrap();
        let parent_node = BTreeNode::new(parent_page, PageType::TableInterior);

        // Create a new cell for the parent
        let cell = BTreeCellFactory::create_table_interior_cell(left_node.page_number, median_key);

        // Update the parent's right-most child to point to the right node
        parent_node.set_right_most_child(right_node.page_number, &self.pager)?;

        // Insert the cell into the parent
        let (split, new_median, new_parent) = parent_node.insert_cell_ordered(cell, &self.pager)?;

        if split {
            // Recursively propagate the split up the tree
            self.propagate_split_table(
                parent_node,
                new_parent.unwrap(),
                new_median.unwrap(),
                path,
            )?;
        }

        Ok(())
    }

    /// Propagates a node split up the tree for index trees.
    ///
    /// # Parameters
    /// * `left_node` - Left node after the split (original node).
    /// * `right_node` - Right node after the split (new node).
    /// * `median_key` - Key that separates the two nodes.
    /// * `path` - Path from root to the split node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn propagate_split_index(
        &mut self,
        left_node: BTreeNode,
        right_node: BTreeNode,
        median_key: i64,
        mut path: Vec<u32>,
    ) -> io::Result<()> {
        // If path is empty, we're splitting the root
        if path.is_empty() {
            self.create_new_root_index(left_node, right_node, median_key)?;
            return Ok(());
        }

        // Get the parent node
        let parent_page = path.pop().unwrap();
        let parent_node = BTreeNode::new(parent_page, PageType::IndexInterior);

        // For index trees, we need to create an interior cell with payload
        // Extract the key from the median and create proper payload
        let payload = self.create_index_payload_from_median_key(median_key)?;

        let (cell, overflow) = BTreeCellFactory::create_index_interior_cell(
            left_node.page_number,
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;

        // Handle overflow if needed
        let cell = if let Some(overflow_data) = overflow {
            match cell {
                BTreeCell::IndexInterior(mut interior_cell) => {
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    interior_cell.overflow_page = Some(overflow_page);
                    BTreeCell::IndexInterior(interior_cell)
                }
                _ => unreachable!("Expected an index interior cell"),
            }
        } else {
            cell
        };

        // Update the parent's right-most child to point to the right node
        parent_node.set_right_most_child(right_node.page_number, &self.pager)?;

        // Insert the cell into the parent
        let (split, new_median, new_parent) = parent_node.insert_cell_ordered(cell, &self.pager)?;

        if split {
            // Recursively propagate the split up the tree
            self.propagate_split_index(
                parent_node,
                new_parent.unwrap(),
                new_median.unwrap(),
                path,
            )?;
        }

        Ok(())
    }

    /// Creates a new root node after the root splits for table trees.
    ///
    /// # Parameters
    /// * `left_node` - Left node after the split (original root).
    /// * `right_node` - Right node after the split (new node).
    /// * `median_key` - Key that separates the two nodes.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn create_new_root_table(
        &mut self,
        left_node: BTreeNode,
        right_node: BTreeNode,
        median_key: i64,
    ) -> io::Result<()> {
        // Create a new interior node to be the new root
        let new_root = BTreeNode::create_interior(
            PageType::TableInterior,
            Some(right_node.page_number),
            &self.pager,
        )?;
        // println!("Creating new root for table tree: {:?}", new_root);

        // Create a cell pointing to the left node
        let cell = BTreeCellFactory::create_table_interior_cell(left_node.page_number, median_key);
        // println!("Creating interior cell for new root: {:?}", cell);
        // Insert the cell into the new root
        new_root.insert_cell(cell, &self.pager)?;

        // Update the tree's root page
        self.root_page = new_root.page_number;

        Ok(())
    }

    /// Creates a new root node after the root splits for index trees.
    ///
    /// # Parameters
    /// * `left_node` - Left node after the split (original root).
    /// * `right_node` - Right node after the split (new node).
    /// * `median_key` - Key that separates the two nodes.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn create_new_root_index(
        &mut self,
        left_node: BTreeNode,
        right_node: BTreeNode,
        median_key: i64,
    ) -> io::Result<()> {
        // Create a new interior node to be the new root
        let new_root = BTreeNode::create_interior(
            PageType::IndexInterior,
            Some(right_node.page_number),
            &self.pager,
        )?;

        // For index trees, we need to create an interior cell with payload
        // Extract the key from the median and create proper payload
        let payload = self.create_index_payload_from_median_key(median_key)?;

        let (cell, overflow) = BTreeCellFactory::create_index_interior_cell(
            left_node.page_number,
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;

        // Handle overflow if needed
        let cell = if let Some(overflow_data) = overflow {
            match cell {
                BTreeCell::IndexInterior(mut interior_cell) => {
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    interior_cell.overflow_page = Some(overflow_page);
                    BTreeCell::IndexInterior(interior_cell)
                }
                _ => unreachable!("Expected an index interior cell"),
            }
        } else {
            cell
        };

        // Insert the cell into the new root
        new_root.insert_cell(cell, &self.pager)?;

        // Update the tree's root page
        self.root_page = new_root.page_number;

        Ok(())
    }

    /// Creates an index payload from a median key.
    ///
    /// In SQLite index trees, interior nodes store the key that separates
    /// the child subtrees. This key is extracted from the original payload
    /// and serialized appropriately.
    ///
    /// # Parameters
    /// * `median_key` - The median key value (typically an i64 hash or extracted key).
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    ///
    /// # Returns
    /// Serialized payload containing the key.
    fn create_index_payload_from_median_key(&self, median_key: i64) -> io::Result<Vec<u8>> {
        // For simplicity, we'll treat the median_key as an integer key
        // In a real implementation, you might need to handle different data types
        // based on the index definition

        let key_value = crate::utils::serialization::SqliteValue::Integer(median_key);
        let mut payload = Vec::new();

        // Serialize just the key (no rowid in interior nodes)
        crate::utils::serialization::serialize_values(&[key_value], &mut payload)?;

        Ok(payload)
    }

    /// Creates a payload from a KeyValue.
    ///
    /// This function does exactly the opposite of `extract_key_from_payload`.
    /// It converts a KeyValue back into a serialized payload that can be stored
    /// in index nodes.
    ///
    /// # Parameters
    /// * `key_value` - The key value to convert to payload
    ///
    /// # Returns
    /// A serialized payload containing the key
    ///
    /// # Errors
    /// Returns an error if the payload cannot be serialized
    #[allow(dead_code)]
    fn create_payload_from_key_value(&self, key_value: &KeyValue) -> io::Result<Vec<u8>> {
        use crate::utils::serialization::{serialize_values, SqliteValue};

        // Convert the KeyValue to a SqliteValue
        let sqlite_value = match key_value {
            KeyValue::Integer(i) => SqliteValue::Integer(*i),
            KeyValue::Float(f) => SqliteValue::Float(*f),
            KeyValue::String(s) => SqliteValue::String(s.clone()),
            KeyValue::Blob(b) => SqliteValue::Blob(b.clone()),
            KeyValue::Null => SqliteValue::Null,
        };

        // Serialize the value as the first (and only) value in the payload
        let mut payload = Vec::new();
        serialize_values(&[sqlite_value], &mut payload)?;

        Ok(payload)
    }

    /// Extracts the actual key value from a median key for comparison purposes.
    ///
    /// This is used when we need to convert a median key back to a KeyValue
    /// for comparison operations in index trees.
    ///
    /// # Parameters
    /// * `median_key` - The median key value.
    ///
    /// # Returns
    /// A KeyValue that can be used for comparisons.
    #[allow(dead_code)]
    fn median_key_to_key_value(&self, median_key: i64) -> KeyValue {
        // For simplicity, we assume the median key is always an integer
        // In a real implementation, you might need type information to
        // properly reconstruct the original key type
        KeyValue::Integer(median_key)
    }

    /// Rebalances the tree after a deletion.
    ///
    /// # Parameters
    /// * `leaf_page` - Page number of the leaf node where deletion occurred.
    /// * `path` - Path from root to the leaf node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn rebalance_after_delete(&mut self, leaf_page: u32, path: Vec<u32>) -> io::Result<()> {
        // If this is the root or if the path is empty, no rebalancing needed
        if path.is_empty() || leaf_page == self.root_page {
            return Ok(());
        }

        // Define minimum cell count for a node (50% full)
        // In SQLite, this is typically fill_factor / 2
        let min_cell_count = 1; // Simplified - should be based on page size

        // Check if the node needs rebalancing
        let leaf_type = match self.tree_type {
            TreeType::Table => PageType::TableLeaf,
            TreeType::Index => PageType::IndexLeaf,
        };

        let leaf_node = BTreeNode::new(leaf_page, leaf_type);
        let cell_count = leaf_node.cell_count(&self.pager)?;

        if cell_count >= min_cell_count {
            // Node has enough cells, no rebalancing needed
            return Ok(());
        }

        // Node is underfilled, try to borrow or merge
        let parent_page = *path.last().unwrap();
        let parent_type = match self.tree_type {
            TreeType::Table => PageType::TableInterior,
            TreeType::Index => PageType::IndexInterior,
        };

        let parent_node = BTreeNode::new(parent_page, parent_type);

        // Find the index of the current node in the parent
        let (left_sibling, right_sibling) = self.find_siblings(&parent_node, leaf_page)?;

        // Try to borrow from the right sibling first
        if let Some(right_page) = right_sibling {
            if self.borrow_from_sibling(&leaf_node, right_page, parent_page, false)? {
                return Ok(());
            }
        }

        // Try to borrow from the left sibling
        if let Some(left_page) = left_sibling {
            if self.borrow_from_sibling(&leaf_node, left_page, parent_page, true)? {
                return Ok(());
            }
        }

        // Borrowing failed, need to merge
        // Prefer merging with left sibling if available
        if let Some(left_page) = left_sibling {
            self.merge_nodes(left_page, leaf_page, parent_page)?;
        } else if let Some(right_page) = right_sibling {
            self.merge_nodes(leaf_page, right_page, parent_page)?;
        } else {
            // No siblings available, this should never happen in a valid B-tree
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Node has no siblings for merging",
            ));
        }

        // Check if parent needs rebalancing (recursive)
        let new_path = if path.len() > 1 {
            path[0..path.len() - 1].to_vec()
        } else {
            Vec::new()
        };

        self.rebalance_after_delete(parent_page, new_path)?;

        Ok(())
    }

    /// Finds the sibling nodes of a given node.
    ///
    /// # Parameters
    /// * `parent_node` - Parent node.
    /// * `node_page` - Page number of the node to find siblings for.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - Option with page number of left sibling (None if no left sibling)
    /// - Option with page number of right sibling (None if no right sibling)
    fn find_siblings(
        &self,
        parent_node: &BTreeNode,
        node_page: u32,
    ) -> io::Result<(Option<u32>, Option<u32>)> {
        let cell_count = parent_node.cell_count(&self.pager)?;

        if cell_count == 0 {
            // Parent has no cells, node must be the only child
            return Ok((None, None));
        }

        // Find the position of node_page in the parent
        let mut pos = None;

        // Check if node is the rightmost child
        let rightmost = parent_node.get_right_most_child(&self.pager)?;
        if rightmost == node_page {
            pos = Some(cell_count);
        }

        // If not rightmost, find the position by scanning cells
        if pos.is_none() {
            for i in 0..cell_count {
                let cell = parent_node.get_cell_owned(i, &self.pager)?;

                let child_page = match cell {
                    BTreeCell::TableInterior(ref interior) => interior.left_child_page,
                    BTreeCell::IndexInterior(ref interior) => interior.left_child_page,
                    _ => unreachable!("Expected an interior cell"),
                };

                if child_page == node_page {
                    pos = Some(i);
                    break;
                }
            }
        }

        let pos = match pos {
            Some(p) => p,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Node not found in parent",
                ));
            }
        };

        // Determine siblings
        let left_sibling = if pos > 0 {
            // Get the left sibling
            if pos == cell_count {
                // Node is rightmost child, its left sibling is the left child of the last cell
                let last_cell = parent_node.get_cell_owned(cell_count - 1, &self.pager)?;

                match self.tree_type {
                    TreeType::Table => match last_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    },
                    TreeType::Index => match last_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    },
                }
            } else {
                // Get the left child of the previous cell
                let prev_cell = parent_node.get_cell_owned(pos - 1, &self.pager)?;

                match self.tree_type {
                    TreeType::Table => match prev_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    },
                    TreeType::Index => match prev_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    },
                }
            }
        } else {
            None
        };

        let right_sibling = if pos < cell_count {
            // Get the right sibling
            if pos == 0 {
                // Node is leftmost child, its right sibling is the left child of the first cell
                let first_cell = parent_node.get_cell_owned(0, &self.pager)?;

                match self.tree_type {
                    TreeType::Table => match first_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    },
                    TreeType::Index => match first_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    },
                }
            } else if pos == cell_count {
                // Node is rightmost child, it has no right sibling
                None
            } else {
                // Get the left child of the next cell
                let next_cell = parent_node.get_cell_owned(pos, &self.pager)?;

                match self.tree_type {
                    TreeType::Table => match next_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    },
                    TreeType::Index => match next_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    },
                }
            }
        } else {
            None
        };

        Ok((left_sibling, right_sibling))
    }

    /// Tries to borrow cells from a sibling node.
    ///
    /// # Parameters
    /// * `target_node` - Node that needs more cells.
    /// * `sibling_page` - Page number of the sibling node.
    /// * `parent_page` - Page number of the parent node.
    /// * `from_left` - `true` if borrowing from left sibling, `false` if from right.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// `true` if borrowing succeeded, `false` otherwise.
    fn borrow_from_sibling(
        &self,
        target_node: &BTreeNode,
        sibling_page: u32,
        parent_page: u32,
        from_left: bool,
    ) -> io::Result<bool> {
        // Define minimum cell count for a node after borrowing
        let min_cell_count = 1; // Simplified - should be based on page size

        // Open sibling node
        let sibling_type = target_node.node_type;
        let sibling_node = BTreeNode::new(sibling_page, sibling_type);

        // Check if sibling has enough cells to spare
        let sibling_cell_count = sibling_node.cell_count(&self.pager)?;

        if sibling_cell_count <= min_cell_count {
            // Sibling doesn't have enough cells to spare
            return Ok(false);
        }

        // Get the separator key from the parent
        let parent_type = match self.tree_type {
            TreeType::Table => PageType::TableInterior,
            TreeType::Index => PageType::IndexInterior,
        };

        let parent_node = BTreeNode::new(parent_page, parent_type);

        if from_left {
            // Borrow from the left sibling
            // Get the rightmost cell from the left sibling
            let sibling_cell = sibling_node.get_cell_owned(sibling_cell_count - 1, &self.pager)?;

            // Remove the cell from the sibling
            sibling_node.remove_cell(sibling_cell_count - 1, &self.pager)?;

            // Update the separator key in the parent
            let separator_idx =
                self.find_separator_index(&parent_node, target_node.page_number, true)?;

            // Update the separator key
            let new_separator = match (self.tree_type, &sibling_cell) {
                (TreeType::Table, BTreeCell::TableLeaf(leaf)) => leaf.row_id,
                (TreeType::Index, BTreeCell::IndexLeaf(leaf)) => {
                    let key_value = extract_key_from_payload(&leaf.payload)?;
                    match key_value {
                        KeyValue::Integer(i) => i,
                        KeyValue::Float(f) => f as i64,
                        _ => {
                            // For string and blob, hash the key
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            std::hash::Hash::hash(&key_value, &mut hasher);
                            hasher.finish() as i64
                        }
                    }
                }
                _ => unreachable!("Unexpected cell type"),
            };

            // Update the separator key in the parent using callback
            parent_node.with_cell_mut(separator_idx, &self.pager, |cell| {
                match self.tree_type {
                    TreeType::Table => match cell {
                        BTreeCell::TableInterior(ref mut interior) => {
                            interior.key = new_separator;
                        }
                        _ => unreachable!("Expected a table interior cell"),
                    },
                    TreeType::Index => {
                        // For index trees, we need to update the payload
                        // This is simplified - in a real implementation, you'd handle this properly
                    }
                }
                Ok(())
            })?;

            // Insert the cell into the target node
            target_node.insert_cell_ordered(sibling_cell, &self.pager)?;

            Ok(true)
        } else {
            // Borrow from the right sibling
            // Get the leftmost cell from the right sibling
            let sibling_cell = sibling_node.get_cell_owned(0, &self.pager)?;

            // Remove the cell from the sibling
            sibling_node.remove_cell(0, &self.pager)?;

            // Update the separator key in the parent
            let separator_idx =
                self.find_separator_index(&parent_node, target_node.page_number, false)?;

            // Update the separator key
            let new_separator = match (self.tree_type, &sibling_cell) {
                (TreeType::Table, BTreeCell::TableLeaf(leaf)) => leaf.row_id,
                (TreeType::Index, BTreeCell::IndexLeaf(leaf)) => {
                    let key_value = extract_key_from_payload(&leaf.payload)?;
                    match key_value {
                        KeyValue::Integer(i) => i,
                        KeyValue::Float(f) => f as i64,
                        _ => {
                            // For string and blob, hash the key
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            std::hash::Hash::hash(&key_value, &mut hasher);
                            hasher.finish() as i64
                        }
                    }
                }
                _ => unreachable!("Unexpected cell type"),
            };

            // Update the separator key in the parent using callback
            parent_node.with_cell_mut(separator_idx, &self.pager, |cell| {
                match self.tree_type {
                    TreeType::Table => match cell {
                        BTreeCell::TableInterior(ref mut interior) => {
                            interior.key = new_separator;
                        }
                        _ => unreachable!("Expected a table interior cell"),
                    },
                    TreeType::Index => {
                        // For index trees, we need to update the payload
                        // This is simplified - in a real implementation, you'd handle this properly
                    }
                }
                Ok(())
            })?;

            // Insert the cell into the target node
            target_node.insert_cell_ordered(sibling_cell, &self.pager)?;

            Ok(true)
        }
    }

    /// Finds the index of the separator between two nodes in the parent node.
    ///
    /// # Parameters
    /// * `parent_node` - Parent node.
    /// * `child_page` - Page number of one of the child nodes.
    /// * `is_left` - `true` if child_page is the left node, `false` if it's the right node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Index of the separator cell in the parent.
    fn find_separator_index(
        &self,
        parent_node: &BTreeNode,
        child_page: u32,
        is_left: bool,
    ) -> io::Result<u16> {
        let cell_count = parent_node.cell_count(&self.pager)?;

        // If child_page is the rightmost child, there's no separator (it's after the last key)
        let rightmost = parent_node.get_right_most_child(&self.pager)?;
        if child_page == rightmost {
            if is_left {
                // The separator is the last key in the parent
                return Ok(cell_count - 1);
            } else {
                // There's no separator if child_page is rightmost and is_left is false
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "No separator for rightmost child",
                ));
            }
        }

        // Find the cell that has child_page as its left child
        for i in 0..cell_count {
            let cell = parent_node.get_cell_owned(i, &self.pager)?;

            let cell_child = match cell {
                BTreeCell::TableInterior(ref interior) => interior.left_child_page,
                BTreeCell::IndexInterior(ref interior) => interior.left_child_page,
                _ => unreachable!("Expected an interior cell"),
            };

            if cell_child == child_page {
                if is_left {
                    // The separator is this cell (since child_page is the left child)
                    return Ok(i);
                } else {
                    // The separator is the previous cell (since child_page is the right child)
                    if i == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "No separator for leftmost child",
                        ));
                    }
                    return Ok(i - 1);
                }
            }
        }

        // Child page not found in parent
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Child page not found in parent",
        ))
    }

    /// Merges two adjacent nodes.
    ///
    /// # Parameters
    /// * `left_page` - Page number of the left node.
    /// * `right_page` - Page number of the right node.
    /// * `parent_page` - Page number of the parent node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn merge_nodes(&self, left_page: u32, right_page: u32, parent_page: u32) -> io::Result<()> {
        // Open the nodes
        let leaf_type = match self.tree_type {
            TreeType::Table => PageType::TableLeaf,
            TreeType::Index => PageType::IndexLeaf,
        };

        let left_node = BTreeNode::new(left_page, leaf_type);
        let right_node = BTreeNode::new(right_page, leaf_type);

        // Get the separator key from the parent
        let parent_type = match self.tree_type {
            TreeType::Table => PageType::TableInterior,
            TreeType::Index => PageType::IndexInterior,
        };

        let parent_node = BTreeNode::new(parent_page, parent_type);

        // Find the separator key in the parent
        let separator_idx = self.find_separator_index(&parent_node, left_page, true)?;

        // Move all cells from right to left
        let right_cell_count = right_node.cell_count(&self.pager)?;

        for i in 0..right_cell_count {
            let cell = right_node.get_cell_owned(i, &self.pager)?;
            left_node.insert_cell_ordered(cell, &self.pager)?;
        }

        // Remove the separator from the parent
        parent_node.remove_cell(separator_idx, &self.pager)?;

        // Update the rightmost child if necessary
        parent_node.with_page_mut(&self.pager, |page| {
            match page {
                crate::page::Page::BTree(btree_page) => {
                    // If the right node was the right-most child, update to point to the left node
                    if let Some(right_most) = btree_page.header.right_most_page {
                        if right_most == right_page {
                            btree_page.header.right_most_page = Some(left_page);
                        }
                    }
                    Ok(())
                }
                _ => unreachable!("Expected a B-Tree page"),
            }
        })?;

        // Mark the right node as free
        // This would normally add it to the free list
        // For simplicity, we're just creating a free page (the page itself isn't actually freed)
        self.pager.create_free_page(0)?;

        Ok(())
    }

    /// Gets the page type of a specific page.
    ///
    /// # Parameters
    /// * `page_number` - Page number to check.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// The page type.
    fn get_page_type(&self, page_number: u32) -> io::Result<PageType> {
        self.pager
            .get_page_callback(page_number, None, |page| page.page_type())
    }

    /// Gets the root page number of the B-Tree.
    ///
    /// # Returns
    /// The root page number.
    pub fn root_page(&self) -> u32 {
        self.root_page
    }

    /// Gets the tree type.
    ///
    /// # Returns
    /// The tree type (Table or Index).
    pub fn tree_type(&self) -> TreeType {
        self.tree_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pager::Pager;
    use crate::tree::record::Record;
    use crate::utils::serialization::SqliteValue;
    use tempfile::tempdir;

    // Helper function to create a test pager
    fn create_test_pager() -> Pager {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        Pager::create(db_path, 4096, None, 0).unwrap()
    }

    // Helper function to create a test record
    fn create_test_record(values: Vec<SqliteValue>) -> Record {
        Record::with_values(values)
    }

    #[test]
    fn test_create_table_btree() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let result = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        );

        assert!(result.is_ok());

        let btree = result.unwrap();
        assert_eq!(btree.tree_type, TreeType::Table);

        // Root page should exist and be a table leaf
        let root_type = btree.get_page_type(btree.root_page).unwrap();
        assert_eq!(root_type, PageType::TableLeaf);
    }

    #[test]
    fn test_create_index_btree() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let result = BTree::create(
            TreeType::Index,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        );

        assert!(result.is_ok());

        let btree = result.unwrap();
        assert_eq!(btree.tree_type, TreeType::Index);

        // Root page should exist and be an index leaf
        let root_type = btree.get_page_type(btree.root_page).unwrap();
        assert_eq!(root_type, PageType::IndexLeaf);
    }

    #[test]
    fn test_open_btree() {
        let pager = create_test_pager();

        // Create a B-Tree
        let btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        let root_page = btree.root_page;
        let pager = btree.pager; // Move pager out

        // Open the B-Tree
        let opened_btree =
            BTree::open(root_page, TreeType::Table, pager, 4096, 0, 255, 32).unwrap();

        assert_eq!(opened_btree.root_page, root_page);
        assert_eq!(opened_btree.tree_type, TreeType::Table);
    }

    #[test]
    fn test_open_btree_wrong_type() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let btree = BTree::create(TreeType::Table, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        let root_page = btree.root_page;
        let pager = btree.pager; // Move pager out

        // Try to open as index tree - should fail
        let result = BTree::open(root_page, TreeType::Index, pager, 4096, 0, 255, 32);

        assert!(result.is_err());
    }

    #[test]
    fn test_insert_and_find_record() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let mut btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        // Create some records
        let records = [
            (
                1,
                create_test_record(vec![
                    SqliteValue::Integer(42),
                    SqliteValue::String("Record 1".to_string()),
                ]),
            ),
            (
                2,
                create_test_record(vec![
                    SqliteValue::Integer(43),
                    SqliteValue::String("Record 2".to_string()),
                ]),
            ),
            (
                3,
                create_test_record(vec![
                    SqliteValue::Integer(44),
                    SqliteValue::String("Record 3".to_string()),
                ]),
            ),
        ];

        // Insert the records
        for (rowid, record) in &records {
            btree.insert(*rowid, record).unwrap();
        }

        // Find the records
        for (rowid, record) in &records {
            let found = btree.find(*rowid).unwrap();

            assert!(found.is_some());
            let found_record = found.unwrap();

            assert_eq!(found_record.len(), record.len());

            // Compare record values
            for i in 0..record.len() {
                match (&record.values[i], &found_record.values[i]) {
                    (SqliteValue::Integer(a), SqliteValue::Integer(b)) => assert_eq!(a, b),
                    (SqliteValue::String(a), SqliteValue::String(b)) => assert_eq!(a, b),
                    _ => panic!("Records don't match"),
                }
            }
        }

        // Try to find a non-existent record
        let not_found = btree.find(999).unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_insert_many_records() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let mut btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        // Insert enough records to cause splits
        let record_count = 50;

        for i in 1..=record_count {
            let record = create_test_record(vec![
                SqliteValue::Integer(i),
                SqliteValue::String(format!("Record {}", i)),
                // Add more data to make the record larger and cause splits faster
                SqliteValue::Blob(vec![i as u8; 100]),
            ]);

            btree.insert(i, &record).unwrap();
        }
        //  panic!("Test completed successfully, but this line should not be reached");
        // Verify all records can be found
        for i in 1..=record_count {
            let found = btree.find(i).unwrap();
            assert!(found.is_some());

            let record = found.unwrap();

            // Verify the record content
            match &record.values[0] {
                SqliteValue::Integer(value) => assert_eq!(*value, i),
                _ => panic!("Expected Integer"),
            }

            match &record.values[1] {
                SqliteValue::String(value) => assert_eq!(value, &format!("Record {}", i)),
                _ => panic!("Expected String"),
            }

            match &record.values[2] {
                SqliteValue::Blob(value) => assert_eq!(value, &vec![i as u8; 100]),
                _ => panic!("Expected Blob"),
            }
        }
    }

    #[test]
    fn test_delete_record() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let mut btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        // Insert some records
        for i in 1..=10 {
            let record = create_test_record(vec![
                SqliteValue::Integer(i),
                SqliteValue::String(format!("Record {}", i)),
            ]);

            btree.insert(i, &record).unwrap();
        }

        // Delete some records
        assert!(btree.delete(3).unwrap()); // Existing record
        assert!(btree.delete(7).unwrap()); // Existing record
        assert!(!btree.delete(999).unwrap()); // Non-existent record

        // Verify that deleted records are gone
        assert!(btree.find(3).unwrap().is_none());
        assert!(btree.find(7).unwrap().is_none());

        // Verify that other records still exist
        for i in [1, 2, 4, 5, 6, 8, 9, 10] {
            assert!(btree.find(i).unwrap().is_some());
        }
    }

    #[test]
    fn test_insert_and_find_index() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let mut btree = BTree::create(
            TreeType::Index,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        // Create some index entries
        // For each key, we'll create payload that consists of the key followed by a rowid
        let entries = [(42, 1), (43, 2), (44, 3)];

        // Insert index entries
        for (key, rowid) in &entries {
            // Create payload for the index: the key in SQLite format
            let mut key_payload = Vec::new();
            crate::utils::serialization::serialize_values(
                &[SqliteValue::Integer(*key)],
                &mut key_payload,
            )
            .unwrap();

            btree.insert_index(&key_payload, *rowid).unwrap();
        }

        // Find the index entries
        for (key, _) in &entries {
            let key_value = KeyValue::Integer(*key);
            let (found, _, _) = btree.find_index_key(&key_value).unwrap();

            assert!(found, "Key {} should be found", key);
        }

        // Try to find a non-existent key
        let non_existent_key = KeyValue::Integer(999);
        let (found, _, _) = btree.find_index_key(&non_existent_key).unwrap();
        assert!(!found, "Key 999 should not be found");
    }

    #[test]
    fn test_delete_index() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let mut btree = BTree::create(
            TreeType::Index,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        // Insert some index entries
        for i in 1..=10 {
            // Create payload for the index: the key in SQLite format
            let mut key_payload = Vec::new();
            crate::utils::serialization::serialize_values(
                &[SqliteValue::Integer(i)],
                &mut key_payload,
            )
            .unwrap();

            btree.insert_index(&key_payload, i).unwrap();
        }

        // Delete some index entries
        assert!(btree.delete_index(&KeyValue::Integer(3)).unwrap()); // Existing key
        assert!(btree.delete_index(&KeyValue::Integer(7)).unwrap()); // Existing key
        assert!(!btree.delete_index(&KeyValue::Integer(999)).unwrap()); // Non-existent key

        // Verify that deleted keys are gone
        let (found3, _, _) = btree.find_index_key(&KeyValue::Integer(3)).unwrap();
        assert!(!found3, "Key 3 should be deleted");

        let (found7, _, _) = btree.find_index_key(&KeyValue::Integer(7)).unwrap();
        assert!(!found7, "Key 7 should be deleted");

        // Verify that other keys still exist
        for i in [1, 2, 4, 5, 6, 8, 9, 10] {
            let (found, _, _) = btree.find_index_key(&KeyValue::Integer(i)).unwrap();
            assert!(found, "Key {} should still exist", i);
        }
    }

    #[test]
    fn test_overflow_chain() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let mut btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            255, // max_payload_fraction (100%)
            32,  // min_payload_fraction (12.5%)
        )
        .unwrap();

        // Create a record with a large payload to force overflow pages
        let large_data = vec![0xAA; 10000]; // 10KB data, should require at least 2 overflow pages
        let large_record = create_test_record(vec![
            SqliteValue::Integer(1),
            SqliteValue::Blob(large_data.clone()),
        ]);

        // Insert the record
        btree.insert(1, &large_record).unwrap();

        // Find the record
        let found = btree.find(1).unwrap();
        assert!(found.is_some());

        let record = found.unwrap();
        assert_eq!(record.len(), 2);

        // Verify the large blob data
        match &record.values[1] {
            SqliteValue::Blob(data) => {
                assert_eq!(data.len(), large_data.len());
                assert_eq!(data, &large_data);
            }
            _ => panic!("Expected Blob"),
        }
    }

    #[test]
    fn test_btree_getters() {
        let pager = create_test_pager();

        let btree = BTree::create(TreeType::Table, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        assert_eq!(btree.tree_type(), TreeType::Table);
        assert!(btree.root_page() > 0);
    }

    #[test]
    fn test_find_on_wrong_tree_type() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Try to find a record (should fail on index tree)
        let result = btree.find(42);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_on_wrong_tree_type() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let mut btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        let record = create_test_record(vec![SqliteValue::Integer(42)]);

        // Try to insert a record (should fail on index tree)
        let result = btree.insert(1, &record);
        assert!(result.is_err());
    }

    #[test]
    fn test_find_index_on_wrong_tree_type() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let btree = BTree::create(TreeType::Table, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Try to find an index key (should fail on table tree)
        let result = btree.find_index_key(&KeyValue::Integer(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_index_on_wrong_tree_type() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let mut btree = BTree::create(TreeType::Table, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        let mut key_payload = Vec::new();
        crate::utils::serialization::serialize_values(
            &[SqliteValue::Integer(42)],
            &mut key_payload,
        )
        .unwrap();

        // Try to insert an index entry (should fail on table tree)
        let result = btree.insert_index(&key_payload, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_on_wrong_tree_type() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let mut btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Try to delete a record (should fail on index tree)
        let result = btree.delete(42);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_index_on_wrong_tree_type() {
        let pager = create_test_pager();

        // Create a table B-Tree
        let mut btree = BTree::create(TreeType::Table, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Try to delete an index key (should fail on table tree)
        let result = btree.delete_index(&KeyValue::Integer(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_payload_fractions() {
        let pager = create_test_pager();

        let btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            0,
            128, // 50% max payload fraction
            64,  // 25% min payload fraction
        )
        .unwrap();

        let max_local = btree.max_local_payload();
        let min_local = btree.min_local_payload();
        let usable_size = btree.usable_page_size();

        assert!(max_local > 0);
        assert!(min_local > 0);
        assert!(min_local <= max_local);
        assert_eq!(usable_size, 4096); // No reserved space
    }

    #[test]
    fn test_reserved_space() {
        let pager = create_test_pager();

        let btree = BTree::create(
            TreeType::Table,
            Arc::new(pager),
            4096,
            100, // 100 bytes reserved space
            255,
            32,
        )
        .unwrap();

        let usable_size = btree.usable_page_size();
        assert_eq!(usable_size, 4096 - 100);
    }

    #[test]
    fn test_create_index_payload_from_median_key() {
        let pager = create_test_pager();

        let btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Test creating payload from median key
        let median_key = 42i64;
        let payload = btree
            .create_index_payload_from_median_key(median_key)
            .unwrap();

        // Verify the payload can be deserialized back to the same key
        let mut cursor = std::io::Cursor::new(&payload);
        let (values, _) = crate::utils::serialization::deserialize_values(&mut cursor).unwrap();

        assert_eq!(values.len(), 1);
        match &values[0] {
            crate::utils::serialization::SqliteValue::Integer(i) => {
                assert_eq!(*i, median_key);
            }
            _ => panic!("Expected integer value"),
        }

        // Test key value conversion
        let key_value = btree.median_key_to_key_value(median_key);
        assert_eq!(key_value, KeyValue::Integer(median_key));
    }

    #[test]
    fn test_key_value_payload_roundtrip() {
        let pager = create_test_pager();

        let btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Test different types of KeyValues
        let test_cases = vec![
            KeyValue::Integer(42),
            KeyValue::Float(3.14159),
            KeyValue::String("Hello, SQLite!".to_string()),
            KeyValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            KeyValue::Null,
        ];

        for original_key in test_cases {
            // Convert KeyValue to payload
            let payload = btree.create_payload_from_key_value(&original_key).unwrap();

            // Convert payload back to KeyValue
            let extracted_key = crate::tree::node::extract_key_from_payload(&payload).unwrap();

            // Verify they match
            assert_eq!(
                original_key, extracted_key,
                "Roundtrip failed for key: {:?}",
                original_key
            );
        }
    }

    #[test]
    fn test_index_split_with_proper_payload() {
        let pager = create_test_pager();

        // Create an index B-Tree
        let mut btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Insert enough entries to force a split
        for i in 1..=20 {
            let mut key_payload = Vec::new();
            crate::utils::serialization::serialize_values(
                &[SqliteValue::Integer(i * 10)], // Use larger keys to ensure ordering
                &mut key_payload,
            )
            .unwrap();

            btree.insert_index(&key_payload, i).unwrap();
        }

        // Verify all entries can still be found after potential splits
        for i in 1..=20 {
            let key_value = KeyValue::Integer(i * 10);
            let (found, _, _) = btree.find_index_key(&key_value).unwrap();
            assert!(found, "Key {} should be found after split", i * 10);
        }

        // The tree should have grown (root might not be a leaf anymore)
        let original_root_type = btree.get_page_type(btree.root_page()).unwrap();

        // Root could be either IndexLeaf (if no split occurred) or IndexInterior (if split occurred)
        assert!(
            original_root_type == PageType::IndexLeaf
                || original_root_type == PageType::IndexInterior,
            "Root should be either IndexLeaf or IndexInterior"
        );
    }

    #[test]
    fn test_complex_index_operations() {
        let pager = create_test_pager();

        let mut btree = BTree::create(TreeType::Index, Arc::new(pager), 4096, 0, 255, 32).unwrap();

        // Insert entries with different key types (but all as integers for simplicity)
        let test_keys = [
            100, 50, 150, 25, 75, 125, 175, 12, 37, 62, 87, 112, 137, 162, 187,
        ];

        for (idx, &key) in test_keys.iter().enumerate() {
            let mut key_payload = Vec::new();
            crate::utils::serialization::serialize_values(
                &[SqliteValue::Integer(key)],
                &mut key_payload,
            )
            .unwrap();

            btree.insert_index(&key_payload, (idx + 1) as i64).unwrap();
        }

        // Verify all keys can be found
        for &key in &test_keys {
            let key_value = KeyValue::Integer(key);
            let (found, _, _) = btree.find_index_key(&key_value).unwrap();
            assert!(found, "Key {} should be found", key);
        }

        // Test deletion of some keys
        let keys_to_delete = [25, 75, 150];
        for &key in &keys_to_delete {
            let key_value = KeyValue::Integer(key);
            let deleted = btree.delete_index(&key_value).unwrap();
            assert!(deleted, "Key {} should be deleted", key);
        }

        // Verify deleted keys are gone
        for &key in &keys_to_delete {
            let key_value = KeyValue::Integer(key);
            let (found, _, _) = btree.find_index_key(&key_value).unwrap();
            assert!(!found, "Key {} should be deleted", key);
        }

        // Verify remaining keys still exist
        for &key in &test_keys {
            if !keys_to_delete.contains(&key) {
                let key_value = KeyValue::Integer(key);
                let (found, _, _) = btree.find_index_key(&key_value).unwrap();
                assert!(found, "Key {} should still exist", key);
            }
        }
    }
}
