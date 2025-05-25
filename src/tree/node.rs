//! # B-Tree Node Module
//!
//! This module defines the `BTreeNode` struct and its associated methods.
//! It provides functionality for creating, opening, and manipulating B-Tree nodes
//! using callback-based access patterns for safe memory management.
//!
//! After implementing the callback methods in the Pager, this module will be able to access pages safely, levaraging the Pager's built in PageGuard system.
//!
//! The BtreeNode API provides also some callback functions to access pages and cells in a safe way, ensuring that the page type is correct before performing operations.
//!
//! The most important methods (those that modify the B-Tree structure) are `insert_cell_ordered` and `split`.
//!
//! The search methods which implement a basic intra-node binary search are `find_index_key`, `find_table_rowid`, and `find_table_key`.
//!
//! We are currently relying on the KeyValue enum for key comparisons, which allows us to compare different types of keys (integers, floats, strings, blobs) according to SQLite's type affinity rules.
//!
//! The module level function `extract_key_from_payload` is used to extract a key from an index node payload depending on the type, which is essential for the search operations.
use crate::page::{BTreeCell, Page, PageType};
use crate::storage::pager::Pager;
use crate::utils::cmp::KeyValue;

use std::hash::Hasher;
use std::io;
use std::io::Cursor;

/// Extracts a key from an index node payload.
///
/// # Parameters
/// * `payload` - The raw payload bytes from which to extract the key
///
/// # Returns
/// A comparable key value extracted from the payload
///
/// # Errors
/// Returns an error if the payload cannot be parsed
pub fn extract_key_from_payload(payload: &[u8]) -> io::Result<KeyValue> {
    use crate::utils::serialization::{deserialize_values, SqliteValue};

    // Parse the payload as SQLite values
    let mut cursor = Cursor::new(payload);
    let (values, _) = deserialize_values(&mut cursor)?;

    // Use the first value as the key
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Empty payload in index node",
        ));
    }

    // Convert the first value to a comparable key
    match &values[0] {
        SqliteValue::Integer(i) => Ok(KeyValue::Integer(*i)),
        SqliteValue::Float(f) => Ok(KeyValue::Float(*f)),
        SqliteValue::String(s) => Ok(KeyValue::String(s.clone())),
        SqliteValue::Blob(b) => Ok(KeyValue::Blob(b.clone())),
        SqliteValue::Null => Ok(KeyValue::Null),
    }
}

/// Represents a B-Tree node with callback-based operations.
///
/// This structure provides both read-only and mutable operations on B-Tree nodes
/// using the Pager's callback system for safe page access.
#[derive(Debug, Clone)]
pub struct BTreeNode {
    /// Number of the page where the node is stored
    pub page_number: u32,
    /// Type of the node (interior or leaf)
    pub node_type: PageType,
}

impl BTreeNode {
    /// Creates a new B-Tree node reference.
    ///
    /// # Parameters
    /// * `page_number` - Number of the page where the node is stored
    /// * `node_type` - Type of the node
    ///
    /// # Returns
    /// A new BTreeNode instance
    pub fn new(page_number: u32, node_type: PageType) -> Self {
        BTreeNode {
            page_number,
            node_type,
        }
    }

    /// Opens an existing B-Tree node and validates its type.
    ///
    /// # Parameters
    /// * `page_number` - Number of the page where the node is stored
    /// * `node_type` - Expected type of the node
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The page cannot be loaded
    /// - The page type doesn't match the expected type
    /// - There are I/O issues
    ///
    /// # Returns
    /// A new BTreeNode instance if validation succeeds
    pub fn open(page_number: u32, node_type: PageType, pager: &Pager) -> io::Result<Self> {
        // Validate the page type using callback
        pager.get_page_callback(page_number, Some(node_type), |page| {
            let actual_type = page.page_type();
            if actual_type != node_type {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Page type mismatch: expected {:?}, found {:?}",
                        node_type, actual_type
                    ),
                ));
            }
            Ok(())
        })??;

        Ok(BTreeNode {
            page_number,
            node_type,
        })
    }

    /// Creates a new B-Tree leaf node.
    ///
    /// # Parameters
    /// * `node_type` - Type of leaf node to create (TableLeaf or IndexLeaf)
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node type is not a leaf type
    /// - The page cannot be created
    /// - There are I/O issues
    ///
    /// # Returns
    /// A new BTreeNode instance representing the created leaf
    pub fn create_leaf(node_type: PageType, pager: &Pager) -> io::Result<Self> {
        if !node_type.is_leaf() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Expected a leaf page type, got {:?}", node_type),
            ));
        }

        // Create the page
        let page_number = pager.create_btree_page(node_type, None)?;

        Ok(BTreeNode {
            page_number,
            node_type,
        })
    }

    /// Creates a new B-Tree interior node.
    ///
    /// # Parameters
    /// * `node_type` - Type of interior node to create (TableInterior or IndexInterior)
    /// * `right_most_page` - Optional page number of the rightmost child
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node type is not an interior type
    /// - The page cannot be created
    /// - There are I/O issues
    ///
    /// # Returns
    /// A new BTreeNode instance representing the created interior node
    pub fn create_interior(
        node_type: PageType,
        right_most_page: Option<u32>,
        pager: &Pager,
    ) -> io::Result<Self> {
        if !node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Expected an interior page type, got {:?}", node_type),
            ));
        }

        // Create the page
        let page_number = pager.create_btree_page(node_type, right_most_page)?;

        Ok(BTreeNode {
            page_number,
            node_type,
        })
    }

    /// Gets the number of cells in the node using a callback.
    ///
    /// # Parameters
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if the page is not a BTree page
    ///
    /// # Returns
    /// The number of cells in the node
    pub fn cell_count(&self, pager: &Pager) -> io::Result<u16> {
        pager.get_page_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => btree_page.header.cell_count,
            _ => unreachable!("Page type already validated"),
        })
    }

    /// Gets the free space in the node using a callback.
    ///
    /// # Parameters
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if the page is not a BTree page
    ///
    /// # Returns
    /// The amount of free space in bytes
    pub fn free_space(&self, pager: &Pager) -> io::Result<usize> {
        pager.get_page_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => btree_page.free_space(),
            _ => unreachable!("Page type already validated"),
        })
    }

    /// Gets the rightmost child page number (only for interior nodes).
    ///
    /// # Parameters
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node is not an interior node
    /// - The page is not a BTree page
    /// - The rightmost page is not set
    ///
    /// # Returns
    /// The page number of the rightmost child
    pub fn get_right_most_child(&self, pager: &Pager) -> io::Result<u32> {
        if !self.node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot get rightmost child from leaf node",
            ));
        }

        pager.get_page_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => btree_page.header.right_most_page.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Interior node missing rightmost page",
                )
            }),
            _ => unreachable!("Page type already validated"),
        })?
    }

    /// Gets an owned copy of a cell at the specified index.
    ///
    /// # Parameters
    /// * `index` - Index of the cell to retrieve
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The index is out of bounds
    /// - The page is not a BTree page
    ///
    /// # Returns
    /// A cloned copy of the cell
    pub fn get_cell_owned(&self, index: u16, pager: &Pager) -> io::Result<BTreeCell> {
        pager.get_page_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => {
                if index >= btree_page.header.cell_count {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Cell index {} out of bounds", index),
                    ));
                }
                Ok(btree_page.cells[index as usize].clone())
            }
            _ => unreachable!("Page type already validated"),
        })?
    }

    /// Searches for the appropriate position in an index node based on the key.
    ///
    /// # Parameters
    /// * `index_key` - The key to search for
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node is not an index node
    /// - The page is not a BTree page
    /// - Key extraction fails
    ///
    /// # Returns
    /// A tuple with:
    /// - `true` if an exact match was found, `false` otherwise
    /// - The index where the key should be inserted
    pub fn find_index_key(&self, index_key: &KeyValue, pager: &Pager) -> io::Result<(bool, u16)> {
        if !self.node_type.is_index() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Node is not an index node",
            ));
        }

        pager.get_page_callback(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    let cell_count = btree_page.header.cell_count;

                    // Binary search
                    let mut left = 0;
                    let mut right = cell_count.saturating_sub(1) as i32;

                    while left <= right {
                        let mid = left + (right - left) / 2;
                        let mid_idx = mid as u16;

                        let cell = &btree_page.cells[mid as usize];
                        let cell_key = match cell {
                            BTreeCell::IndexLeaf(leaf_cell) => {
                                extract_key_from_payload(&leaf_cell.payload)?
                            }
                            BTreeCell::IndexInterior(interior_cell) => {
                                extract_key_from_payload(&interior_cell.payload)?
                            }
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Expected index cell type",
                                ))
                            }
                        };

                        match cell_key.partial_cmp(index_key) {
                            Some(std::cmp::Ordering::Equal) => {
                                return Ok((true, mid_idx));
                            }
                            Some(std::cmp::Ordering::Greater) => {
                                right = mid - 1;
                            }
                            Some(std::cmp::Ordering::Less) => {
                                left = mid + 1;
                            }
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Incomparable key types",
                                ));
                            }
                        }
                    }

                    // No exact match found, go to the left position.
                    // This will porduce a left-biased search, which is more efficient for most disk architectures.
                    Ok((false, left as u16))
                }
                _ => unreachable!("Page type already validated"),
            }
        })?
    }

    /// Searches for a record by rowid in a table leaf node.
    ///
    /// # Parameters
    /// * `rowid` - Row ID to search for
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node is not a table leaf
    /// - The page is not a BTree page
    ///
    /// # Returns
    /// A tuple with:
    /// - `true` if the exact rowid was found, `false` otherwise
    /// - Index of the cell containing the rowid or where it should be inserted
    pub fn find_table_rowid(&self, rowid: i64, pager: &Pager) -> io::Result<(bool, u16)> {
        if self.node_type != PageType::TableLeaf {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Node is not a table leaf",
            ));
        }

        pager.get_page_callback(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    let cell_count = btree_page.header.cell_count;

                    if cell_count == 0 {
                        return Ok((false, 0));
                    }

                    // Binary search
                    let mut left = 0;
                    let mut right = cell_count.saturating_sub(1) as i32;

                    while left <= right {
                        let mid = left + (right - left) / 2;
                        let mid_idx = mid as u16;

                        let cell = &btree_page.cells[mid as usize];
                        let mid_rowid = match cell {
                            BTreeCell::TableLeaf(cell) => cell.row_id,
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Expected table leaf cell",
                                ))
                            }
                        };

                        if mid_rowid == rowid {
                            return Ok((true, mid_idx));
                        } else if mid_rowid > rowid {
                            right = mid - 1;
                        } else {
                            left = mid + 1;
                        }
                    }

                    Ok((false, left as u16)) // As always, go to the left position
                }
                _ => unreachable!("Page type already validated"),
            }
        })?
    }

    /// Searches for a key in a table interior node.
    /// If not found, returns the child page that may contain the key
    ///
    /// # Parameters
    /// * `key` - Key to search for (rowid)
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node is not a table interior
    /// - The page is not a BTree page
    ///
    /// # Returns
    /// A tuple with:
    /// - `true` if the exact key was found, `false` otherwise
    /// - Page number of the child that may contain the key
    /// - Index of the cell containing the key or where it should be inserted
    pub fn find_table_key(&self, key: i64, pager: &Pager) -> io::Result<(bool, u32, u16)> {
        if self.node_type != PageType::TableInterior {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Node is not a table interior",
            ));
        }

        pager.get_page_callback(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    let cell_count = btree_page.header.cell_count;

                    // Binary search
                    let mut left = 0;
                    let mut right = cell_count.saturating_sub(1) as i32;

                    while left <= right {
                        let mid = left + (right - left) / 2;
                        let mid_idx = mid as u16;
                        // println!("Searching: left={}, right={}, mid={}", left, right, mid);
                        let cell = &btree_page.cells[mid as usize];
                        // println!("Cell at mid {}: {:?}", mid_idx, cell);
                        let mid_key = match cell {
                            BTreeCell::TableInterior(cell) => cell.key,
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Expected table interior cell",
                                ))
                            }
                        };

                        match mid_key.partial_cmp(&key) {
                            Some(std::cmp::Ordering::Equal) => {
                                let child = match cell {
                                    BTreeCell::TableInterior(cell) => cell.left_child_page,
                                    _ => unreachable!(),
                                };
                                print!(
                                    "Key {} found at index {}, returning child page {:?}",
                                    key, mid_idx, child
                                );
                                return Ok((true, child, mid_idx));
                            }
                            Some(std::cmp::Ordering::Greater) => {
                                // println!("Key {} is less than mid key {}", key, mid_key);
                                right = mid - 1;
                            }
                            Some(std::cmp::Ordering::Less) => {
                                // println!("Key {} is greater than mid key {}", key, mid_key);
                                left = mid + 1;
                            }
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Incomparable key types",
                                ));
                            }
                        }
                    }

                    // Handle edge cases
                    if let Some(right_most_page) = btree_page.header.right_most_page {
                        if left >= cell_count as i32 {
                            // If left is bigger than the last index, return the rightmost page.
                            // This handles the case where the key is greater than all keys in the node.
                            return Ok((false, right_most_page, cell_count));
                        }
                    }

                    if cell_count == 0 || right < 0 {
                        // println!("No cells found or right is negative");
                        // This means the node is empty or we are at the leftmost position
                        // Return the leftmost child page
                        if cell_count == 0 {
                            return Err(io::Error::new(io::ErrorKind::NotFound, "Node is empty"));
                        }

                        match &btree_page.cells[0] {
                            BTreeCell::TableInterior(cell) => Ok((false, cell.left_child_page, 0)),
                            _ => Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Expected table interior cell",
                            )),
                        }
                    } else {
                        let idx = left as u16; // If not found, return the left position
                                               // println!("Found position: idx={}", idx);
                        let cell = &btree_page.cells[idx as usize];
                        match cell {
                            BTreeCell::TableInterior(cell) => {
                                Ok((false, cell.left_child_page, idx))
                            }
                            _ => Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Expected table interior cell",
                            )),
                        }
                    }
                }
                _ => unreachable!("Page type already validated"),
            }
        })?
    }

    /// Sets the rightmost child page number (only for interior nodes).
    ///
    /// # Parameters
    /// * `page_number` - Page number of the rightmost child
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The node is not an interior node
    /// - The page is not a BTree page
    ///
    /// # Returns
    /// Success or failure indication
    pub fn set_right_most_child(&self, page_number: u32, pager: &Pager) -> io::Result<()> {
        if !self.node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot set rightmost child on leaf node",
            ));
        }

        pager.get_page_mut_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => {
                btree_page.header.right_most_page = Some(page_number);
                Ok(())
            }
            _ => unreachable!("Page type already validated"),
        })
    }

    /// Inserts a cell into the node without ordering.
    ///
    /// # Parameters
    /// * `cell` - Cell to insert
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The cell type is incompatible with the node type
    /// - There is insufficient space
    /// - The page is not a BTree page
    ///
    /// # Returns
    /// Index of the inserted cell
    pub fn insert_cell(&self, cell: BTreeCell, pager: &Pager) -> io::Result<u16> {
        // Verify cell type compatibility
        match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {}
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {}
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {}
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Cell type incompatible with node type: {:?}",
                        self.node_type
                    ),
                ));
            }
        }

        let cell_index = pager.get_page_mut_callback(
            self.page_number,
            Some(self.node_type),
            |page| match page {
                Page::BTree(btree_page) => {
                    btree_page.add_cell(cell)?;
                    Ok(btree_page.header.cell_count - 1)
                }
                _ => unreachable!("Page type already validated"),
            },
        )?;
        Ok(cell_index)
    }

    /// Inserts a cell into the node in the correct order based on its key.
    ///
    /// # Parameters
    /// * `cell` - Cell to insert
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - The cell type is incompatible
    /// - There is insufficient space and the node cannot be split
    /// - Key extraction fails
    ///
    /// # Returns
    /// A tuple with:
    /// - `true` if the node was split, `false` otherwise
    /// - Median key (if the node was split)
    /// - New node (if the node was split)
    pub fn insert_cell_ordered(
        &self,
        cell: BTreeCell,
        pager: &Pager,
    ) -> io::Result<(bool, Option<i64>, Option<BTreeNode>)> {
        // Verify cell type compatibility
        match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {}
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {}
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {}
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Cell type incompatible with node type: {:?}",
                        self.node_type
                    ),
                ));
            }
        }

        // Calculate required space
        let cell_size = cell.size();
        let cell_index_size = 2; // 2 bytes for cell index

        // Check if there's enough space
        let free_space = self.free_space(pager)?;

        if free_space < cell_size + cell_index_size {
            // Not enough space, need to split
            let (new_node, median_key, _) = self.split(pager)?;

            let insert_in_new = self.should_insert_in_new_node(&cell, median_key)?;

            if insert_in_new {
                // Insert in the new node (recursively)
                new_node.insert_cell_ordered(cell, pager)?;
                return Ok((true, Some(median_key), Some(new_node)));
            } else {
                // Insert in the current node (recursively)
                self.insert_cell_ordered(cell, pager)?;
                return Ok((true, Some(median_key), Some(new_node)));
            }
        }
        // If the node is empty, insert at the start
        if self.cell_count(pager)? == 0 {
            return self
                .insert_cell(cell, pager)
                .map(|_index| (false, None, None));
        }

        // There's enough space, find the correct position
        let position = self.find_position_for_cell(&cell, pager)?;

        // Insert the cell at the calculated position
        pager.get_page_mut_callback(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    // Calculate cell offset
                    let offset = if btree_page.header.cell_count > 0
                        && position < btree_page.header.cell_count
                    {
                        let existing_offset = btree_page.cell_indices[position as usize];
                        existing_offset - cell_size as u16
                    } else {
                        btree_page.header.content_start_offset - cell_size as u16
                    };

                    // Insert the cell and its index
                    if position < btree_page.header.cell_count {
                        btree_page.cells.insert(position as usize, cell);
                        btree_page.cell_indices.insert(position as usize, offset);
                    } else {
                        btree_page.cells.push(cell);
                        btree_page.cell_indices.push(offset);
                    }

                    // Update page metadata
                    btree_page.header.cell_count += 1;
                    btree_page.header.content_start_offset = btree_page
                        .cell_indices
                        .iter()
                        .min()
                        .copied()
                        .unwrap_or(btree_page.header.content_start_offset);

                    Ok(())
                }
                _ => unreachable!("Page type already validated"),
            }
        })?;

        Ok((false, None, None))
    }

    /// Splits the current node into two nodes.
    /// The split function divides the cells into two halves,
    /// creating a new node for the second half and returning it along with the median key.
    ///
    /// # Parameters
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if:
    /// - There are insufficient cells to split
    /// - The new node cannot be created
    /// - Key extraction fails
    ///
    /// # Returns
    /// A tuple with:
    /// - The new node created during the split
    /// - The median key
    /// - The index of the median cell
    pub fn split(&self, pager: &Pager) -> io::Result<(BTreeNode, i64, u16)> {
        let cell_count = self.cell_count(pager)?;

        if cell_count <= 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Not enough cells to split node",
            ));
        }

        let split_point = cell_count / 2;

        // Create a new node of the same type
        let new_node = match self.node_type {
            PageType::TableLeaf | PageType::IndexLeaf => {
                BTreeNode::create_leaf(self.node_type, pager)?
            }
            PageType::TableInterior | PageType::IndexInterior => {
                BTreeNode::create_interior(self.node_type, None, pager)?
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Cannot split a non-BTree page",
                ));
            }
        };

        // Extract cells to move and determine median key
        let (cells_to_move, median_key, median_index) =
            self.prepare_split_data(split_point, pager)?;

        // Move cells to the new node
        for cell in cells_to_move {
            new_node.insert_cell(cell, pager)?;
        }

        // Handle interior node rightmost pointer
        if self.node_type.is_interior() {
            if let Ok(right_most) = self.get_right_most_child(pager) {
                new_node.set_right_most_child(right_most, pager)?;
            }
        }

        Ok((new_node, median_key, median_index))
    }

    /// Determines if a cell should be inserted in the new node after a split.
    /// Basically checks if the cell's key is greater than or equal to the median key.
    /// # Parameters
    /// * `cell` - The cell to be inserted
    /// * `median_key` - The median key from the split
    ///
    /// # Errors
    /// Returns an error if key extraction fails
    ///
    /// # Returns
    /// `true` if the cell should go in the new node, `false` otherwise
    fn should_insert_in_new_node(&self, cell: &BTreeCell, median_key: i64) -> io::Result<bool> {
        match (&self.node_type, cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
                // println!("Checking if table cell with row_id {} should go in new node with median key {}", table_cell.row_id, median_key);
                // println!("Comparing {} >= {}", table_cell.row_id, median_key);

                Ok(table_cell.row_id >= median_key)
            }
            (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
                Ok(table_cell.key >= median_key)
            }
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(index_cell)) => {
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                let median_key_value = KeyValue::Integer(median_key);

                match key_value.partial_cmp(&median_key_value) {
                    Some(std::cmp::Ordering::Less) => Ok(false),
                    Some(_) => Ok(true),
                    None => {
                        // Fallback for incomparable types
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&key_value, &mut hasher);
                        let hashed_key = hasher.finish() as i64;
                        Ok(hashed_key >= median_key)
                    }
                }
            }
            (PageType::IndexInterior, BTreeCell::IndexInterior(index_cell)) => {
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                let median_key_value = KeyValue::Integer(median_key);

                match key_value.partial_cmp(&median_key_value) {
                    Some(std::cmp::Ordering::Less) => Ok(false),
                    Some(_) => Ok(true),
                    None => {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&key_value, &mut hasher);
                        let hashed_key = hasher.finish() as i64;
                        Ok(hashed_key >= median_key)
                    }
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cell type incompatible with node type",
            )),
        }
    }

    /// Finds the appropriate position to insert a cell based on its key.
    ///
    /// # Parameters
    /// * `cell` - The cell to find a position for
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if key extraction or comparison fails
    ///
    /// # Returns
    /// The index where the cell should be inserted
    fn find_position_for_cell(&self, cell: &BTreeCell, pager: &Pager) -> io::Result<u16> {
        match (self.node_type, cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
                // Use binary search for table leaf
                pager.get_page_callback(self.page_number, Some(self.node_type), |page| {
                    match page {
                        Page::BTree(btree_page) => {
                            if btree_page.header.cell_count == 0 {
                                return Ok(0);
                            }

                            let mut left = 0;
                            let mut right = btree_page.header.cell_count as i32 - 1;

                            while left <= right {
                                let mid = left + (right - left) / 2;
                                let mid_cell = &btree_page.cells[mid as usize];

                                match mid_cell {
                                    BTreeCell::TableLeaf(leaf_cell) => {
                                        if leaf_cell.row_id == table_cell.row_id {
                                            return Ok(mid as u16); // Replace existing
                                        } else if leaf_cell.row_id > table_cell.row_id {
                                            right = mid - 1;
                                        } else {
                                            left = mid + 1;
                                        }
                                    }
                                    _ => {
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "Expected table leaf cell",
                                        ))
                                    }
                                }
                            }

                            Ok(left as u16)
                        }
                        _ => unreachable!("Page type already validated"),
                    }
                })?
            }
            (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => pager
                .get_page_callback(self.page_number, Some(self.node_type), |page| match page {
                    Page::BTree(btree_page) => {
                        if btree_page.header.cell_count == 0 {
                            return Ok(0);
                        }

                        let mut left = 0;
                        let mut right = btree_page.header.cell_count as i32 - 1;

                        while left <= right {
                            let mid = left + (right - left) / 2;
                            let mid_cell = &btree_page.cells[mid as usize];

                            match mid_cell {
                                BTreeCell::TableInterior(interior_cell) => {
                                    if interior_cell.key == table_cell.key {
                                        return Ok(mid as u16);
                                    } else if interior_cell.key > table_cell.key {
                                        right = mid - 1;
                                    } else {
                                        left = mid + 1;
                                    }
                                }
                                _ => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "Expected table interior cell",
                                    ))
                                }
                            }
                        }

                        Ok(left as u16)
                    }
                    _ => unreachable!("Page type already validated"),
                })?,
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(index_cell)) => {
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                let (_, idx) = self.find_index_key(&key_value, pager)?;
                Ok(idx)
            }
            (PageType::IndexInterior, BTreeCell::IndexInterior(index_cell)) => {
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                let (_, idx) = self.find_index_key(&key_value, pager)?;
                Ok(idx)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cell type incompatible with node type",
            )),
        }
    }

    /// Prepares data for splitting a node.
    ///
    /// # Parameters
    /// * `split_point` - The index at which to split the node
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if there are insufficient cells or key extraction fails
    ///
    /// # Returns
    /// A tuple with:
    /// - Vector of cells to move to the new node
    /// - The median key
    /// - The index of the median cell
    fn prepare_split_data(
        &self,
        split_point: u16,
        pager: &Pager,
    ) -> io::Result<(Vec<BTreeCell>, i64, u16)> {
        let result =
            pager.get_page_mut_callback(self.page_number, Some(self.node_type), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        if btree_page.cells.len() <= split_point as usize {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "Not enough cells to split",
                            ));
                        }

                        let mut cells_to_move = Vec::new();

                        let median_info = if self.node_type.is_interior() {
                            // For interior nodes, handle differently
                            let right_most_page = btree_page.header.right_most_page;

                            if right_most_page.is_none() {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Interior node without rightmost page pointer",
                                ));
                            }

                            let mid_cell_idx = split_point - 1;
                            let median_key = match &btree_page.cells[mid_cell_idx as usize] {
                                BTreeCell::TableInterior(cell) => {
                                    btree_page.header.right_most_page = Some(cell.left_child_page);
                                    (cell.key, mid_cell_idx)
                                }
                                BTreeCell::IndexInterior(cell) => {
                                    btree_page.header.right_most_page = Some(cell.left_child_page);

                                    let key_value = extract_key_from_payload(&cell.payload)?;
                                    let key = match key_value {
                                        KeyValue::Integer(i) => i,
                                        KeyValue::Float(f) => f as i64,
                                        _ => {
                                            let mut hasher =
                                                std::collections::hash_map::DefaultHasher::new();
                                            std::hash::Hash::hash(&key_value, &mut hasher);
                                            hasher.finish() as i64
                                        }
                                    };
                                    (key, mid_cell_idx)
                                }
                                _ => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "Expected interior cell",
                                    ))
                                }
                            };

                            // Collect cells to move (excluding the median)
                            for i in (mid_cell_idx as usize + 1)..btree_page.cells.len() {
                                // println!("Moving cell from index {}: {:?}", i, btree_page.cells[i]);
                                cells_to_move.push(btree_page.cells[i].clone());
                            }

                            // Remove cells from original node except the median
                            btree_page.cells.truncate((mid_cell_idx + 1) as usize);
                            btree_page
                                .cell_indices
                                .truncate((mid_cell_idx + 1) as usize);
                            // println!("Truncated cells to: {:?}", btree_page.cells);
                            median_key
                        } else {
                            // For leaf nodes
                            let median_cell = &btree_page.cells[split_point as usize];

                            let median_key = match (self.node_type, median_cell) {
                                (PageType::TableLeaf, BTreeCell::TableLeaf(cell)) => {
                                    (cell.row_id, split_point)
                                }
                                (PageType::IndexLeaf, BTreeCell::IndexLeaf(cell)) => {
                                    let key_value = extract_key_from_payload(&cell.payload)?;
                                    let key = match key_value {
                                        KeyValue::Integer(i) => i,
                                        KeyValue::Float(f) => f as i64,
                                        _ => {
                                            let mut hasher =
                                                std::collections::hash_map::DefaultHasher::new();
                                            std::hash::Hash::hash(&key_value, &mut hasher);
                                            hasher.finish() as i64
                                        }
                                    };
                                    (key, split_point)
                                }
                                _ => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "Expected leaf cell",
                                    ))
                                }
                            };

                            // Collect cells to move
                            for i in (split_point as usize)..btree_page.cells.len() {
                                cells_to_move.push(btree_page.cells[i].clone());
                                // println!("Moving cell from index {}: {:?}", i, btree_page.cells[i]);
                            }

                            // Remove cells from original node except the median
                            btree_page.cells.truncate((split_point + 1) as usize);
                            // println!("Truncated cells to: {:?}", btree_page.cells);
                            btree_page.cell_indices.truncate((split_point + 1) as usize);

                            median_key
                        };

                        // Update metadata
                        btree_page.header.cell_count = btree_page.cells.len() as u16;
                        btree_page.update_content_start_offset();

                        Ok((cells_to_move, median_info.0, median_info.1))
                    }
                    _ => unreachable!("Page type already validated"),
                }
            })?;
        Ok(result)
    }

    /// Gets a mutable reference to a cell using a callback.
    ///
    /// # Parameters
    /// * `index` - Index of the cell to retrieve
    /// * `pager` - Reference to the pager
    /// * `f` - Callback function that receives the mutable cell reference
    ///
    /// # Errors
    /// Returns an error if the index is out of bounds
    ///
    /// # Returns
    /// The result of the callback function
    pub fn with_cell_mut<F, R>(&self, index: u16, pager: &Pager, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut BTreeCell) -> io::Result<R>,
    {
        pager.get_page_mut_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => {
                if index >= btree_page.header.cell_count {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Cell index {} out of bounds", index),
                    ));
                }
                Ok(f(&mut btree_page.cells[index as usize]))
            }
            _ => unreachable!("Page type already validated"),
        })?
    }

    /// Removes a cell from the node.
    ///
    /// # Parameters
    /// * `index` - Index of the cell to remove
    /// * `pager` - Reference to the pager
    ///
    /// # Errors
    /// Returns an error if the index is out of bounds
    ///
    /// # Returns
    /// The removed cell
    pub fn remove_cell(&self, index: u16, pager: &Pager) -> io::Result<BTreeCell> {
        pager.get_page_mut_callback(self.page_number, Some(self.node_type), |page| match page {
            Page::BTree(btree_page) => {
                if index >= btree_page.header.cell_count {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Cell index {} out of bounds", index),
                    ));
                }

                let removed_cell = btree_page.cells.remove(index as usize);
                btree_page.cell_indices.remove(index as usize);
                btree_page.header.cell_count -= 1;
                btree_page.update_content_start_offset();

                Ok(removed_cell)
            }
            _ => unreachable!("Page type already validated"),
        })
    }

    /// Executes a callback with read-only access to the entire page.
    ///
    /// # Parameters
    /// * `pager` - Reference to the pager
    /// * `f` - Callback function that receives the page reference
    ///
    /// # Errors
    /// Returns an error if there are I/O issues
    ///
    /// # Returns
    /// The result of the callback function
    pub fn with_page<F, R>(&self, pager: &Pager, f: F) -> io::Result<R>
    where
        F: FnOnce(&Page) -> R,
    {
        pager.get_page_callback(self.page_number, Some(self.node_type), f)
    }

    /// Executes a callback with mutable access to the entire page.
    ///
    /// # Parameters
    /// * `pager` - Reference to the pager
    /// * `f` - Callback function that receives the mutable page reference
    ///
    /// # Errors
    /// Returns an error if there are I/O issues
    ///
    /// # Returns
    /// The result of the callback function
    pub fn with_page_mut<F, R>(&self, pager: &Pager, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut Page) -> io::Result<R>,
    {
        pager.get_page_mut_callback(self.page_number, Some(self.node_type), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{IndexLeafCell, TableInteriorCell, TableLeafCell};
    use crate::storage::pager::Pager;
    use crate::utils::serialization::SqliteValue;
    use tempfile::tempdir;

    fn create_test_pager() -> Pager {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        Pager::create(db_path, 4096, None, 0).unwrap()
    }

    #[test]
    fn test_create_leaf_node() {
        let pager = create_test_pager();

        // Test creating a table leaf node
        let result = BTreeNode::create_leaf(PageType::TableLeaf, &pager);
        assert!(result.is_ok());

        let node = result.unwrap();
        assert_eq!(node.node_type, PageType::TableLeaf);
        assert_eq!(node.cell_count(&pager).unwrap(), 0);
        assert!(node.free_space(&pager).unwrap() > 0);
    }

    #[test]
    fn test_create_interior_node() {
        let pager = create_test_pager();

        // Test creating a table interior node
        let result = BTreeNode::create_interior(PageType::TableInterior, Some(42), &pager);
        assert!(result.is_ok());

        let node = result.unwrap();
        assert_eq!(node.node_type, PageType::TableInterior);
        assert_eq!(node.get_right_most_child(&pager).unwrap(), 42);
    }

    #[test]
    fn test_invalid_node_creation() {
        let pager = create_test_pager();

        // Test creating a leaf with interior type should fail
        let result = BTreeNode::create_leaf(PageType::TableInterior, &pager);
        assert!(result.is_err());

        // Test creating an interior with leaf type should fail
        let result = BTreeNode::create_interior(PageType::TableLeaf, Some(42), &pager);
        assert!(result.is_err());
    }

    #[test]
    fn test_node_operations() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Create and insert a cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        let index = node.insert_cell(cell, &pager).unwrap();
        assert_eq!(index, 0);
        assert_eq!(node.cell_count(&pager).unwrap(), 1);
    }

    #[test]
    fn test_ordered_insertion() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert cells in random order
        let row_ids = [5, 1, 8, 3, 7];

        for &row_id in &row_ids {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 4,
                row_id,
                payload: vec![row_id as u8; 4],
                overflow_page: None,
            });

            let (split, _, _) = node.insert_cell_ordered(cell, &pager).unwrap();
            assert!(!split); // Should not split with so few cells
        }

        assert_eq!(node.cell_count(&pager).unwrap(), 5);

        // Verify cells are in order
        for i in 0..5 {
            let cell = node.get_cell_owned(i, &pager).unwrap();
            match cell {
                BTreeCell::TableLeaf(leaf_cell) => {
                    if i > 0 {
                        let prev_cell = node.get_cell_owned(i - 1, &pager).unwrap();
                        match prev_cell {
                            BTreeCell::TableLeaf(prev_leaf) => {
                                assert!(prev_leaf.row_id < leaf_cell.row_id);
                            }
                            _ => panic!("Expected table leaf cell"),
                        }
                    }
                }
                _ => panic!("Expected table leaf cell"),
            }
        }
    }

    #[test]
    fn test_find_table_rowid() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert some test data
        let row_ids = [10, 20, 30, 40, 50];
        for &row_id in &row_ids {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 4,
                row_id,
                payload: vec![row_id as u8; 4],
                overflow_page: None,
            });
            node.insert_cell_ordered(cell, &pager).unwrap();
        }

        // Test finding existing keys
        for &row_id in &row_ids {
            let (found, _) = node.find_table_rowid(row_id, &pager).unwrap();
            assert!(found, "Should find row_id {}", row_id);
        }

        // Test finding non-existent keys
        let (found, idx) = node.find_table_rowid(15, &pager).unwrap();
        assert!(!found);
        assert_eq!(idx, 1); // Should be between 10 and 20

        let (found, idx) = node.find_table_rowid(5, &pager).unwrap();
        assert!(!found);
        assert_eq!(idx, 0); // Should be before all elements

        let (found, idx) = node.find_table_rowid(60, &pager).unwrap();
        assert!(!found);
        assert_eq!(idx, 5); // Should be after all elements
    }

    #[test]
    fn test_index_key_operations() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::IndexLeaf, &pager).unwrap();

        // Create some test index entries
        let keys = [10, 20, 30, 40, 50];
        for &key in &keys {
            // Create payload with the key
            let mut payload = Vec::new();
            crate::utils::serialization::serialize_values(
                &[SqliteValue::Integer(key)],
                &mut payload,
            )
            .unwrap();

            let cell = BTreeCell::IndexLeaf(IndexLeafCell {
                payload_size: payload.len() as u64,
                payload,
                overflow_page: None,
            });

            node.insert_cell_ordered(cell, &pager).unwrap();
        }

        for &key in &keys {
            let key_value = KeyValue::Integer(key);
            let (found, _) = node.find_index_key(&key_value, &pager).unwrap();
            assert!(found, "Should find key {}", key);
        }

        // Test non-existent key
        let key_value = KeyValue::Integer(25);
        let (found, idx) = node.find_index_key(&key_value, &pager).unwrap();
        assert!(!found);
        assert_eq!(idx, 2); // Should be between 20 and 30
    }

    #[test]
    fn test_node_split() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert enough cells to force a split
        for i in 1..=100 {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 100, // Large payload to force split sooner
                row_id: i,
                payload: vec![i as u8; 100],
                overflow_page: None,
            });

            let (split, median_key, new_node) = node.insert_cell_ordered(cell, &pager).unwrap();

            if split {
                assert!(median_key.is_some());
                assert!(new_node.is_some());

                let median = median_key.unwrap();
                let new_node = new_node.unwrap();

                // Verify split properties
                assert!(median > 0);
                assert!(node.cell_count(&pager).unwrap() > 0);
                assert!(new_node.cell_count(&pager).unwrap() > 0);

                // println!("Split occurred at iteration {}, median key: {}", i, median);
                break;
            }
        }
    }

    #[test]
    fn test_cell_type_validation() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Try to insert incompatible cell type
        let cell = BTreeCell::TableInterior(TableInteriorCell {
            left_child_page: 42,
            key: 100,
        });

        let result = node.insert_cell(cell, &pager);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("incompatible"));
    }

    #[test]
    fn test_interior_node_operations() {
        let pager = create_test_pager();
        let right_most_page = 100;

        // Create interior node
        let node =
            BTreeNode::create_interior(PageType::TableInterior, Some(right_most_page), &pager)
                .unwrap();

        assert_eq!(node.get_right_most_child(&pager).unwrap(), right_most_page);

        // Change rightmost child
        node.set_right_most_child(200, &pager).unwrap();

        // Verify change
        assert_eq!(node.get_right_most_child(&pager).unwrap(), 200);
    }

    #[test]
    fn test_table_interior_key_search() {
        let pager = create_test_pager();
        let node = BTreeNode::create_interior(PageType::TableInterior, Some(1000), &pager).unwrap();

        // Insert some interior cells
        let keys_and_children = [(10, 100), (20, 200), (30, 300), (40, 400)];

        for (key, child) in keys_and_children {
            let cell = BTreeCell::TableInterior(TableInteriorCell {
                left_child_page: child,
                key,
            });
            let insert_result = node.insert_cell_ordered(cell, &pager).unwrap();

            assert!(!insert_result.0); // Should not split
            assert!(insert_result.1.is_none());
            assert!(insert_result.2.is_none());
        }

        // Test finding existing keys
        for (key, expected_child) in keys_and_children {
            let (found, child_page, _) = node.find_table_key(key, &pager).unwrap();
            assert!(found);
            assert_eq!(child_page, expected_child);
        }

        // Test finding keys that fall between existing keys
        let (found, child_page, _) = node.find_table_key(15, &pager).unwrap();
        assert!(!found);
        assert_eq!(child_page, 200); // Should go to child of key 20
                                     // println!("Found key 15, should go to child of key 20: {}", child_page);
                                     // Test key smaller than all keys
        let (found, child_page, _) = node.find_table_key(5, &pager).unwrap();
        assert!(!found);
        assert_eq!(child_page, 100); // Should go to child of key 10
                                     // println!("Found key 5, should go to child of key 10: {}", child_page);
                                     // Test key larger than all keys
        let (found, child_page, _) = node.find_table_key(50, &pager).unwrap();
        assert!(!found);
        assert_eq!(child_page, 1000); // Should go to rightmost child
    }

    #[test]
    fn test_error_handling() {
        let pager = create_test_pager();

        // Test opening non-existent page
        let result = BTreeNode::open(999, PageType::TableLeaf, &pager);
        assert!(result.is_err());

        // Test accessing rightmost child on leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();
        let result = node.get_right_most_child(&pager);
        assert!(result.is_err());
    }

    #[test]
    fn test_callback_safety() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Test that callbacks work properly
        let cell_count1 = node.cell_count(&pager).unwrap();
        let cell_count2 = node.cell_count(&pager).unwrap();
        let free_space = node.free_space(&pager).unwrap();

        assert_eq!(cell_count1, cell_count2);
        assert!(free_space > 0);

        // Test with_page callback
        let page_number = node.with_page(&pager, |page| page.page_number()).unwrap();

        assert_eq!(page_number, node.page_number);
    }

    #[test]
    fn test_cell_mutation_with_callback() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert a cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });

        node.insert_cell(cell, &pager).unwrap();

        // Modify the cell using callback
        node.with_cell_mut(0, &pager, |cell| {
            match cell {
                BTreeCell::TableLeaf(leaf_cell) => {
                    leaf_cell.payload[0] = 255; // Change first byte
                    Ok(())
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Expected table leaf cell",
                )),
            }
        })
        .unwrap();

        // Verify the change
        let modified_cell = node.get_cell_owned(0, &pager).unwrap();
        match modified_cell {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.payload[0], 255);
                assert_eq!(leaf_cell.row_id, 42);
            }
            _ => panic!("Expected table leaf cell"),
        }
    }

    #[test]
    fn test_cell_removal() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert multiple cells
        let row_ids = [10, 20, 30, 40, 50];
        for &row_id in &row_ids {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 4,
                row_id,
                payload: vec![row_id as u8; 4],
                overflow_page: None,
            });
            node.insert_cell_ordered(cell, &pager).unwrap();
        }

        assert_eq!(node.cell_count(&pager).unwrap(), 5);

        // Remove middle cell
        let removed_cell = node.remove_cell(2, &pager).unwrap();
        match removed_cell {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.row_id, 30);
            }
            _ => panic!("Expected table leaf cell"),
        }

        // Verify cell count decreased
        assert_eq!(node.cell_count(&pager).unwrap(), 4);

        // Verify remaining cells are still in order
        let remaining_row_ids = [10, 20, 40, 50];
        for (i, &expected_row_id) in remaining_row_ids.iter().enumerate() {
            let cell = node.get_cell_owned(i as u16, &pager).unwrap();
            match cell {
                BTreeCell::TableLeaf(leaf_cell) => {
                    assert_eq!(leaf_cell.row_id, expected_row_id);
                }
                _ => panic!("Expected table leaf cell"),
            }
        }
    }

    #[test]
    fn test_large_payload_handling() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Test with large payload that might cause overflow
        let large_payload = vec![0xAA; 2000]; // 2KB payload
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: large_payload.len() as u64,
            row_id: 1,
            payload: large_payload.clone(),
            overflow_page: None,
        });

        let result = node.insert_cell(cell, &pager);

        // Should either succeed or fail gracefully
        match result {
            Ok(_) => {
                // If it succeeds, verify the cell was added
                assert_eq!(node.cell_count(&pager).unwrap(), 1);
                let retrieved_cell = node.get_cell_owned(0, &pager).unwrap();
                match retrieved_cell {
                    BTreeCell::TableLeaf(leaf_cell) => {
                        assert_eq!(leaf_cell.row_id, 1);
                        assert_eq!(leaf_cell.payload_size as usize, large_payload.len());
                    }
                    _ => panic!("Expected table leaf cell"),
                }
            }
            Err(e) => {
                // If it fails, should be due to insufficient space
                assert!(e.to_string().contains("space") || e.to_string().contains("bytes"));
            }
        }
    }

    #[test]
    fn test_multiple_pager_operations() {
        let pager = create_test_pager();
        let node1 = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();
        let node2 = BTreeNode::create_leaf(PageType::IndexLeaf, &pager).unwrap();

        // Both nodes should have different page numbers
        assert_ne!(node1.page_number, node2.page_number);

        // Operations on one node shouldn't affect the other
        let cell1 = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 5,
            row_id: 100,
            payload: vec![1, 2, 3, 4, 5],
            overflow_page: None,
        });

        let mut payload2 = Vec::new();
        crate::utils::serialization::serialize_values(&[SqliteValue::Integer(200)], &mut payload2)
            .unwrap();

        let cell2 = BTreeCell::IndexLeaf(IndexLeafCell {
            payload_size: payload2.len() as u64,
            payload: payload2,
            overflow_page: None,
        });

        node1.insert_cell(cell1, &pager).unwrap();
        node2.insert_cell(cell2, &pager).unwrap();

        assert_eq!(node1.cell_count(&pager).unwrap(), 1);
        assert_eq!(node2.cell_count(&pager).unwrap(), 1);
    }

    #[test]
    fn test_page_callback_error_handling() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Test callback that returns an error
        let result = node.with_page_mut(&pager, |_page| -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "Test error"))
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Test error"));
    }

    #[test]
    fn test_concurrent_callback_safety() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert a cell first
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });
        node.insert_cell(cell, &pager).unwrap();

        // Multiple read operations should work fine
        let count1 = node.cell_count(&pager).unwrap();
        let count2 = node.cell_count(&pager).unwrap();
        let space1 = node.free_space(&pager).unwrap();
        let space2 = node.free_space(&pager).unwrap();

        assert_eq!(count1, count2);
        assert_eq!(space1, space2);

        // Test that page references are consistent
        let page_num1 = node.with_page(&pager, |page| page.page_number()).unwrap();
        let page_num2 = node.with_page(&pager, |page| page.page_number()).unwrap();

        assert_eq!(page_num1, page_num2);
        assert_eq!(page_num1, node.page_number);
    }

    #[test]
    fn test_boundary_conditions() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Test accessing cell at boundary indices
        let result = node.get_cell_owned(0, &pager);
        assert!(result.is_err()); // No cells yet

        // Insert one cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 5,
            row_id: 1,
            payload: vec![1, 2, 3, 4, 5],
            overflow_page: None,
        });
        node.insert_cell(cell, &pager).unwrap();

        // Now index 0 should work
        let result = node.get_cell_owned(0, &pager);
        assert!(result.is_ok());

        // But index 1 should fail
        let result = node.get_cell_owned(1, &pager);
        assert!(result.is_err());

        // Test removing cell at boundaries
        let result = node.remove_cell(1, &pager);
        assert!(result.is_err()); // Out of bounds

        let result = node.remove_cell(0, &pager);
        assert!(result.is_ok()); // Should work

        // Now the node should be empty again
        assert_eq!(node.cell_count(&pager).unwrap(), 0);
    }

    #[test]
    fn test_index_node_comprehensive() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::IndexLeaf, &pager).unwrap();

        // Create test data with different data types
        let test_data = [
            (SqliteValue::Integer(10), "ten"),
            (SqliteValue::Integer(20), "twenty"),
            (SqliteValue::String("apple".to_string()), "fruit1"),
            (SqliteValue::String("banana".to_string()), "fruit2"),
            (SqliteValue::Float(3.14), "pi"),
        ];

        // Insert all test data
        for (key_value, _description) in &test_data {
            let mut payload = Vec::new();
            crate::utils::serialization::serialize_values(&[key_value.clone()], &mut payload)
                .unwrap();

            let cell = BTreeCell::IndexLeaf(IndexLeafCell {
                payload_size: payload.len() as u64,
                payload,
                overflow_page: None,
            });

            node.insert_cell_ordered(cell, &pager).unwrap();
        }

        assert_eq!(node.cell_count(&pager).unwrap(), 5);

        // Test finding each key
        for (key_value, description) in &test_data {
            let search_key = match key_value {
                SqliteValue::Integer(i) => KeyValue::Integer(*i),
                SqliteValue::String(s) => KeyValue::String(s.clone()),
                SqliteValue::Float(f) => KeyValue::Float(*f),
                _ => continue,
            };

            let (found, _) = node.find_index_key(&search_key, &pager).unwrap();
            assert!(found, "Should find key for {}", description);
        }

        // Test finding non-existent keys
        let non_existent_keys = [
            KeyValue::Integer(15),
            KeyValue::String("cherry".to_string()),
            KeyValue::Float(2.71),
        ];

        for non_existent_key in &non_existent_keys {
            let (found, _) = node.find_index_key(non_existent_key, &pager).unwrap();
            assert!(
                !found,
                "Should not find non-existent key {:?}",
                non_existent_key
            );
        }
    }

    #[test]
    fn test_node_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let node_page_number;

        // Create and populate node in one scope
        {
            let pager = Pager::create(&db_path, 4096, None, 0).unwrap();
            let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();
            node_page_number = node.page_number;

            // Insert test data
            for i in 1..=10 {
                let cell = BTreeCell::TableLeaf(TableLeafCell {
                    payload_size: 4,
                    row_id: i,
                    payload: vec![i as u8; 4],
                    overflow_page: None,
                });
                node.insert_cell_ordered(cell, &pager).unwrap();
            }

            // Ensure data is written
            pager.flush().unwrap();
        } // pager goes out of scope

        // Reopen and verify data persisted
        {
            let pager = Pager::open(&db_path, None).unwrap();
            let node = BTreeNode::open(node_page_number, PageType::TableLeaf, &pager).unwrap();

            // Verify all data is still there
            assert_eq!(node.cell_count(&pager).unwrap(), 10);

            for i in 1..=10 {
                let (found, _) = node.find_table_rowid(i, &pager).unwrap();
                assert!(found, "Should find persisted row_id {}", i);
            }
        }
    }

    #[test]
    fn test_split_with_different_cell_sizes() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Insert cells with varying payload sizes to test split behavior
        let mut split_occurred = false;

        for i in 1..=50 {
            // Create cells with increasing payload sizes
            let payload_size = i * 10; // 10, 20, 30, ... bytes
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: payload_size as u64,
                row_id: i,
                payload: vec![i as u8; payload_size as usize],
                overflow_page: None,
            });

            let (split, median_key, new_node) = node.insert_cell_ordered(cell, &pager).unwrap();

            if split {
                split_occurred = true;
                let median = median_key.unwrap();
                let new_node = new_node.unwrap();

                // Verify split properties
                assert!(median > 0);
                assert!(node.cell_count(&pager).unwrap() > 0);
                assert!(new_node.cell_count(&pager).unwrap() > 0);

                // Total cells should be preserved
                let total_cells =
                    node.cell_count(&pager).unwrap() + new_node.cell_count(&pager).unwrap();
                assert_eq!(total_cells, (i + 1) as u16);

                // println!("Split occurred with {} cells, median: {}", (i+1), median);
                break;
            }
        }

        assert!(
            split_occurred,
            "Expected a split to occur with varying cell sizes"
        );
    }

    #[test]
    fn test_empty_node_operations() {
        let pager = create_test_pager();
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &pager).unwrap();

        // Test operations on empty node
        assert_eq!(node.cell_count(&pager).unwrap(), 0);
        assert!(node.free_space(&pager).unwrap() > 0);

        // Search operations should handle empty node gracefully
        let (found, idx) = node.find_table_rowid(42, &pager).unwrap();
        assert!(!found);
        assert_eq!(idx, 0);

        // Attempting to access cells should fail appropriately
        let result = node.get_cell_owned(0, &pager);
        assert!(result.is_err());

        let result = node.remove_cell(0, &pager);
        assert!(result.is_err());
    }
}
