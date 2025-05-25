//! # B-Tree Module
//!
//! This module implements a B-Tree data structure, which is used for indexing and storing
//! data in a way that allows for efficient searching, insertion, and deletion operations.

pub mod btree;
pub mod cell;
pub mod node;
pub mod record;

// Re-export the necessary components for external use
pub use btree::{BTree, TreeType};
pub use cell::BTreeCellFactory;
pub use node::BTreeNode;
pub use record::Record;
