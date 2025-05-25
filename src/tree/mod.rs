//! # B-Tree Module
//!
//! Este módulo implementa las estructuras y operaciones relacionadas con los árboles B-Tree
//! utilizados en SQLite para almacenar tablas e índices.

pub mod btree;
pub mod cell;
pub mod node;
pub mod record;

// Re-exportar para facilitar el acceso
pub use btree::{BTree, TreeType};
pub use cell::BTreeCellFactory;
pub use node::BTreeNode;
pub use record::Record;
