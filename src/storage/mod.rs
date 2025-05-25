//! # Storage Module
//!
//! Este módulo proporciona las funcionalidades de almacenamiento para el motor
//! de base de datos SQLite, incluyendo el acceso a disco y la gestión de páginas.

pub mod cache;
pub mod disk;
pub mod pager;

// Re-exportar para facilitar el acceso
pub use cache::BufferPool;
pub use disk::DiskManager;
pub use pager::Pager;
