//! # Utils Module
//!
//! Este módulo contiene utilidades generales que se utilizan en toda la implementación
//! del motor de almacenamiento SQLite.

pub mod cmp;
pub mod serialization;
pub mod varint;

// Re-exportar para facilitar el acceso
pub use varint::{decode_varint, encode_varint, varint_size, MAX_VARINT_SIZE};
