//! # Storage Module
//! This module provides the storage layer for the database, including buffer management,
//! disk management, and paging. It is responsible for managing how data is stored and retrieved
//! from disk, as well as caching frequently accessed data in memory to improve performance.
pub mod cache;
pub mod disk;
pub mod frames;
pub mod pager;
pub mod wal;

#[cfg(test)]
mod tests;
