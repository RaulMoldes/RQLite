# RQLiteDB

**RQlite** is a lightweight, SQLite-inspired storage engine written entirely in Rust. Designed for **educational purposes**, this project implements the backend components of a relational database system, focusing on efficiency, memory safety, and extensibility.

The primary goal was to learn  the internals of relational databases by implementing them from scratch. Instead of building a toy example, the engine tries to stay as close to real-world systems as possible while keeping the codebase understandable.

> Note: This is not a full-featured SQL database. It includes only the core storage engine components and is not intended for production use.

---

## Features

- **B+ Tree Indexing**
  - Supports both **primary** and **secondary** indexes
  - **Index-organized storage** for efficient lookups

- **Transaction Management**
  - Basic concurrency control mechanisms
  - Transaction support via extensible journaling system (planned)
  - Built with **future ACID compliance** in mind

- **Memory Management**
  - **Buffer pool** with **LRU eviction** strategy for effective memory use
  - Page-based storage system modeled after SQLite

- **Compression** and **Dynamic typing**
  - Similar to what SQlite does, I am using a VLE compression mechanism for integers.
  - Additionally, to make it SQlite compatible, I implemented a dynamic type system base on type affinity similar to that of SQLite.

- **Concurrency**
  - Early-stage support for **concurrent access**
  - Designed for **thread-safe** operations

- **Pager & Disk Management**
  - Implements a low-level pager layer for page-level I/O
  - Binary format loosely based on SQLite’s design

- **Built with Rust**
  - Ensures **memory safety**
  - Leverages Rust's strong type system and ownership model for correctness

---

This project is heavily inspired by the architecture and design principles of [SQLite](https://www.sqlite.org/index.html), especially the detailed and well-documented [SQLite File Format and Internals](https://www.sqlite.org/fileformat2.html).

Additionally, along the implementation I followed [CMU Database Systems Course](https://15445.courses.cs.cmu.edu/) (15-445/645) provided deep insights into database architecture, concurrency control, and storage engine design. Huge thanks to the professors and contributors to that course.

---

# Getting Started

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (version 1.65+ recommended)
- Cargo (comes with Rust)

## Building the Engine

```bash
git clone https://github.com/RaulMoldes/RQLite.git
cd RQLite
cargo build --release
```
## Running tests

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test module
cargo test storage::tests
```

**Note**: Some of the tests rely on temp files I/O and are currently using all the same file. I recommend using the flag ```--test-threads=1```  when running ```cargo test``` to avoid the testing functions from racing when accessing the same  files.


## Example of usage:

You can run examples with: 

```bash
cargo run example <example-name>
```

Anyway, a an example of how to use RQLite API from an external crate is shown here:

```rust
use rqlite_engine::storage::pager::Pager;
use rqlite_engine::tree::btree::{BTree, TreeType};
use rqlite_engine::tree::record::Record;
use rqlite_engine::utils::serialization::SqliteValue;

// Create a new database
let pager = Pager::create("example.db", 4096, None, 0)?;

// Create a table B-tree
let mut btree = BTree::create(
    TreeType::Table,
    pager,
    4096,
    0,
    255, // max payload fraction
    32,  // min payload fraction
)?;

// Create and insert a record
let record = Record::with_values(vec![
    SqliteValue::Integer(1),
    SqliteValue::String("Hello, RQLite!".to_string()),
    SqliteValue::Float(3.14159),
]);

btree.insert(1, &record)?;

// Find the record
if let Some(found_record) = btree.find(1)? {
    println!("Found record with {} values", found_record.len());
}
```

# System architecture overview:

```bash
┌─────────────────┐
│   B-Tree API    │  ← High-level tree operations
├─────────────────┤
│   Tree Nodes    │  ← Node management & operations
├─────────────────┤
│   Cell Factory  │  ← Cell creation & payload handling
├─────────────────┤
│     Pager       │  ← Page-level operations & caching
├─────────────────┤
│  Buffer Pool    │  ← LRU cache management
├─────────────────┤
│ Disk Manager    │  ← Raw file I/O operations
└─────────────────┘
```

# Folder structure:

```bash
src/
├── lib.rs                    # Main library entry point
├── header.rs                 # Database header management
├── page.rs                   # Page structures and serialization
├── storage/
│   ├── mod.rs               # Storage module exports
│   ├── disk.rs              # Low-level disk I/O operations
│   ├── pager.rs             # Page management and caching
│   └── cache.rs             # Buffer pool implementation
├── tree/
│   ├── mod.rs               # Tree module exports
│   ├── btree.rs             # High-level B-tree operations
│   ├── node.rs              # Node management and algorithms
│   ├── cell.rs              # Cell factory and creation logic
│   └── record.rs            # Record serialization/deserialization
└── utils/
    ├── mod.rs               # Utilities module exports
    ├── varint.rs            # Variable-length integer compression
    ├── serialization.rs     # SQLite value serialization
    └── cmp.rs               # Key comparison utilities
```

# Contributions, future enhancements and current issues:

There is a list of the current identified issues at the header of the ```tests/integration_tests.rs``` file.
Mainly, we need to avoid creating a full copy of the pager for each table, which makes the engine quite inefficient.

Apart from that, future improvements include:

* A more robust catalog. Currently, the catalog is implemented as a Hashmap on the RQLite struct (see ```lib.rs```). A more robust implementation could aid to maintain state of the database after power losses.

* Improve the journaling system to make the system more ACID. Possibly use a journal file instead of keeping the journal pages in memory. See ```pager.rs``` for details.

* Add WAL logging. You can look at this lecture to learn on how to implement: https://www.youtube.com/watch?v=-mY8ALcJV


