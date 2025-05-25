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

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (version 1.65+ recommended)
- Cargo (comes with Rust)

### Building the Engine

```bash
git clone https://github.com/RaulMoldes/rqlite.git
cd rqlite
cargo build --release

```
### Running tests

```bash
cargo test -- --test-threads=1
```

## Structure:
```bash
rqlite/
│
├── src/
│   ├── tree/             # B+ tree implementation
│   ├── storage/       # All the low level abstractions for interacting with disk and memory
│   ├── utils/ # Mainly serialization and compression utilities
│   ├── page.rs # Page model and page types
│   ├── header.rs      # Page header model
│   └── lib.rs             # Engine entry point
│
├── tests/                 # Integration tests
├── examples/               # Some examples of its use.
└── README.md              # This file
```

