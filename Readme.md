# AxmosDB

AxmosDB is an experimental relational database server written in Rust, designed to explore modern database internals with a clean, modular architecture. Unlike other toy-like database projects, AxmosDB was built from the beginning with the purpose of becoming a fully functional, productive database system. This goal required exploring the intrinsics of modern database systems in depth in order to take the best of each one.

## Quick Start

First, start the server specifying the port to listen on:

**Note**: The safe-drop feature flushes the database contents to disk when it is dropped. It is safer to compile with this feature on.

```bash
cargo run --features safe-drop --bin axmos-server -- -p 5432
```

Then connect to it with the client:

```bash
cargo run --bin axmos-client -- -p 5432
```

Once connected, create a new database using the `CREATEDB` command:

```bash
CREATEDB mydb.axm
```

This creates the main database file (`mydb.axm`) and the write-ahead log file (`axmos.log`). You can also open an existing database file with the `OPENDB` command:

```bash
OPENDB mydb.axm
```

After that you can start running SQL queries. To work with transactions, open a new session with atomic guarantees using `BEGIN`, and close it with either `ROLLBACK` or `COMMIT`.

### Available Commands

The following administrative commands are available through the client:

```text
EXPLAIN <query>      Show the execution plan as a tree
ANALYZE              Recompute table statistics for the whole database
VACUUM               Clean up tuple versions that are no longer needed, recovering disk space
QUIT                 Close the connection
SHUTDOWN             Flush all pending changes to disk and stop the server
```

---

## Architecture Overview

The following diagram illustrates how the main components of AxmosDB interact with each other:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Connection                              │
│                                (TCP Protocol)                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Query Execution Pipeline                         │
│  ┌──────────┐   ┌───────────┐   ┌────────┐   ┌───────────┐   ┌──────────┐  │
│  │  Parser  │ → │ Simplifier│ → │ Binder │ → │ Optimizer │ → │ Executor │  │
│  │  (Pratt) │   │           │   │        │   │ (Cascades)│   │ (Volcano)│  │
│  └──────────┘   └───────────┘   └────────┘   └───────────┘   └──────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Transaction Coordinator                            │
│                   (MVCC with Snapshot Isolation, OCC)                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               Storage Engine                                │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     B+Tree Index-Organized Tables                     │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│  ┌─────────────────────────────┐    ┌───────────────────────────────────┐  │
│  │           Pager             │    │        Write-Ahead Log (WAL)      │  │
│  │  (Page Cache, Allocation,   │    │       (ARIES Recovery Protocol)   │  │
│  │   Direct I/O Management)    │    │                                   │  │
│  └─────────────────────────────┘    └───────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Single Database File                             │
│                                 (*.axm)                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Index-Organized Storage Engine

Initially, AxmosDB was going to be an exact port of SQLite to Rust. However, it soon became clear that this approach would be counterproductive since there is already a project called Turso that occupies that space. Going down that path would have made the project just another rewrite of SQLite in Rust, which is something the community has already seen and is already tired of.

That said, many of the design principles covered by SQLite developers are excellent, and I have used their documentation extensively to inform my own design. The most important of these is the single-file approach combined with the Pager and the Index-Organized Storage Architecture.

In SQLite and AxmosDB, every table is stored as a B+tree of slotted pages. InnoDB (MySQL) also uses this approach. This design is particularly elegant because it allows reusing the same data structures for both tables and indexes. The Pager serves as the main storage manager, handling page allocation, deallocation, caching, and disk maintenance.

In my storage engine implementation, the Pager is also responsible for handling write-ahead logging. There is a component in the Pager called the WriteAheadLog where writes to tables are recorded before being written to the main database file. This allows the database to recover from a possible crash.

My WAL recovery implementation follows the three-phase ARIES protocol: analysis, undo, and redo. Each time the database is opened, it checks the directory for a WAL file. If one exists, the recovery process runs, redoing everything that needs to be redone and undoing every change that needs to be undone. Once recovery finishes, the WAL is truncated to zero length.

---

## Page-Level Latching

In databases, there is a distinction between database object locking (table locks, row locks) and latching. Latching is what database people call what OS developers call locks.

Since AxmosDB is multitenant, it must support multiple threads accessing its data structures at the same time. The implementation uses Parking Lot's lock API, which provides a handful of lock type implementations. Parking Lot uses a single byte to store the mutex and causes generally less contention than standard OS futexes (`std::sync::Mutex`), which makes it more suitable for this implementation.

There is also much to consider regarding how to acquire these locks when traversing B+trees. When you modify a page, you might need to rebalance the whole structure (which means modifying other pages), and you do not want to end up in a situation where someone already has the lock on a page you need to modify to complete the rebalancing operation.

The solution is to differentiate between tree access modes. When accessing a tree to read, you can free the lock on a node once you have acquired the lock on its child. When accessing in write mode, you cannot release any lock until you know you have finished your operation on the tree.

---

## OS Concerns: Direct I/O

There are basically two approaches to developing a database storage system.

Option A is to rely on the OS cache, as PostgreSQL used to do (although they are moving away from it). Option B is to force all writes to disk using Direct I/O. This is preferred since you want to make sure every single bit of information is on disk once you tell the client that is the case.

On Linux systems, Direct I/O can be achieved via the `O_DIRECT` flag, which must be set when the file is opened. `O_DIRECT` forces all memory blocks to be aligned to the filesystem block size (both size and address), which is one of the main reasons why fixed-size pages are used in databases.

---

## ACID-Compliant Transactions

The Transaction Coordinator component in the database is the main manager for the MVCC system. In AxmosDB, tuples (rows) are stored with a special layout that supports multiple versions:

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                              Tuple Structure                               │
├────────────┬─────────────┬────────────┬──────────────┬───────────────────┬─┤
│   Tuple    │    Null     │   Tuple    │    Tuple     │  Version Chain    │ │
│   Header   │   Bitmap    │    Keys    │    Values    │   (Delta Log)     │ │
├────────────┴─────────────┴────────────┴──────────────┴───────────────────┴─┤
│                                                                            │
│  Version Chain Detail:                                                     │
│  ┌──────────┬─────────┐  ┌──────────┬─────────┐  ┌──────────┬─────────┐   │
│  │ Version  │ Delta   │  │ Version  │ Delta   │  │ Version  │ Delta   │   │
│  │   n      │   n     │→ │  n - 1   │  n - 1  │→ │  n - 2   │  n - 2  │   │
│  │ (xmin)   │ (diff)  │  │ (xmin)   │ (diff)  │  │ (xmin)   │ (diff)  │   │
│  └──────────┴─────────┘  └──────────┴─────────┘  └──────────┴─────────┘   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

The deltas include the changes (diffs) required to recover a specific version of the tuple in case it is necessary. This differs from how PostgreSQL implements MVCC, where the whole data of the tuple is copied to a new location, which in my opinion is inefficient.

With each delta, we also store the transaction that created it (`xmin`), and in the tuple header we store the transaction deleting the tuple (`xmax`) if applicable. When creating a transaction, the coordinator creates a snapshot of the database status at that specific point in time, and that metadata is used by the transaction to access the specific versions of each tuple it is allowed to read.

To prevent tuples from becoming too large (since we never delete versions), there is a vacuum process that runs periodically to clean up old versions that are no longer needed by any active transaction.

---

## Query Execution Pipeline

My goal was to develop an entire database from scratch, so I have also developed the parser myself. The SQL parser follows a Pratt Parsing approach, which makes it easy and enjoyable to create parsers.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Query Execution Pipeline                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │    SQL Query    │
                              │   "SELECT ..."  │
                              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │     Parser      │
                              │  (Pratt-based)  │
                              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │   Simplifier    │
                              │ (SELECT 1+1 →   │
                              │   SELECT 2)     │
                              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │     Binder      │
                              │ (Resolve table  │
                              │  & column IDs)  │
                              └────────┬────────┘
                                       │
                        ┌──────────────┴──────────────┐
                        │                             │
                        ▼                             ▼
               ┌────────────────┐            ┌────────────────┐
               │  DDL Statement │            │ DML / SELECT   │
               │ (Execute Now)  │            │   Statement    │
               └────────────────┘            └───────┬────────┘
                                                     │
                                                     ▼
                                            ┌────────────────┐
                                            │   Optimizer    │
                                            │  (Cascades)    │
                                            │ Uses ANALYZE   │
                                            │   statistics   │
                                            └───────┬────────┘
                                                     │
                                                     ▼
                                            ┌────────────────┐
                                            │   Executor     │
                                            │   (Volcano)    │
                                            │ open/next/close│
                                            └───────┬────────┘
                                                     │
                                                     ▼
                                            ┌────────────────┐
                                            │    Results     │
                                            └────────────────┘
```

After parsing, a small simplification is executed over the initial AST to simplify complex expressions before execution. For example, `SELECT 1+1` is simplified into `SELECT 2`.

After that, the binder runs. The binder is responsible for translating the AST produced by the parser into a Bound AST in which table indexes and column IDs are resolved. After the binder, DDL statements are executed directly, while DML and SELECT statements go one step further through the query optimizer.

I have implemented an extensible optimizer following a Cascades approach, similar to SQL Server. I drew inspiration from a paper by Microsoft Research titled "Building Extensible Query Optimizers," which I recommend if you are not familiar with this type of system. The optimizer uses table statistics computed via `ANALYZE` to generate the most optimal execution plan.

Finally, the plan is executed. The Executor trait follows a Volcano-style iterator model with three main methods: `open`, `next`, and `close`. Each executor calls its children in the pipeline, producing one row at a time until the last executor is completely exhausted.

Note that although most basic operators are implemented, the following are still missing: MergeJoin, ExternalSort, and HashJoin.

---

## TCP Protocol

AxmosDB communicates with clients over a custom TCP protocol. The server listens on a configurable port (default 5432) and handles requests for database operations, SQL queries, and administrative commands.

The protocol supports the following request types: database creation and opening, SQL execution, transaction control (BEGIN, COMMIT, ROLLBACK), query explanation, statistics analysis, vacuum operations, and server management.

---

## Acknowledgements

I would not have been able to develop this project without the CMU courses on database systems.

I have also used quite a lot of SQLite's, PostgreSQL's, and InnoDB's documentation pages, as well as the database development community on Reddit.

---

## License

Apache-2.0

## Repository

[https://github.com/RaulMoldes/AxmosDB](https://github.com/RaulMoldes/AxmosDB)
