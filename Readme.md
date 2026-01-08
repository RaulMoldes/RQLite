# AxmosDB

AxmosDB is an experimental relational database server written in Rust, designed to explore modern database internals with a clean, modular architecture.

Unlike other toylike database projects, AzmosDB was built from the beginning with the purpose of becoming a full y funcional, productive database system. This forced me to explore the intrinsics of modern database systems in depth in order to take the BEST of each one.

# Architecture

## Index Organized Storage Engine.

Initially, I was going to built AxmosDB as an exact port of SQLite to Rust. Luckily, I soon realized that this was shooting myself in the foot. There is already a project called Turso which aims to occupy that space. Doing that would make the project just become another rewrite of SQLite in Rust, which is something the community has already seen and is already tired of.

Anyway, I think there are many good things on the design principales covered by SQLite developers, and I have used the documentation a lot in order to define mine.

The most clear of them all is the single file approach and the Pager and the Index Organized Storage Architecture. In SQLite and AxmosDB every table is stored as a B+tree of slotted pages. InnoDB (MySQL) also uses this approach. This is nice because you are able to reuse the same data structures you create for the tables as for the database indexes. The Pager IS the msin storage manager that handles Page allocation, deallocation, caching and disk maintenance.

In my storage engine implementation, the Pager is also responsable to handles write ahead logging. There is a component in the Pager, called the WriteAheadLog, where writes to tables are written before writing then to the msin database file. This allows us to recoger the database from a possible crash. My WAL recovery implementation follows the three phase ARIES protocol (analysis, undo and redo). Each time the database is opened, It will look on the directory if there is a wal file. If there is one, the recovery process will be run, redoing everything that needs to be redone and undoing every change that needs to be undone. Once the recovery finished, the wal is truncated to zero length.

## Page-Level latching

On databases there is a distinción between database object locking (Table Locks, row Locks) and latching. Latching is what database people call what OS developers call locks. 

As my database is multitenant, It must support multiple threads accessing the data structures that support it at the same time. I am using Parking Lot's lock api which provides a handful implementation of several types of locks. Parking lot uses a single byte to store the mutex, and causes generally less contention than standard OS futexes (std::sync::Mutex) which makes it more suitable for this implementation.

There is also a lot to think on how to acquire this locks when traversing btrees, as when you modify a page, you might need to rebalance the whole structure (which means modifying other pages) ,and you do not want to come to the situation where someone already has the lock on a page you need to modify in order to complete the rebalance operation.

The solution is to differentiate between tree access modes. When accessing a tree to read, you can just free the lock on a node once you have acquired the lock on its child. When accessing on write mode, you cannot realase any lock until you know you have finished your operation on the tree.


## ACID compliant transactions.

The transactions coordinator component in the database is the main manager for the MVCC systems. In Axmos DB, tuples (rows) are stored with an special layout. 

```text
[Tuple header][null bitmap][Tuple Keys][Tuple Values][Version n][Delta n][Version n - 1][Delta n - 1]...
```

The deltas incluye the changes (diffs) required to recovery a specific version of the tuple in case it is necessary. This differs to how postgres implements MVCC since on postgres the whole data of the versiones Tuple is copied to a new location, which is inefficient in my opinion.

With each delta, we also store the transactions who created it (xmin), and in the tuple header we store the transactions deleting the tuple (xmax) if applicable.

When creating a transaction, the coordinator creates a snapshot of the database status at that specific point in time, and that metadata is used by the created transactionnto access the specific versions of each tuple it is allowed to read.

To prevent tuples becoming too large (as we never delete them) there is a vacuum process which runs every now and then in order to clean everything up.

## Query execution pipeline

My goal was to develop an entire database from scratch, so I have also developed the parser myself. My SQL parser follows a Pratt Parsing approach which makes it easy and fun to create parsers.

After parser, a small simplification is executed over the first parsed ast to simplify complex expressions before the execution (for example SELECT 1+1 is simplified into SELECT 2).

After that the binder runs. The binder Is responsible of translating the AST produces by the parser into a Bound AST in which table indexes and column ids are resolved.

After the binder, DDL statements are executed directly. DML and SELECT statements go one step further (through the query optimizer). I have implemented an extensible optimizer following a cascades approach (like SQL Servers). I drawn inspiration from a paper by Microsoft Research: Building extensible query optimizers, which I recommend if you are not familiar with this type of systems. The optimizer uses table statistics (computed via ANALYZE) to generate the most optimal execution plan.

Finally, the plan is executed. My Executor trait follows a Volcano style iterator model Executor with three main methods (open, next and close). each Executor calls its children in the pipeline producing one row at a time until the last executor is completely exhausted.

Note that although most basic operators are implemented, there is still a missing implementation for:

- MergeJoin
- ExternalSort
- HashJoin

After that the results are returned to the client and pretty-printed.

