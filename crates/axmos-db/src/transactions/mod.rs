//! MVCC Transaction System for AxmosDB
use crate::{
    ObjectId, TRANSACTION_ZERO, UInt64,
    database::{
        SharedCatalog,
        errors::{TransactionError, TransactionResult},
    },
    io::pager::SharedPager,
    types::TransactionId,
};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub mod worker;

pub type Version = u16;
pub type RowId = UInt64;

/// Last commit timestamp per tuple
type TupleCommitLog = Arc<RwLock<HashMap<LogicalId, u64>>>;

/// Logical identifier for a tuple (table + row)
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy)]
pub struct LogicalId(ObjectId, RowId);

impl std::fmt::Display for LogicalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table {}, row {}", self.0, self.1)
    }
}

impl LogicalId {
    pub fn new(table: ObjectId, row: RowId) -> Self {
        Self(table, row)
    }

    pub fn table(&self) -> ObjectId {
        self.0
    }

    pub fn row(&self) -> RowId {
        self.1
    }
}

/// Snapshot of the database state at transaction start time.
/// Used to determine tuple visibility under snapshot isolation.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID that owns this snapshot
    xid: TransactionId,
    /// Lowest active transaction ID when snapshot was taken
    xmin: TransactionId,
    /// Next transaction ID to be assigned when snapshot was taken
    xmax: TransactionId,
    /// Set of transaction IDs that were active (uncommitted) at snapshot time
    active_txs: HashSet<TransactionId>,
}

impl Snapshot {
    pub fn new(
        xid: TransactionId,
        xmin: TransactionId,
        xmax: TransactionId,
        active: HashSet<TransactionId>,
    ) -> Self {
        Self {
            xid,
            xmin,
            xmax,
            active_txs: active,
        }
    }

    /// Returns the transaction ID that owns this snapshot
    pub fn xid(&self) -> TransactionId {
        self.xid
    }

    /// Check if a tuple with given xmin/xmax is visible to this snapshot.
    pub fn is_tuple_visible(&self, tuple_xmin: TransactionId, tuple_xmax: TransactionId) -> bool {
        // Case 1: The tuple was created by our own transaction, then it should be visible to us unless we also deleted it.
        if tuple_xmin == self.xid {
            // Visible unless we also deleted it
            return tuple_xmax != self.xid;
        }

        // Case 2: If the creating transaction had not committed before our snapshot, we should not be able to see the tuple.
        if !self.is_committed_before_snapshot(tuple_xmin) {
            return false;
        }

        // Check if the tuple has been deleted
        if tuple_xmax == TRANSACTION_ZERO {
            // Not deleted
            return true;
        }

        // Deletion is visible only if deleter committed before our snapshot
        // So tuple is visible if deleter has NOT committed before our snapshot
        !self.is_committed_before_snapshot(tuple_xmax)
    }

    /// Check if a transaction was committed before this snapshot was taken
    pub fn is_committed_before_snapshot(&self, txid: TransactionId) -> bool {
        // Transaction started after our snapshot,
        // Then it is impossible it committed before the snapshot was taken
        // Transaction was active when we took snapshot (had not commited)
        if (txid >= self.xmax && self.xmax != TRANSACTION_ZERO) || self.active_txs.contains(&txid) {
            return false;
        }
        // Transaction ID is below our snapshot's xmax and wasn't active
        // This means it must have committed before our snapshot
        true
    }
}

/// State of a transaction in the system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is actively running
    Active,
    /// Transaction is in the process of committing
    Committing,
    /// Transaction has been committed
    Committed,
    /// Transaction has been aborted
    Aborted,
}

/// Metadata for a single transaction
#[derive(Debug)]
pub struct TransactionMetadata {
    id: TransactionId,
    state: TransactionState,
    snapshot: Snapshot,
    /// Tuples read by this transaction
    read_set: HashSet<LogicalId>,
    /// Tuples written by this transaction with their original version
    write_set: HashMap<LogicalId, Version>,
    /// Tracks the timestamp when the transaction actually started.
    start_ts: u64,
}

impl TransactionMetadata {
    pub fn new(id: TransactionId, snapshot: Snapshot, start_ts: u64) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            snapshot,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            start_ts,
        }
    }

    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    pub fn add_to_read_set(&mut self, id: LogicalId) {
        self.read_set.insert(id);
    }

    pub fn add_to_write_set(&mut self, id: LogicalId, version: Version) {
        self.write_set.insert(id, version);
    }

    pub fn write_set(&self) -> &HashMap<LogicalId, Version> {
        &self.write_set
    }
}

/// Shared transaction table protected by RwLock
type SharedTransactionTable = Arc<RwLock<HashMap<TransactionId, TransactionMetadata>>>;

/// Central coordinator for all transactions in the system.
///
///  Creates transactions, snapshots, tracks active transactions, validates commits and maintains commit order.
#[derive(Debug, Clone)]
pub struct TransactionCoordinator {
    /// All active and recently committed transactions
    transactions: SharedTransactionTable,
    /// Shared pager for I/O operations
    pager: SharedPager,
    /// Shared catalog for schema access
    catalog: SharedCatalog,
    /// Last committed transaction to create snapshots
    last_committed: TransactionId,
    /// Global commit timestamp counter
    commit_counter: Arc<AtomicU64>,
    /// Last commit timestamp per tuple - tracks when each tuple was last modified
    tuple_commits: TupleCommitLog,
}

enum ValidationResult {
    Conflict(LogicalId),
    Allowed,
}

impl TransactionCoordinator {
    pub fn new(pager: SharedPager, catalog: SharedCatalog) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            pager,
            catalog,
            last_committed: TRANSACTION_ZERO,
            commit_counter: Arc::new(AtomicU64::new(1)),
            tuple_commits: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a snapshot of the database state for a given transaction id.
    pub fn snapshot(&self, txid: TransactionId) -> TransactionResult<Snapshot> {
        let txs = self.transactions.read();

        // Collect active transaction IDs
        let active: HashSet<TransactionId> = txs
            .iter()
            .filter(|(_, entry)| entry.state == TransactionState::Active)
            .map(|(id, _)| *id)
            .collect();

        // xmin is the smallest active transaction ID (or our ID if none active)
        let xmin = active.iter().min().copied().unwrap_or(txid);

        // xmax is the next transaction ID to be assigned
        let xmax = self.last_committed;

        Ok(Snapshot::new(txid, xmin, xmax, active))
    }

    /// Begin a new transaction.
    /// Returns a [TransactionHandle] to the thread that requested the begin operation.
    pub fn begin(&self) -> TransactionResult<TransactionHandle> {
        let txid = TransactionId::new();

        // Create snapshot under read lock
        let snapshot = self.snapshot(txid)?;
        let start_ts = self.commit_counter.load(Ordering::SeqCst);

        // Insert new transaction entry
        {
            let mut txs = self.transactions.write();

            let entry = TransactionMetadata::new(txid, snapshot.clone(), start_ts);
            txs.insert(txid, entry);
        }

        Ok(TransactionHandle {
            id: txid,
            snapshot,
            coordinator: self.clone(),
        })
    }

    /// Validate and commit a transaction
    ///
    /// This performs optimistic concurrency control validation:
    /// Checks that no tuple in write set was modified by another committed transaction
    /// If validation passes, marks the transaction as committed
    pub fn commit(&self, txid: TransactionId) -> TransactionResult<()> {
        // Perform validation
        match self.validate_write_set(txid)? {
            ValidationResult::Allowed => {
                self.set_transaction_state(txid, TransactionState::Committed)?;
                Ok(())
            }

            ValidationResult::Conflict(id) => {
                self.set_transaction_state(txid, TransactionState::Aborted)?;
                Err(TransactionError::WriteWriteConflict(txid, id))
            }
        }
    }

    /// Abort a transaction
    pub fn abort(&self, txid: TransactionId) -> TransactionResult<()> {
        let mut txs = self.transactions.write();

        let entry = txs.get_mut(&txid).ok_or(TransactionError::NotFound(txid))?;

        entry.state = TransactionState::Aborted;
        Ok(())
    }

    /// Get a transaction's snapshot for visibility checks
    pub fn get_snapshot(&self, txid: TransactionId) -> TransactionResult<Snapshot> {
        let txs = self.transactions.read();

        let entry = txs.get(&txid).ok_or(TransactionError::NotFound(txid))?;

        Ok(entry.snapshot.clone())
    }

    /// Record a read operation for a transaction
    pub fn record_read(&self, txid: TransactionId, logical_id: LogicalId) -> TransactionResult<()> {
        let mut txs = self.transactions.write();

        let entry = txs.get_mut(&txid).ok_or(TransactionError::NotFound(txid))?;

        if entry.state != TransactionState::Active {
            return Err(TransactionError::Other(format!(
                "Transaction {} is not active",
                txid
            )));
        }

        entry.add_to_read_set(logical_id);
        Ok(())
    }

    /// Record a write operation for a transaction
    pub fn record_write(
        &self,
        txid: TransactionId,
        logical_id: LogicalId,
        version: Version,
    ) -> TransactionResult<()> {
        let mut txs = self.transactions.write();

        let entry = txs.get_mut(&txid).ok_or(TransactionError::NotFound(txid))?;

        if entry.state != TransactionState::Active {
            return Err(TransactionError::Other(format!(
                "Transaction {} is not active",
                txid
            )));
        }

        entry.add_to_write_set(logical_id, version);
        Ok(())
    }

    fn set_transaction_state(
        &self,
        txid: TransactionId,
        state: TransactionState,
    ) -> TransactionResult<()> {
        let mut txs = self.transactions.write();
        let entry = txs.get_mut(&txid).ok_or(TransactionError::NotFound(txid))?;

        let valid_transition = matches!(
            (entry.state, state),
            (TransactionState::Active, _)
                | (TransactionState::Committing, TransactionState::Committed)
                | (TransactionState::Committing, TransactionState::Aborted)
        );

        if !valid_transition {
            return Err(TransactionError::Other(format!(
                "Invalid state transition for {}: {:?} → {:?}",
                txid, entry.state, state
            )));
        }

        entry.state = state;
        Ok(())
    }

    /// Validate write set for conflicts
    /// Returns [CommitResult::Allowed] if no conflicts detected
    fn validate_write_set(&self, txid: TransactionId) -> TransactionResult<ValidationResult> {
        self.set_transaction_state(txid, TransactionState::Committing)?;

        let txs = self.transactions.read();
        let entry = txs.get(&txid).ok_or(TransactionError::NotFound(txid))?;

        let start_ts = entry.start_ts();
        let write_set: Vec<LogicalId> = entry.write_set().keys().copied().collect();

        // Check if any tuple we wrote was committed after we started
        {
            let tuple_commits = self.tuple_commits.read();
            for logical_id in &write_set {
                if let Some(&last_commit_ts) = tuple_commits.get(logical_id)
                    && last_commit_ts >= start_ts
                {
                    return Ok(ValidationResult::Conflict(*logical_id));
                }
            }
        }

        // Validation passed, assign commit timestamp and record tuple commits
        let commit_ts = self.commit_counter.fetch_add(1, Ordering::SeqCst);

        {
            let mut tuple_commits = self.tuple_commits.write();
            for logical_id in write_set {
                tuple_commits.insert(logical_id, commit_ts);
            }
        }

        Ok(ValidationResult::Allowed)
    }

    /// Cleanup old entries from tuple_commits (call periodically)
    pub fn cleanup_tuple_commits(&self, min_active_start_ts: u64) {
        let mut tuple_commits = self.tuple_commits.write();
        tuple_commits.retain(|_, &mut commit_ts| commit_ts >= min_active_start_ts);
    }

    /// Clean up old committed/aborted transactions
    /// Should be called periodically by a background vacuum process
    pub fn cleanup_old_transactions(&self, min_active_xid: TransactionId) {
        self.transactions.write().retain(|id, entry| {
            // Keep active transactions
            if entry.state == TransactionState::Active {
                return true;
            }
            // Keep transactions that might still be needed for visibility
            *id >= min_active_xid
        });
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }

    pub fn catalog(&self) -> SharedCatalog {
        self.catalog.clone()
    }
}

/// Handle to an active transaction
/// Provides a safe interface for transaction operations
#[derive(Debug)]
pub struct TransactionHandle {
    id: TransactionId,
    snapshot: Snapshot,
    coordinator: TransactionCoordinator,
}

impl TransactionHandle {
    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Commit this transaction
    pub fn commit(self) -> TransactionResult<()> {
        self.coordinator.commit(self.id)
    }

    /// Abort this transaction
    pub fn abort(self) -> TransactionResult<()> {
        self.coordinator.abort(self.id)
    }
}

#[cfg(test)]
mod coordinator_tests {
    use super::*;
    use crate::{
        IncrementalVaccum, TextEncoding, configs::AxmosDBConfig, database::Database,
        io::pager::Pager,
    };
    use std::{collections::HashSet, io, path::Path};

    /// Creates mock fixtures for testing.
    fn create_db(
        page_size: usize,
        capacity: usize,
        path: impl AsRef<Path>,
    ) -> io::Result<Database> {
        let config = AxmosDBConfig {
            page_size: page_size as u32,
            cache_size: Some(capacity as u16),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path).unwrap();

        Database::new(SharedPager::from(pager), 3, 2)
    }

    /// Test: A transaction can see tuples it created itself.
    ///
    /// Scenario:
    /// - Transaction 10 creates a tuple (xmin=10, xmax=0)
    /// - Transaction 10's snapshot should see this tuple as visible
    #[test]
    fn test_coordinator_1() {
        let xid = TransactionId::from(10u64);
        let snapshot = Snapshot::new(xid, xid, TransactionId::from(11u64), HashSet::new());

        // Tuple created by our transaction, not deleted
        let tuple_xmin = xid;
        let tuple_xmax = TRANSACTION_ZERO;

        assert!(
            snapshot.is_tuple_visible(tuple_xmin, tuple_xmax),
            "Transaction should see its own uncommitted writes"
        );
    }

    /// Test: A transaction cannot see tuples created by active (uncommitted) transactions.
    ///
    /// Scenario:
    /// - Transaction 7 is active (uncommitted) when transaction 10 takes its snapshot
    /// - Transaction 7 creates a tuple (xmin=7, xmax=0)
    /// - Transaction 10 should NOT see this tuple
    #[test]
    fn test_coordinator_2() {
        let xid_10 = TransactionId::from(10u64);
        let xid_7 = TransactionId::from(7u64);

        let mut active = HashSet::new();
        active.insert(xid_7);

        let snapshot = Snapshot::new(
            xid_10,
            TransactionId::from(5u64),
            TransactionId::from(11u64),
            active,
        );

        // Tuple created by active transaction 7
        let tuple_xmin = xid_7;
        let tuple_xmax = TRANSACTION_ZERO;

        assert!(
            !snapshot.is_tuple_visible(tuple_xmin, tuple_xmax),
            "Transaction should NOT see writes from active (uncommitted) transactions"
        );
    }

    /// Test: A transaction can see tuples from transactions that committed before the snapshot.
    ///
    /// Scenario:
    /// - Transaction 5 committed before transaction 10 started
    /// - Transaction 5 created a tuple (xmin=5, xmax=0)
    /// - Transaction 10 should see this tuple
    #[test]
    fn test_coordinator_3() {
        let xid_10 = TransactionId::from(10u64);
        let xid_5 = TransactionId::from(5u64);

        let snapshot = Snapshot::new(
            xid_10,
            xid_5,
            TransactionId::from(11u64),
            HashSet::new(), // No active transactions - xid_5 already committed
        );

        // Tuple created by committed transaction 5
        let tuple_xmin = xid_5;
        let tuple_xmax = TRANSACTION_ZERO;

        assert!(
            snapshot.is_tuple_visible(tuple_xmin, tuple_xmax),
            "Transaction should see writes from committed transactions"
        );
    }

    /// Test: A deleted tuple is not visible if the deletion was committed before the snapshot.
    ///
    /// Scenario:
    /// - Transaction 3 created a tuple
    /// - Transaction 5 deleted the tuple (both committed before snapshot)
    /// - Transaction 10 should NOT see the tuple
    #[test]
    fn test_coordinator_4() {
        let xid_10 = TransactionId::from(10u64);
        let xid_3 = TransactionId::from(3u64);
        let xid_5 = TransactionId::from(5u64);

        let snapshot = Snapshot::new(xid_10, xid_3, TransactionId::from(11u64), HashSet::new());

        // Tuple created by tx3, deleted by tx5 (both committed)
        let tuple_xmin = xid_3;
        let tuple_xmax = xid_5;

        assert!(
            !snapshot.is_tuple_visible(tuple_xmin, tuple_xmax),
            "Transaction should NOT see tuples deleted before its snapshot"
        );
    }

    /// Test: A deleted tuple IS visible if the deleting transaction is still active.
    ///
    /// Scenario:
    /// - Transaction 3 created a tuple (committed)
    /// - Transaction 7 deleted the tuple but hasn't committed yet
    /// - Transaction 10 should still see the tuple
    #[test]
    fn test_coordinator_5() {
        let xid_10 = TransactionId::from(10u64);
        let xid_3 = TransactionId::from(3u64);
        let xid_7 = TransactionId::from(7u64);

        let mut active = HashSet::new();
        active.insert(xid_7);

        let snapshot = Snapshot::new(xid_10, xid_3, TransactionId::from(11u64), active);

        // Tuple created by tx3 (committed), deleted by tx7 (active)
        let tuple_xmin = xid_3;
        let tuple_xmax = xid_7;

        assert!(
            snapshot.is_tuple_visible(tuple_xmin, tuple_xmax),
            "Transaction should see tuples with uncommitted deletes"
        );
    }

    /// Test: First-committer-wins - concurrent transactions writing to the same tuple.
    ///
    /// Scenario:
    /// 1. Transaction A starts (start_ts=1)
    /// 2. Transaction B starts (start_ts=1)
    /// 3. Both write to tuple X
    /// 4. Transaction A commits first -> succeeds, records commit_ts=2 for tuple X
    /// 5. Transaction B tries to commit -> fails because tuple X was modified after B started
    #[test]
    fn test_coordinator_6() -> io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 100, &path)?;
        let pager = db.pager();
        let catalog = db.catalog();
        let coordinator = TransactionCoordinator::new(pager, catalog);

        let tuple_id = LogicalId::new(ObjectId::from(1u64), UInt64::from(1u64));

        // Step 1 & 2: Both transactions start
        let handle_a = coordinator.begin().expect("Failed to begin transaction A");
        let txid_a = handle_a.id();

        let handle_b = coordinator.begin().expect("Failed to begin transaction B");
        let txid_b = handle_b.id();

        // Step 3: Both write to the same tuple
        coordinator
            .record_write(txid_a, tuple_id, 0)
            .expect("Failed to record write for A");
        coordinator
            .record_write(txid_b, tuple_id, 0)
            .expect("Failed to record write for B");

        // Step 4: Transaction A commits first - should succeed
        let result_a = handle_a.commit();
        assert!(
            result_a.is_ok(),
            "First committer should succeed, got: {:?}",
            result_a
        );

        // Step 5: Transaction B tries to commit - should fail with conflict
        let result_b = handle_b.commit();
        assert!(
            matches!(result_b, Err(TransactionError::WriteWriteConflict(_, _))),
            "Second committer should fail with WriteWriteConflict, got: {:?}",
            result_b
        );
        Ok(())
    }

    /// Test: No conflict when transactions write to different tuples.
    ///
    /// Scenario:
    /// 1. Transaction A writes to tuple X
    /// 2. Transaction B writes to tuple Y
    /// 3. Both commit successfully (no conflict)
    #[test]
    fn test_coordinator_7() -> io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 100, &path)?;
        let pager = db.pager();
        let catalog = db.catalog();

        let coordinator = TransactionCoordinator::new(pager, catalog);

        let tuple_x = LogicalId::new(ObjectId::from(1u64), UInt64::from(1u64));
        let tuple_y = LogicalId::new(ObjectId::from(1u64), UInt64::from(2u64));

        let handle_a = coordinator.begin().expect("Failed to begin transaction A");
        let txid_a = handle_a.id();

        let handle_b = coordinator.begin().expect("Failed to begin transaction B");
        let txid_b = handle_b.id();

        // Write to different tuples
        coordinator
            .record_write(txid_a, tuple_x, 0)
            .expect("Failed to record write for A");
        coordinator
            .record_write(txid_b, tuple_y, 0)
            .expect("Failed to record write for B");

        // Both should commit successfully
        assert!(
            handle_a.commit().is_ok(),
            "Transaction A should commit successfully"
        );
        assert!(
            handle_b.commit().is_ok(),
            "Transaction B should commit successfully"
        );
        Ok(())
    }

    /// Test: Serial transactions don't conflict even on the same tuple.
    ///
    /// Scenario:
    /// 1. Transaction A writes to tuple X and commits
    /// 2. Transaction B starts AFTER A committed
    /// 3. Transaction B writes to tuple X and commits successfully
    ///
    /// This works because B's start_ts is after A's commit_ts.
    #[test]
    fn test_coordinator_8() -> io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 100, &path)?;
        let pager = db.pager();
        let catalog = db.catalog();
        let coordinator = TransactionCoordinator::new(pager, catalog);

        let tuple_x = LogicalId::new(ObjectId::from(1u64), UInt64::from(1u64));

        // Transaction A: write and commit
        let handle_a = coordinator.begin().expect("Failed to begin transaction A");
        let txid_a = handle_a.id();
        coordinator
            .record_write(txid_a, tuple_x, 0)
            .expect("Failed to record write for A");
        handle_a.commit().expect("Transaction A should commit");

        // Transaction B: starts after A committed
        let handle_b = coordinator.begin().expect("Failed to begin transaction B");
        let txid_b = handle_b.id();
        coordinator
            .record_write(txid_b, tuple_x, 1)
            .expect("Failed to record write for B");

        // B should succeed - A's commit happened before B started
        assert!(
            handle_b.commit().is_ok(),
            "Transaction B should commit successfully after A finished"
        );

        Ok(())
    }

    /// Test: Aborted transactions release their resources properly.
    ///
    /// Scenario:
    /// 1. Transaction A writes to tuple X and aborts
    /// 2. Transaction B writes to tuple X and commits successfully
    ///
    /// Aborted transactions should not cause conflicts.
    #[test]
    fn test_coordinator_9() -> io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 100, &path)?;
        let pager = db.pager();
        let catalog = db.catalog();
        let coordinator = TransactionCoordinator::new(pager, catalog);

        let tuple_x = LogicalId::new(ObjectId::from(1u64), UInt64::from(1u64));

        // Transaction A: write and abort
        let handle_a = coordinator.begin().expect("Failed to begin transaction A");
        let txid_a = handle_a.id();
        coordinator
            .record_write(txid_a, tuple_x, 0)
            .expect("Failed to record write for A");
        handle_a.abort().expect("Transaction A should abort");

        // Transaction B: write to same tuple
        let handle_b = coordinator.begin().expect("Failed to begin transaction B");
        let txid_b = handle_b.id();
        coordinator
            .record_write(txid_b, tuple_x, 0)
            .expect("Failed to record write for B");

        // B should succeed - A was aborted, not committed
        assert!(
            handle_b.commit().is_ok(),
            "Transaction B should commit after A aborted"
        );

        Ok(())
    }
}
