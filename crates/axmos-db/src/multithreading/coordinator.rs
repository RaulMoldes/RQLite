//! MVCC Transaction System for AxmosDB
use crate::{
    common::errors::{TransactionError, TransactionResult},
    io::pager::SharedPager,
    types::{LogicalId, TransactionId},
};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub type Version = u16;

/// Last commit timestamp per tuple
type TupleCommitLog = Arc<RwLock<HashMap<LogicalId, u64>>>;

// Macro for creating snapshots with low boilerplate.
#[macro_export]
macro_rules! snapshot {
    (xid: $xid:expr, xmin: $xmin:expr, xmax: $xmax:expr) => {{
        $crate::transactions::Snapshot::new(
            $crate::types::TransactionId::from($xid as u64),
            $crate::types::TransactionId::from($xmin as u64),
            $crate::types::TransactionId::from($xmax as u64),
            std::collections::HashSet::new(),
        )
    }};

    (xid: $xid:expr, xmin: $xmin:expr, xmax: $xmax:expr, active: [$($active:expr),* $(,)?]) => {{
        let mut active_set = std::collections::HashSet::new();
        $(active_set.insert($crate::types::TransactionId::from($active as u64));)*
        $crate::transactions::Snapshot::new(
            $crate::types::TransactionId::from($xid as u64),
            $crate::types::TransactionId::from($xmin as u64),
            $crate::types::TransactionId::from($xmax as u64),
            active_set,
        )
    }};
}

/// Snapshot of the database state at transaction start time.
/// Used to determine tuple visibility under snapshot isolation.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID that owns this snapshot
    xid: TransactionId,
    /// Lowest active transaction ID when snapshot was taken
    xmin: TransactionId,
    /// Last committed transaction when snapshot was taken (Set to None for the first transaction in the system)
    xmax: Option<TransactionId>,
    /// Set of transaction IDs that were active (uncommitted) at snapshot time
    active_txs: HashSet<TransactionId>,
}

impl Snapshot {
    pub fn new(
        xid: TransactionId,
        xmin: TransactionId,
        xmax: Option<TransactionId>,
        active: HashSet<TransactionId>,
    ) -> Self {
        Self {
            xid,
            xmin,
            xmax,
            active_txs: active,
        }
    }
    #[inline]
    pub fn xmin(&self) -> TransactionId {
        self.xmin
    }

    #[inline]
    pub fn xmax(&self) -> Option<TransactionId> {
        self.xmax
    }

    /// Returns the transaction ID that owns this snapshot
    #[inline]
    pub fn xid(&self) -> TransactionId {
        self.xid
    }

    /// Check if a tuple with given xmin/xmax is visible to this snapshot.
    pub fn is_tuple_visible(
        &self,
        tuple_xmin: TransactionId,
        tuple_xmax: Option<TransactionId>,
    ) -> bool {
        // Case 1: The tuple was created by our own transaction, then it should be visible to us unless we also deleted it.
        if tuple_xmin == self.xid
            && let Some(xmax) = tuple_xmax
        {
            // Visible unless we also deleted it
            return xmax != self.xid;
        }

        // Case 2: If the creating transaction had not committed before our snapshot, we should not be able to see the tuple.
        if !self.is_committed_before_snapshot(tuple_xmin) {
            return false;
        }

        // Check if the tuple has been deleted
        if let Some(deleted_tx) = tuple_xmax {
            // if there is a deleting transaction check if the deletion is visible to us.
            // Deletion is visible only if deleter committed before our snapshot
            // So tuple is visible if deleter has NOT committed before our snapshot
            return !self.is_committed_before_snapshot(deleted_tx);
        };

        true
    }

    /// Check if a transaction was committed before this snapshot was taken
    pub fn is_committed_before_snapshot(&self, txid: TransactionId) -> bool {
        // Transaction started after our snapshot,
        // Then it is impossible it committed before the snapshot was taken
        if let Some(max_tx) = self.xmax
            && max_tx <= txid
        {
            return false;
        };

        // Transaction was active when we took snapshot (had not commited)
        if self.active_txs.contains(&txid) {
            return false;
        };

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
    /// Transaction ID
    id: TransactionId,
    /// Transaction state
    state: TransactionState,
    /// Snapshot of the transaction coordinator at the time the transaction was created.
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

impl Clone for TransactionCoordinator {
    fn clone(&self) -> Self {
        Self {
            transactions: Arc::clone(&self.transactions),
            pager: self.pager.clone(),
            last_committed: self.last_committed,
            commit_counter: Arc::clone(&self.commit_counter),
            tuple_commits: Arc::clone(&self.tuple_commits),
            last_created_transaction: AtomicU64::new(
                self.last_created_transaction.load(Ordering::Relaxed),
            ),
        }
    }
}
/// Central coordinator for all transactions in the system.
///
///  Creates transactions, snapshots, tracks active transactions, validates commits and maintains commit order.
pub struct TransactionCoordinator {
    /// All active and recently committed transactions
    transactions: SharedTransactionTable,
    /// Shared pager for I/O operations
    pager: SharedPager,
    /// Last committed transaction to create snapshots
    last_committed: Option<TransactionId>,
    /// Global commit timestamp counter
    commit_counter: Arc<AtomicU64>,
    /// Last commit timestamp per tuple - tracks when each tuple was last modified
    tuple_commits: TupleCommitLog,
    /// Last created transaction id:
    last_created_transaction: AtomicU64,
}

enum ValidationResult {
    Conflict(LogicalId),
    Allowed,
}

impl TransactionCoordinator {
    pub fn new(pager: SharedPager) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            pager,
            last_committed: None,
            commit_counter: Arc::new(AtomicU64::new(1)),
            tuple_commits: Arc::new(RwLock::new(HashMap::new())),
            last_created_transaction: AtomicU64::new(0),
        }
    }

    pub fn with_last_created_transaction(self, last_id: TransactionId) -> Self {
        self.last_created_transaction
            .store(last_id, Ordering::Relaxed);
        self
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
        let txid = 0; // TODO , GENERATE TRANSACTION IDS.

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
}

/// Handle to an active transaction
/// Provides a safe interface for transaction operations
pub struct TransactionHandle {
    id: TransactionId,
    snapshot: Snapshot,
    coordinator: TransactionCoordinator,
}

impl TransactionHandle {
    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
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
