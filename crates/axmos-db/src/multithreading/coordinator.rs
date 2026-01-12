//! MVCC Transaction System for AxmosDB
use crate::{
    io::pager::SharedPager,
    types::{LogicalId, TransactionId},
};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Transaction errors (top-level)
#[derive(Debug)]
pub enum TransactionError {
    Aborted(TransactionId),
    WriteWriteConflict(TransactionId, LogicalId),
    NotFound(TransactionId),
    TupleNotVisible(TransactionId, LogicalId),
    TransactionAlreadyStarted,
    TransactionNotStarted,
    InvalidTransactionState(TransactionState),
    Other(String),
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Aborted(id) => write!(f, "Transaction {} aborted", id),

            Self::WriteWriteConflict(txid, tuple) => {
                write!(f, "Transaction {} conflict on tuple {}", txid, tuple)
            }
            Self::NotFound(id) => write!(f, "Transaction {} not found", id),
            Self::TupleNotVisible(id, logical) => {
                write!(f, "Tuple {} not visible to transaction {}", logical, id)
            }
            Self::TransactionAlreadyStarted => f.write_str("transaction already in progress"),
            Self::TransactionNotStarted => f.write_str("transaction has not started yet"),
            Self::InvalidTransactionState(st) => write!(f, "invalid transaction state {st}"),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl Error for TransactionError {}

pub type TransactionResult<T> = Result<T, TransactionError>;
/// Last commit timestamp per tuple
type TupleCommitLog = Arc<RwLock<HashMap<LogicalId, u64>>>;

// Macro for creating snapshots with low boilerplate.
#[macro_export]
macro_rules! snapshot {
    (xid: $xid:expr, xmin: $xmin:expr, xmax: $xmax:expr) => {{
        $crate::multithreading::coordinator::Snapshot::new(
            $crate::types::TransactionId::from($xid as u64),
            $crate::types::TransactionId::from($xmin as u64),
            Some($crate::types::TransactionId::from($xmax as u64)),
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
        )
    }};


    (xid: $xid:expr, xmin: $xmin:expr) => {{
        $crate::multithreading::coordinator::Snapshot::new(
            $crate::types::TransactionId::from($xid as u64),
            $crate::types::TransactionId::from($xmin as u64),
            None,
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
        )
    }};

    (xid: $xid:expr, xmin: $xmin:expr, xmax: $xmax:expr, active: [$($active:expr),* $(,)?]) => {{
        let mut active_set = std::collections::HashSet::new();
        $(active_set.insert($crate::types::TransactionId::from($active as u64));)*
        $crate::multithreading::Snapshot::new(
            $crate::types::TransactionId::from($xid as u64),
            $crate::types::TransactionId::from($xmin as u64),
            Some($crate::types::TransactionId::from($xmax as u64)),
            active_set,
            std::collections::HashSet::new(),
        )
    }};
}

/// Snapshot of the database state at transaction start time.
/// Used to determine tuple visibility under snapshot isolation.
#[derive(Debug, Default, Clone)]
pub struct Snapshot {
    /// Transaction ID that owns this snapshot
    xid: TransactionId,
    /// Lowest active transaction ID when snapshot was taken
    xmin: TransactionId,
    /// Last committed transaction when snapshot was taken (Set to None for the first transaction in the system)
    xmax: Option<TransactionId>,
    /// Set of transaction IDs that were active (uncommitted) at snapshot time
    active_txs: HashSet<TransactionId>,
    /// Set of transaction IDs that were aborted (uncommitted) at snapshot time
    aborted_txs: HashSet<TransactionId>,
}

impl Snapshot {
    pub fn new(
        xid: TransactionId,
        xmin: TransactionId,
        xmax: Option<TransactionId>,
        active: HashSet<TransactionId>,
        aborted: HashSet<TransactionId>,
    ) -> Self {
        Self {
            xid,
            xmin,
            xmax,
            active_txs: active,
            aborted_txs: aborted,
        }
    }

    pub fn is_transaction_aborted(&self, xid: TransactionId) -> bool {
        self.aborted_txs.contains(&xid)
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
            && max_tx < txid
        {
            return false;
        };

        // Transaction was active when we took snapshot (had not commited)
        if self.active_txs.contains(&txid) || self.aborted_txs.contains(&txid) {
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

impl Display for TransactionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Aborted => f.write_str("ABORTED"),
            Self::Active => f.write_str("ACTIVE"),
            Self::Committed => f.write_str("COMMITTED"),
            Self::Committing => f.write_str("COMMITTING"),
        }
    }
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
    write_set: HashMap<LogicalId, u8>,
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

    pub fn add_to_write_set(&mut self, id: LogicalId, version: u8) {
        self.write_set.insert(id, version);
    }

    pub fn write_set(&self) -> &HashMap<LogicalId, u8> {
        &self.write_set
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active)
    }
}

/// Shared transaction table protected by RwLock
type SharedTransactionTable = Arc<RwLock<HashMap<TransactionId, TransactionMetadata>>>;

impl Clone for TransactionCoordinator {
    fn clone(&self) -> Self {
        Self {
            transactions: Arc::clone(&self.transactions),
            pager: self.pager.clone(),
            commit_counter: Arc::clone(&self.commit_counter),
            tuple_commits: Arc::clone(&self.tuple_commits),
        }
    }
}

/// Central coordinator for all transactions in the system.
///
/// Creates transactions, snapshots, tracks active transactions, validates commits and maintains commit order.
/// Transaction state (last_created, last_committed, aborted bitmap) is persisted in PageZero header.
pub struct TransactionCoordinator {
    /// All active and recently committed transactions (in-memory only)
    transactions: SharedTransactionTable,
    /// Shared pager for I/O operations (source of truth for persistent state)
    pager: SharedPager,
    /// Global commit timestamp counter for OCC validation
    commit_counter: Arc<AtomicU64>,
    /// Last commit timestamp per tuple - tracks when each tuple was last modified
    tuple_commits: TupleCommitLog,
}

enum ValidationResult {
    Conflict(LogicalId),
    Allowed,
}

impl TransactionCoordinator {
    pub fn new(pager: SharedPager) -> Self {
        let coordinator = Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            pager,
            commit_counter: Arc::new(AtomicU64::new(1)),
            tuple_commits: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load aborted transactions from PageZero into memory
        coordinator.load_aborted_transactions();

        coordinator
    }

    /// Returns the last committed transaction ID from PageZero header
    pub fn get_last_committed(&self) -> TransactionId {
        self.pager.read().get_last_committed_transaction()
    }

    /// Returns the last created transaction ID from PageZero header
    fn get_last_created(&self) -> TransactionId {
        self.pager.read().get_last_created_transaction()
    }

    /// Loads aborted transactions from PageZero bitmap into the in-memory transaction table
    pub fn load_aborted_transactions(&self) {
        let aborted_txs = self.pager.read().get_aborted_transactions();
        let mut txs = self.transactions.write();

        for txid in aborted_txs {
            if !txs.contains_key(&txid) {
                let snapshot = Snapshot::new(txid, txid, None, HashSet::new(), HashSet::new());
                let mut entry = TransactionMetadata::new(txid, snapshot, 0);
                entry.state = TransactionState::Aborted;
                txs.insert(txid, entry);
            }
        }
    }

    /// Get a set of transactions with the provided state.
    pub fn transaction_set(&self, state: TransactionState) -> HashSet<TransactionId> {
        self.transactions
            .read()
            .iter()
            .filter(|(_, entry)| entry.state() == state)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Create a snapshot of the database state for a given transaction id.
    pub fn snapshot(&self, txid: TransactionId) -> TransactionResult<Snapshot> {
        // Collect active transaction ids
        let active: HashSet<TransactionId> = self.transaction_set(TransactionState::Active);

        // Collect tracked aborted transaction ids
        let aborted: HashSet<TransactionId> = self.transaction_set(TransactionState::Aborted);

        // xmin is the smallest active transaction ID (or our ID if none active)
        let xmin = active.iter().min().copied().unwrap_or(txid);

        // xmax is the last committed transaction from PageZero
        let xmax = {
            let last = self.get_last_committed();
            if last == 0 { None } else { Some(last) }
        };

        Ok(Snapshot::new(txid, xmin, xmax, active, aborted))
    }

    /// Begin a new transaction.
    /// Returns a [TransactionHandle] to the thread that requested the begin operation.
    pub fn begin(&self) -> TransactionResult<TransactionHandle> {
        // Atomically get and increment the transaction ID in PageZero
        let txid = {
            let mut pager = self.pager.write();
            let current = pager.get_last_created_transaction();
            pager.set_last_created_transaction(current + 1);
            current
        };

        // Create snapshot
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
            commit_handle: Some(CommitHandle::new(self.clone())),
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

                // Update last_committed in PageZero if this is the highest committed txid
                {
                    let mut pager = self.pager.write();
                    let current_last = pager.get_last_committed_transaction();
                    if txid > current_last {
                        pager.set_last_committed_transaction(txid);
                    }
                }

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
        {
            let mut txs = self.transactions.write();

            let entry = txs.get_mut(&txid).ok_or(TransactionError::NotFound(txid))?;

            entry.state = TransactionState::Aborted;
        }

        // Persist aborted state to page zero for crash recovery
        self.pager.write().mark_transaction_aborted(txid);
        Ok(())
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
        version: u8,
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
    pub fn cleanup_old_transactions(&self, min_active_xid: TransactionId) -> usize {
        let current_count = self.transactions.read().len();
        self.transactions.write().retain(|id, entry| {
            // Keep active transactions
            if entry.state == TransactionState::Active {
                return true;
            }
            // Keep transactions that might still be needed for visibility
            *id >= min_active_xid
        });
        let final_count = self.transactions.read().len();
        final_count.saturating_sub(current_count)
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }

    /// Aborts all currently active transactions.
    /// Returns the list of transaction IDs that were aborted.
    pub fn abort_all(&self) -> Vec<TransactionId> {
        let mut txs = self.transactions.write();
        let mut aborted = Vec::new();

        for (id, entry) in txs.iter_mut() {
            if entry.state == TransactionState::Active {
                entry.state = TransactionState::Aborted;
                aborted.push(*id);
            }
        }

        aborted
    }

    /// Cleans up all completed (committed or aborted) transactions from memory.
    /// Only keeps transactions that are still needed for visibility checks.
    /// Returns the number of transactions cleaned up.
    pub fn vacuum_transactions(&self) -> usize {
        let oldest_required = self.get_last_committed();
        let removed = self.cleanup_old_transactions(oldest_required);

        // Also clean up tuple commits that are no longer needed
        let min_start_ts = self.get_min_active_start_ts();
        self.cleanup_tuple_commits(min_start_ts);

        removed
    }

    /// Gets the minimum start timestamp among all active transactions.
    fn get_min_active_start_ts(&self) -> u64 {
        let txs = self.transactions.read();

        txs.values()
            .filter(|entry| entry.state == TransactionState::Active)
            .map(|entry| entry.start_ts())
            .min()
            .unwrap_or(self.commit_counter.load(Ordering::SeqCst))
    }
}

/// Handle to an active transaction
/// Provides a safe interface for transaction operations
pub struct TransactionHandle {
    id: TransactionId,
    snapshot: Snapshot,
    commit_handle: Option<CommitHandle>,
}

impl TransactionHandle {
    pub fn new(id: TransactionId, snapshot: Snapshot) -> Self {
        Self {
            id,
            snapshot,
            commit_handle: None,
        }
    }

    pub(crate) fn with_commit_handle(mut self, commit_handle: CommitHandle) -> Self {
        self.commit_handle = Some(commit_handle);
        self
    }
}

/// The commit handle is not [Clone] so that we make sure each transaction only commits or aborts once.
impl Clone for TransactionHandle {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            snapshot: self.snapshot.clone(),
            commit_handle: None,
        }
    }
}

pub(crate) struct CommitHandle {
    coordinator: TransactionCoordinator,
}

impl CommitHandle {
    pub fn new(coordinator: TransactionCoordinator) -> Self {
        Self { coordinator }
    }

    /// Commit this transaction, consuming itself
    pub fn commit(self, id: TransactionId) -> TransactionResult<()> {
        self.coordinator.commit(id)
    }

    /// Abort this transaction, consuming itself.
    pub fn abort(self, id: TransactionId) -> TransactionResult<()> {
        self.coordinator.abort(id)
    }
}

impl TransactionHandle {
    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn can_commit(&self) -> bool {
        self.commit_handle.is_some()
    }

    pub fn commit(&mut self) -> TransactionResult<()> {
        if let Some(handle) = self.commit_handle.take() {
            handle.commit(self.id())?;
        };

        Ok(())
    }

    pub fn abort(&mut self) -> TransactionResult<()> {
        if let Some(handle) = self.commit_handle.take() {
            handle.abort(self.id())?;
        };
        Ok(())
    }
}

// If the handle is not committed before, the transaction will be aborted whenever it is dropped.
impl Drop for TransactionHandle {
    fn drop(&mut self) {
        let _ = self.abort();
    }
}
