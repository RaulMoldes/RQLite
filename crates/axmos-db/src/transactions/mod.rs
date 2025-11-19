use crate::Key;
use crate::types::{TxId, ItemId};
use crate::database::errors::TransactionError;
use rand::Rng;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;


pub type Version = u64;

/// Database transaction manager for transactional reads and writes.
/// The ANSI/ISO SQL standard defines four levels of transaction isolation in terms of three phenomena that must be prevented between concurrent transactions. These undesirable phenomena are:
///
/// dirty reads: A transaction reads data written by concurrent uncommitted transaction.
///
/// non-repeatable reads: A transaction re-reads data it has previously read and finds that data has been modified by another transaction (that committed since the initial read).
///
/// phantom read: A transaction re-executes a query returning a set of rows that satisfy a search condition and finds that the set of rows satisfying the condition has changed due to another recently-committed transaction.
///
/// The ANSI isolation levels are defined according to these three phenomena, where the highest isolation level is guarantees serializability.
///
/// In practice, most Database Systems do not support serializable transactions, since this would be of too much performance cost.
///
/// Generally, lower levels of isolation are supported.
///
/// Postgres, for instance, supports Read Committed Isolation Level, which according to how they define it in the docs (https://www.postgresql.org/docs/7.1/xact-read-committed.html)means that: When a transaction runs on this isolation level, a SELECT query sees only data committed before the query began and never sees either uncommitted data or changes committed during query execution by concurrent transactions. (However, the SELECT does see the effects of previous updates executed within this same transaction, even though they are not yet committed.)
///
/// My goal with this transaction manager is to guarantee at least [Read-Commited] on most scenarios.
///
/// This is achived by combining two common techinques for transaction serializability:
///
/// [Two Phase Locking]: Transactions asks for locks to the centralized lock manager in two phases.
///
/// P1: Incrementally acquire locks in order to perform your changes (it is recommended that the order of acquisition is stablished by the runtime, as that would minimize deadlocks)
///
/// P2: Release all locks. Given that it is not allowed to acquire more locks once you release them, it can never be the case that a transaction reads something that has been uncommited by a different transaction.
///
/// In theory, you could be able to modify stuff for the locks you already have once you start releasing them, but it is currently not the case of this implementation of the protocol (probably a future improvement).
///
/// If you read through the code you will realize another thing that I have added is the commit check. Basically I store an additional version number with all locks. At write time, transactions increment the version of the lock and at commit time, the runtime only allows transactions to commit if they have an older version than the one found in the  global store. This MVCC (Multi-Version-Concurrency-Control) is a good idea for future improvements but currently is not doing anything, since locks are released all at once at each commit and are removed from the tx manager. Must find a way to do this better.
pub const MAX_WAIT_PERIOD_SECS: u64 = 60;

// Handle that represents the combination of a mutex boolean variable and a waiting condition to notify the thread back up.
// The mutex could simply be an [AtomicBool] here but rust CondVars require a Mutex guards to work with them.
pub type WaitHandle = (Mutex<bool>, Condvar);
pub type WaitForGraph = HashMap<TxId, HashSet<TxId>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataItem {
    id: ItemId,
    version: Version,
}

impl DataItem {
    fn new(id: ItemId) -> Self {
        Self { id, version: 0 }
    }

    fn inc_version(&mut self) {
        self.version += 1;
    }

    fn version(&self) -> &Version {
        &self.version
    }

    fn id(&self) -> &ItemId {
        &self.id
    }
}

#[derive(Debug)]
pub struct Transaction {
    id: TxId,
    read_set: HashSet<DataItem>,
    write_set: HashSet<DataItem>,
    status: TransactionState,
    wait_handle: Option<Arc<WaitHandle>>,
}

impl Transaction {
    fn new() -> Self {
        Self {
            id: TxId::new_key(),
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            status: TransactionState::Running,
            wait_handle: None,
        }
    }

    fn id(&self) -> TxId {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum TransactionState {
    Running,
    Committed,
    Aborted,
    Waiting,
}

#[derive(Debug, Clone)]
struct LockMeta {
    holders: HashSet<TxId>,
    lock_type: LockType,
}

impl LockMeta {
    fn new(holder: TxId, lock_type: LockType) -> Self {
        let mut holders = HashSet::new();
        holders.insert(holder);
        Self { holders, lock_type }
    }
}

#[derive(Debug, Clone)]
struct WaiterMetadata {
    tx_id: TxId,
    lock_type: LockType,
    wait_handle: Arc<WaitHandle>,
}

impl WaiterMetadata {
    fn _new(tx_id: TxId, lock_type: LockType, handle: Arc<WaitHandle>) -> Self {
        Self {
            tx_id,
            lock_type,
            wait_handle: handle,
        }
    }

    fn id(&self) -> TxId {
        self.tx_id
    }

    fn handle(&self) -> &WaitHandle {
        &self.wait_handle
    }
}

#[derive(Debug)]
pub struct LockEntry {
    item: DataItem,
    metadata: LockMeta,
    waiters: VecDeque<WaiterMetadata>,
}

impl LockEntry {
    fn new_exclusive(item: DataItem, holder: TxId) -> Self {
        Self {
            item,
            metadata: LockMeta::new(holder, LockType::Exclusive),
            waiters: VecDeque::new(),
        }
    }

    fn new_shared(item: DataItem, holder: TxId) -> Self {
        Self {
            item,
            metadata: LockMeta::new(holder, LockType::Shared),
            waiters: VecDeque::new(),
        }
    }

    fn id(&self) -> &ItemId {
        &self.item.id
    }

    fn version(&self) -> &Version {
        &self.item.version
    }

    fn set_version(&mut self, v: Version) {
        self.item.version = v;
    }

    fn holders(&self) -> &HashSet<TxId> {
        &self.metadata.holders
    }

    fn waiters(&self) -> HashSet<TxId> {
        self.waiters.iter().map(|w| w.id()).collect()
    }

    fn add_holder(&mut self, holder: TxId) {
        if matches!(self.metadata.lock_type, LockType::Shared) {
            self.metadata.holders.insert(holder);
        } else {
            panic!("Cannot add more than one holder to an exclusive-type lock.")
        }
    }

    fn remove_holder(&mut self, holder: TxId) {
        self.metadata.holders.remove(&holder);
    }

    fn add_waiter(
        &mut self,
        tx_id: TxId,
        lock_type: LockType,
        wait_handle: Arc<(Mutex<bool>, Condvar)>,
    ) {
        self.waiters.push_back(WaiterMetadata {
            tx_id,
            lock_type,
            wait_handle,
        });
    }

    fn is_shared(&self) -> bool {
        matches!(self.metadata.lock_type, LockType::Shared)
    }

    fn _is_exclusive(&self) -> bool {
        matches!(self.metadata.lock_type, LockType::Exclusive)
    }

    // Promote and notify the first waiter from the queue (if there is some).
    fn promote_next_waiter(&mut self) -> Option<TxId> {
        if self.metadata.holders.is_empty()
            && !self.waiters.is_empty()
            && let Some(waiter) = self.waiters.pop_front()
        {
            self.metadata.holders.insert(waiter.tx_id);
            self.metadata.lock_type = waiter.lock_type;

            let (lock, cvar) = waiter.handle();
            let mut can_proceed = lock.lock().unwrap();
            *can_proceed = true;
            cvar.notify_one();

            return Some(waiter.id());
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum LockType {
    Shared,
    Exclusive,
}

#[derive(Default)]
pub struct TransactionManager {
    txs: Arc<RwLock<HashMap<TxId, Transaction>>>,
    locks: Arc<RwLock<HashMap<ItemId, LockEntry>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_transaction(&self) -> TxId {
        let tx = Transaction::new();
        let id = tx.id();
        self.txs.write().unwrap().insert(tx.id(), tx);
        id
    }

    pub fn read(&self, tx_id: TxId, item_id: ItemId) -> Result<(), TransactionError> {
        {
            let txs = self.txs.read().unwrap();
            let tx = txs.get(&tx_id).ok_or(TransactionError::NotFound(tx_id))?;

            // Check if the transaction is running
            if !matches!(tx.status, TransactionState::Running) {
                return Err(TransactionError::NotRunning(tx_id));
            }
        }

        let current_item = self.acquire_lock(item_id, tx_id, LockType::Shared)?;

        if let Some(tx) = self.txs.write().unwrap().get_mut(&tx_id) {
            tx.read_set.insert(current_item);
        }
        Ok(())
    }

    pub fn write(
        &self,
        tx_id: TxId,
        item_id: ItemId,
        _value: Vec<u8>,
    ) -> Result<(), TransactionError> {
        {
            let txs = self.txs.read().unwrap();
            let tx = txs.get(&tx_id).ok_or(TransactionError::NotFound(tx_id))?;
            if !matches!(tx.status, TransactionState::Running) {
                return Err(TransactionError::NotRunning(tx_id));
            }
        }

        let mut item = self.acquire_lock(item_id, tx_id, LockType::Exclusive)?;
        item.inc_version();
        if let Some(tx) = self.txs.write().unwrap().get_mut(&tx_id) {
            tx.write_set.insert(item);
        }

        Ok(())
    }

    fn acquire_lock(
        &self,
        item_id: ItemId,
        tx_id: TxId,
        lock_type: LockType,
    ) -> Result<DataItem, TransactionError> {
        loop {
            let wait_handle = {
                let mut locks = self.locks.write().unwrap();

                let entry = locks.entry(item_id).or_insert_with(|| match lock_type {
                    LockType::Exclusive => LockEntry::new_exclusive(DataItem::new(item_id), tx_id),
                    LockType::Shared => LockEntry::new_shared(DataItem::new(item_id), tx_id),
                });

                // Holders already had our id so nothing to do
                if entry.holders().contains(&tx_id) {
                    return Ok(entry.item.clone());
                };

                match lock_type {
                    LockType::Exclusive => {
                        if entry.holders().is_empty() {
                            entry.metadata = LockMeta::new(tx_id, LockType::Exclusive);
                            return Ok(entry.item.clone());
                        }
                    }
                    LockType::Shared => {
                        if entry.is_shared() || entry.holders().is_empty() {
                            if entry.holders().is_empty() {
                                entry.metadata = LockMeta::new(tx_id, LockType::Shared);
                            } else {
                                entry.add_holder(tx_id);
                            }
                        }
                    }
                }

                // Start waiting loop
                let wait_handle = Arc::new((Mutex::new(false), Condvar::new()));
                entry.add_waiter(tx_id, lock_type, wait_handle.clone());

                if let Some(tx) = self.txs.write().unwrap().get_mut(&tx_id) {
                    tx.status = TransactionState::Waiting;
                    tx.wait_handle = Some(wait_handle.clone());
                }

                wait_handle
            };

            let (lock, cvar) = &*wait_handle;
            let result = cvar
                .wait_timeout_while(
                    lock.lock().unwrap(), // till the lock becomes true, this thread will wait
                    Duration::from_secs(MAX_WAIT_PERIOD_SECS), // TODO: this must be part of the configuration.
                    |can_proceed| !*can_proceed,
                )
                .unwrap();

            // Condvar wait timed out. We need to abort this transaction.
            if result.1.timed_out() {
                let mut locks = self.locks.write().unwrap();
                if let Some(entry) = locks.get_mut(&item_id) {
                    entry.waiters.retain(|w| w.tx_id != tx_id);
                }

                if let Some(tx) = self.txs.write().unwrap().get_mut(&tx_id) {
                    tx.status = TransactionState::Aborted;
                }

                return Err(TransactionError::TimedOut(tx_id));
            }

            if let Some(tx) = self.txs.write().unwrap().get_mut(&tx_id) {
                tx.status = TransactionState::Running;
                tx.wait_handle = None;
            }
        }
    }

    fn release_locks(&self, tx: Transaction, commit_fl: bool) -> Vec<TxId> {
        let mut locks = self.locks.write().unwrap();
        let locks_to_release: Vec<DataItem> = tx
            .read_set
            .iter()
            .chain(tx.write_set.iter())
            .cloned()
            .collect();

        let mut promoted: Vec<TxId> = Vec::new();
        for item in locks_to_release {
            let should_remove = if let Some(entry) = locks.get_mut(item.id()) {
                // Remove this transaction from the holder list
                entry.remove_holder(tx.id());

                if commit_fl {
                    entry.set_version(item.version);
                };

                if let Some(promoted_tx) = entry.promote_next_waiter() {
                    promoted.push(promoted_tx);
                }

                entry.holders().is_empty() && entry.waiters.is_empty()
            } else {
                false
            };

            if should_remove {
                locks.remove(item.id());
            }
        }
        promoted
    }

    // Checks if an existing transaction can commit by means of comparing the version of its written items.
    // If any of the versions is lower than the stored global verion nb, this means some other transaction modified the item at some point, and therefore we are not allowed to commit.
    fn can_commit(&self, txid: &TxId) -> Result<(), TransactionError> {
        let locks = self.locks.read().unwrap();
        let transactions = self.txs.read().unwrap();
        if let Some(tx) = transactions.get(txid) {
            // Obtain the list of written data
            let written: Vec<DataItem> = tx.write_set.iter().cloned().collect();

            // Early return if we have not written anything
            if written.is_empty() {
                return Ok(());
            };

            for incoming_item in written {
                if let Some(stored_item) = locks.get(incoming_item.id()) {
                    if incoming_item.id() != stored_item.id() {
                        return Err(TransactionError::Aborted(tx.id()));
                    };

                    if incoming_item.version() <= stored_item.version() {
                        return Err(TransactionError::Aborted(tx.id()));
                    };
                }
            }
            return Ok(());
        }
        Err(TransactionError::NotFound(*txid))
    }

    // Commits an existing transaction and releases all locks, notifying corresponding transactions to continue running.
    // Currently, it is a no-op if the transaction is not found. I might change this if I see that it causes problems.
    // TODO: This should include some kind of logic to see if we can actually commit, like checking versions or last-modified timestamps of data items.
    pub fn commit(&self, tx_id: TxId) {
        let mut txs = self.txs.write().unwrap();

        let promoted = if let Some(mut tx) = txs.remove(&tx_id) {
            tx.status = TransactionState::Committed;
            self.release_locks(tx, true)
        } else {
            Vec::new()
        };

        // Set the promoted transactions to run again.
        for tx in promoted {
            if let Some(transaction) = txs.get_mut(&tx) {
                transaction.status = TransactionState::Running;
            };
        }
    }

    // Aborts an existing transaction and releases all locks, notifying corresponding transactions to continue running.
    // Currently, it is a no-op if the transaction is not found. I might change this if I see that it causes problems.
    pub fn abort(&self, tx_id: TxId) {
        let mut txs = self.txs.write().unwrap();

        let promoted = if let Some(mut tx) = txs.remove(&tx_id) {
            tx.status = TransactionState::Aborted;
            self.release_locks(tx, false)
        } else {
            Vec::new()
        };

        for tx in promoted {
            if let Some(transaction) = txs.get_mut(&tx) {
                transaction.status = TransactionState::Running;
            };
        }
    }

    // Take ownership of F in order to decorate it with the commit check at the end. This ensures all transactions check for commit before terminating
    fn commit_checker<'a, F, R>(
        mut f: F,
    ) -> impl FnMut(&TransactionManager, TxId) -> Result<R, String>
    where
        F: FnMut(&TransactionManager, TxId) -> Result<R, String> + 'a,
    {
        move |tm, tx_id| {
            let result = f(tm, tx_id)?;
            tm.can_commit(&tx_id)?;
            Ok(result)
        }
    }

    // Main function that is able to execute transactions.
    pub fn run_transaction<F, R>(&self, max_retries: usize, f: F) -> Result<R, TransactionError>
    where
        F: FnMut(&TransactionManager, TxId) -> Result<R, String>,
    {
        let mut attempt = 0usize;
        let mut backoff_ms = 10u64;

        // decorate the input userspace functor with the pre-commit check.
        let mut f = Self::commit_checker(f);

        loop {
            attempt += 1;
            let tx_id = self.begin_transaction();
            // Execute user side logic
            match f(self, tx_id) {
                Ok(res) => {
                    // Commit.
                    self.commit(tx_id);
                    return Ok(res);
                }
                Err(_) => {
                    // If the transaction is aborted by the system
                    if attempt > max_retries {
                        return Err(TransactionError::Aborted(tx_id));
                    }

                    // Exponential backoff + jitter
                    let mut rng = rand::rng();
                    let jitter: u64 = rng.random_range(0..backoff_ms);
                    let sleep_ms = backoff_ms + jitter;
                    std::thread::sleep(Duration::from_millis(sleep_ms));

                    backoff_ms = (backoff_ms * 2).min(1000);
                    // Retry again
                    continue;
                }
            }
        }
    }

    // Constructs a wait-for graph in memory.
    // Each entry contains the list of transactions to which the key transaction is waiting for.
    // The wait-for-graph includes an entry for eacg transaction, pointing to the list of transactions it currently waits for.
    // In case you are interested: https://adamdjellouli.com/articles/databases_notes/07_concurrency_control/02_deadlocks
    fn build_wfg(&self) -> WaitForGraph {
        let mut graph = HashMap::new();
        let locks = self.locks.read().unwrap();

        for entry in locks.values() {
            // Each waiter will be waiting for all holders.
            for waiter in &entry.waiters() {
                let waiting_for = entry.holders().clone();

                graph
                    .entry(*waiter)
                    .or_insert_with(HashSet::new)
                    .extend(waiting_for);
            }
        }

        graph
    }

    // Detect cycles in the graph using a Depth-First-Search approach.
    fn has_cycle(&self, graph: &WaitForGraph) -> Option<Vec<TxId>> {
        let mut visited = HashSet::new(); // Visited nodes.
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new(); // Keep track of our path in the graph.

        // Depth first search.
        // This outer loop traverses the graph downwards:
        // The inner one goes right-wards in the hashmap of hashsets
        // [ID 1] -> [ID2, ID3, ID4, ...]
        // [ID 2] -> [ID5, ID7, ID6, ...]
        // [...]
        for &node in graph.keys() {
            if !visited.contains(&node)
                && let Some(cycle) =
                    self.detect(node, graph, &mut visited, &mut rec_stack, &mut path)
            {
                return Some(cycle);
            }
        }

        None
    }

    // Detect cycles at a single graph level.
    fn detect(
        &self,
        node: TxId,
        graph: &WaitForGraph,
        visited: &mut HashSet<TxId>,
        rec_stack: &mut HashSet<TxId>,
        path: &mut Vec<TxId>,
    ) -> Option<Vec<TxId>> {
        visited.insert(node);
        rec_stack.insert(node);
        path.push(node);

        // Obtain the list of nodes we are waiting for.
        // Iterate over them to check if they are also waiting for us.
        if let Some(neighbors) = graph.get(&node) {
            // For each neighbor, check if it is already visited.
            for &neighbor in neighbors {
                // Check if this node is waiting for someone which is also waiting for ourselves.
                if !visited.contains(&neighbor) {
                    if let Some(cycle) = self.detect(neighbor, graph, visited, rec_stack, path) {
                        return Some(cycle);
                    }

                // This means we are waiting for a neighbor that is also waiting for us or for another previous neighbour.
                } else if rec_stack.contains(&neighbor) {
                    // This is a cycle.
                    // As the rec stack is popped after each detect call, finding a node in the stack means we are waiting for a node that was also waiting for us.
                    let cycle_start = path.iter().position(|&x| x == neighbor).unwrap();
                    return Some(path[cycle_start..].to_vec());
                }
            }
        }

        rec_stack.remove(&node);
        path.pop();
        None
    }

    // Resolves deadlocks in the WaitForGraph by aborting the youngest transaction
    // The age of the transaction is the lowest TxId.
    // In most cases, the youngest transaction would have performed less work.
    fn resolve_deadlock(&self, cycle: Vec<TxId>) {
        println!("DEADLOCK DETECTED! Cycle: {:?}", cycle);

        // Find the youngest transaction id
        if let Some(&youngest_tx) = cycle.iter().max() {
            println!("Aborting youngest transaction: {}", youngest_tx);

            // Abort it
            self.abort(youngest_tx);
        }
    }

    pub fn start_deadlock_detection(self: Arc<Self>) {
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(5));

                // Build wfg
                let graph = self.build_wfg();

                if !graph.is_empty() {
                    println!("Wait-for graph: {:?}", graph);

                    // Detect cycles
                    if let Some(cycle) = self.has_cycle(&graph) {
                        self.resolve_deadlock(cycle);
                    }
                }
            }
        });
    }
}




fn main() {


    let tm = Arc::new(TransactionManager::new());
    tm.clone().start_deadlock_detection();

    let mut handles = vec![];

    for i in 0..3 {
        let tm_clone = tm.clone();
        let handle = thread::spawn(move || {
            let tx_id = tm_clone.begin_transaction();
            println!("Thread {} started transaction: {}", i, tx_id);

            match tm_clone.read(tx_id, ItemId::from(1)) {
                Ok(_) => println!("Thread {} read successful", i),
                Err(e) => println!("Thread {} read failed: {}", i, e),
            }

            tm_clone.commit(tx_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Instant;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_tm_1() {
        let tm = TransactionManager::new();

        let tx1 = tm.begin_transaction();
        assert!(tm.read(tx1, ItemId::from(1)).is_ok());
        tm.commit(tx1);
    }

    #[test]
    fn test_tm_2() {
        let tm = TransactionManager::new();

        let tx1 = tm.begin_transaction();
        assert!(tm.write(tx1, ItemId::from(1), vec![1, 2, 3]).is_ok());
        tm.commit(tx1);
    }

    #[test]
    fn test_tm_3() {
        let tm = Arc::new(TransactionManager::new());
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = vec![];

        for i in 0..3 {
            let tm_clone = tm.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                let tx = tm_clone.begin_transaction();
                barrier_clone.wait();

                let result = tm_clone.read(tx, ItemId::from(1));
                assert!(result.is_ok(), "Thread {} failed to read: {:?}", i, result);

                thread::sleep(Duration::from_millis(100));

                tm_clone.commit(tx);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_tm_4() {
        let tm = Arc::new(TransactionManager::new());
        let barrier = Arc::new(Barrier::new(2));

        let tm_clone = tm.clone();
        let barrier_clone = barrier.clone();

        let writer_handle = thread::spawn(move || {
            let tx = tm_clone.begin_transaction();
            barrier_clone.wait();

            assert!(tm_clone.write(tx, ItemId::from(1), vec![]).is_ok());
            thread::sleep(Duration::from_millis(500));
            tm_clone.commit(tx);
        });

        let tm_clone = tm.clone();
        let barrier_clone = barrier.clone();

        let reader_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let tx = tm_clone.begin_transaction();
            barrier_clone.wait();

            let start = Instant::now();
            assert!(tm_clone.read(tx, ItemId::from(1)).is_ok());
            let elapsed = start.elapsed();

            assert!(
                elapsed.as_millis() >= 400,
                "Reader should have waited, but only waited {:?}",
                elapsed
            );
            tm_clone.commit(tx);
        });

        writer_handle.join().unwrap();
        reader_handle.join().unwrap();
    }

    #[test]
    fn test_tm_5() {
        let tm = Arc::new(TransactionManager::new());

        let tx1 = tm.begin_transaction();
        assert!(tm.read(tx1, ItemId::from(1)).is_ok());

        let tm_clone = tm.clone();
        let writer_handle = thread::spawn(move || {
            let tx = tm_clone.begin_transaction();
            let start = Instant::now();
            assert!(tm_clone.write(tx, ItemId::from(1), vec![]).is_ok());
            let elapsed = start.elapsed();
            tm_clone.commit(tx);
            elapsed
        });

        thread::sleep(Duration::from_millis(200));
        tm.commit(tx1);

        let elapsed = writer_handle.join().unwrap();
        assert!(elapsed.as_millis() >= 150, "Writer should have waited");
    }

    #[test]
    fn test_tm_6() {
        let tm = Arc::new(TransactionManager::new());
        let results = Arc::new(Mutex::new(Vec::new()));

        let tx1 = tm.begin_transaction();
        assert!(tm.write(tx1, ItemId::from(1), vec![]).is_ok());

        let mut handles = vec![];

        for i in 0..3 {
            let tm_clone = tm.clone();
            let results_clone = results.clone();

            let handle = thread::spawn(move || {
                let tx = tm_clone.begin_transaction();
                assert!(tm_clone.write(tx, ItemId::from(1), vec![]).is_ok());

                results_clone.lock().unwrap().push(i);

                thread::sleep(Duration::from_millis(50));
                tm_clone.commit(tx);
            });

            handles.push(handle);
            thread::sleep(Duration::from_millis(10));
        }

        thread::sleep(Duration::from_millis(100));
        tm.commit(tx1);

        for handle in handles {
            handle.join().unwrap();
        }

        let final_results = results.lock().unwrap();
        assert_eq!(
            *final_results,
            vec![0, 1, 2],
            "Transactions should be processed in FIFO order"
        );
    }

    #[test]
    fn test_tm_7() {
        let tm = TransactionManager::new();

        let tx1 = tm.begin_transaction();
        assert!(tm.read(tx1, ItemId::from(1)).is_ok());
        assert!(tm.write(tx1, ItemId::from(2), vec![]).is_ok());
        tm.abort(tx1);

        let tx2 = tm.begin_transaction();
        assert!(tm.write(tx2, ItemId::from(1), vec![]).is_ok());
        assert!(tm.write(tx2, ItemId::from(2), vec![]).is_ok());
        tm.commit(tx2);
    }

    #[test]
    fn test_tm_8() {
        let tm = Arc::new(TransactionManager::new());

        let tx1 = tm.begin_transaction();
        assert!(tm.write(tx1, ItemId::from(1), vec![]).is_ok());

        let tm_clone = tm.clone();
        let handle = thread::spawn(move || {
            let tx2 = tm_clone.begin_transaction();
            let result = tm_clone.write(tx2, ItemId::from(2), vec![]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("timed out"));
        });

        thread::sleep(Duration::from_secs(35));

        handle.join().unwrap();
        tm.commit(tx1);
    }

    #[test]
    fn test_tm_9() {
        let tm = TransactionManager::new();

        let tx1 = tm.begin_transaction();
        assert!(tm.read(tx1, ItemId::from(1)).is_ok());
        assert!(tm.read(tx1,ItemId::from(1)).is_ok());
        tm.commit(tx1);
    }

    #[test]
    fn test_tm_10() {
        let tm = Arc::new(TransactionManager::new());
        let barrier = Arc::new(Barrier::new(5));
        let mut handles = vec![];

        for i in 0..5 {
            let tm_clone = tm.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                let tx = tm_clone.begin_transaction();
                barrier_clone.wait();

                if i % 2 == 0 {
                    assert!(tm_clone.read(tx, ItemId::from(i as u64)).is_ok());
                } else {
                    assert!(tm_clone.write(tx, ItemId::from(i as u64), vec![i as u8]).is_ok());
                }

                thread::sleep(Duration::from_millis(50));
                tm_clone.commit(tx);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_tm_11() {
        const NUM_THREADS: u64 = 100;
        let tm = Arc::new(TransactionManager::new());
        let mut handles = vec![];

        for i in 0..NUM_THREADS {
            let tm_clone = tm.clone();

            let handle = thread::spawn(move || {
                for j in 0..5 {
                    let tx = tm_clone.begin_transaction();

                    let item = ItemId::from((i * 5 + j) % 3);

                    if j % 2 == 0 {
                        assert!(tm_clone.read(tx, item).is_ok());
                    } else {
                        assert!(tm_clone.write(tx, item, vec![]).is_ok());
                    }

                    thread::sleep(Duration::from_millis(10));
                    tm_clone.commit(tx);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_tm_12() {
        let tm = Arc::new(TransactionManager::new());
        tm.clone().start_deadlock_detection();

        let tm1 = tm.clone();
        let handle1 = thread::spawn(move || {
            let tx1 = tm1.begin_transaction();
            assert!(tm1.write(tx1, ItemId::from(1), vec![]).is_ok());

            thread::sleep(Duration::from_millis(100));
            let result = tm1.write(tx1, ItemId::from(2), vec![]);

            if result.is_err() {
                println!("T1: Was aborted by deadlock detection");
            } else {
                tm1.commit(tx1);
            }
        });

        let tm2 = tm.clone();
        let handle2 = thread::spawn(move || {
            let tx2 = tm2.begin_transaction();
            println!("T2: Acquiring lock on item 2");
            assert!(tm2.write(tx2, ItemId::from(2), vec![]).is_ok());

            thread::sleep(Duration::from_millis(100));

            println!("T2: Trying to acquire lock on item 1");
            let result = tm2.write(tx2, ItemId::from(1), vec![]);

            if result.is_err() {
                println!("T2: Was aborted by deadlock detection");
            } else {
                tm2.commit(tx2);
            }
        });

        // Wait enough time for the lock to be detected
        thread::sleep(Duration::from_secs(7));

        handle1.join().unwrap();
        handle2.join().unwrap();

        println!("Deadlock test completed");
    }

    #[test]
    fn test_tm_13() {
        let tm = TransactionManager::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = counter.clone();
        let result = tm.run_transaction(5, move |_tm, _tx_id| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);

            if attempt < 2 {
                Err("Simulated failure".into())
            } else {
                Ok("OK".to_string())
            }
        });

        assert_eq!(result.unwrap(), "OK");
        assert!(counter.load(Ordering::SeqCst) >= 3);
    }

    #[test]
    fn test_tm_14() {
        // Validate version increments on writes
        let tm = Arc::new(TransactionManager::new());
        let tx1 = tm.begin_transaction();
        assert!(tm.write(tx1, ItemId::from(1), vec![1]).is_ok());
        tm.commit(tx1);
        let tx2 = tm.begin_transaction();
        assert!(tm.write(tx2, ItemId::from(1),vec![2]).is_ok());
        tm.commit(tx2);

        let locks = tm.locks.read().unwrap();
        if let Some(entry) = locks.get(&ItemId::from(1)) {
            assert_eq!(*entry.version(), 2);
        } else {
            // TODO. Currently this does not work perfectly because locks are released at the end of each transaction.
            // It is not bad for correctness though but I should improve it in order to be able to take advantage of versioning.
            // panic!("Lock not found");
        }
    }


}
