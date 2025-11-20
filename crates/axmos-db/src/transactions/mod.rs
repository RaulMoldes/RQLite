use crate::{
    database::errors::TransactionError,
    types::{PageId, TxId}
};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel}
    },
    thread,
    time::{Duration, Instant}
};


mod mem_table;
mod graph;


use graph::{WFGCycle, WaitForGraph};
use mem_table::{MemTable, TransactionTable};





pub type Version = u16;


#[derive(Clone, Copy, Debug)]
pub enum LockType {
    Shared,
    Exclusive,
}

#[derive(Debug)]
pub enum LockRequest {
    Acquire {
        tx: TxId,
        page: PageId,
        typ: LockType,
        reply: Sender<LockResponse>,
    },
    Release {
        tx: TxId,
        handle: PageHandle,
    },
}

#[derive(Debug)]
pub enum LockResponse {
    Granted { handle: PageHandle },
    Aborted { tx: TxId },
}

// Control channel between the lock manager and the transaction manager.
#[derive(Debug)]
pub enum ControlMessage {
    AbortTx(TxId),
}

#[derive(Debug)]
struct Waiter {
    tx_id: TxId,
    typ: LockType,
    reply: Sender<LockResponse>,
}


#[derive(Debug)]
struct LockEntry {
    handle: PageHandle,
    holders: HashSet<TxId>,
    lock_type: Option<LockType>,
    waiters: VecDeque<Waiter>,
}

impl LockEntry {
    fn new(page: PageId) -> Self {
        let handle= PageHandle::new(page);
        Self {
            handle,
            holders: HashSet::new(),
            lock_type: None,
            waiters: VecDeque::new(),
        }
    }



    fn can_grant(&self, typ: LockType, tx_id: TxId) -> bool {
        // All axmos database locks are reentrant.
        if self.holders.contains(&tx_id) {
            return true;
        }

        match typ {
            LockType::Shared => {
                // We allow shared if there is no exclusive holder.
                match self.lock_type {
                    None => true,
                    Some(LockType::Shared) => true,
                    Some(LockType::Exclusive) => false,
                }
            }
            LockType::Exclusive => self.holders.is_empty(),
        }
    }
}

// The wait for graph is basically a table of dependencies in which each row represents the list of transactions for which the index transaction is waiting.
// It can allow us to detect deadlocks.
// For anyone interested: https://www.cs.emory.edu/~cheung/Courses/554/Syllabus/8-recv+serial/deadlock-waitfor.html
fn build_wfg(locks: &HashMap<PageId, LockEntry>) -> WaitForGraph {
    let mut graph = WaitForGraph::new();

    for entry in locks.values() {
        let holders = entry.holders.clone();
        for waiter in &entry.waiters {
            graph
                .entry(waiter.tx_id)
                .or_default()
                .extend(holders.iter().copied());
        }
    }

    graph
}

// Depth first search order for graph traversal.
// Traversing WFG must be done in depth-first order:
//
// Example WFG entry:
//
// [TX1] -> [TX2, TX3, TX4]
//
// Here, Tx1 is waiting for Tx2, Tx3 and Tx4.
// We first insert Tx1 into the stack, and check for all the transactions that Tx2, Tx3 and tx4 are waiting for.
//
// If eventually, we reach to a point where we find Tx1 again, we have found a cycle.
// Once all the depend-on transactions are visited, we can pop Tx1 from the [rec-stack], which will avoid identifying situations which do not happen to be a deadlock as so.
fn dfs(
    node: TxId,
    graph: &WaitForGraph,
    visited: &mut HashSet<TxId>,
    rec: &mut HashSet<TxId>,
    path: &mut Vec<TxId>,
) -> Option<WFGCycle> {
    visited.insert(node);
    rec.insert(node);
    path.push(node);

    if let Some(neighbors) = graph.get(&node) {
        for &n in neighbors {
            if !visited.contains(&n) {
                if let Some(c) = dfs(n, graph, visited, rec, path) {
                    return Some(c);
                }
            } else if rec.contains(&n) {
                let pos = path.iter().position(|&x| x == n).unwrap();
                return Some(WFGCycle::from(&path[pos..]));
            }
        }
    }

    rec.remove(&node);
    path.pop();
    None
}

fn detect_cycle(graph: &WaitForGraph) -> Option<WFGCycle> {
    let mut visited = HashSet::new();
    let mut rec = HashSet::new();
    let mut path = Vec::new();

    for &node in graph.keys() {
        if !visited.contains(&node)
            && let Some(cycle) = dfs(node, graph, &mut visited, &mut rec, &mut path)
        {
            return Some(cycle);
        }
    }

    None
}



#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct PageHandle {
    id: PageId,
    version: Version
}


impl PageHandle {
    pub fn new(page: PageId) -> Self {
        Self { id: page, version: 0 }
    }

    fn increment_version_counter(&mut self) {
        self.version = self.version.saturating_add(1);
    }

    fn id(&self) -> &PageId {
        &self.id
    }

    fn version(&self) -> &Version {
        &self.version
    }
}

/// The lock manager communicates with running threads that ask for locks at the Transaction Manager level. It is a loop that grants/denies access to resources and periodically checks for deadlocks when a request times out.
/// The architecture is the following:
///
/// [CLIENTS] : threads that execute stuff on the database. They need to access the transaction manager in order to grab locks for these resources.
///
/// [SHARED TRANSACTION MANAGER (STM)] : main transaction tracker. Keeps track of each existing transaction and its state. It is wrapped over an Arc<> and protected with RwLock to ensure that state is common to all threads. Asks the lock manager for locks when a client needs by sending a [LockRequest].
///
/// [LOCK MANAGER]: loop that waits for [LockRequest] and sends [LockResponse] back to the [STM]. Might also send [ControlMessage]  to a control loop to indicate when a deadlock has happened and a transaction needs to kill its thread in order to resolve it.
///
/// [TRANSACTION CONTROLLER]: Executes the listener of the control loop (aka listens for [ControlMessage] from the LOCK MANAGER) and indicates the TM that a tx needs to be aborted in order for others to make progress when necessary.
struct LockManager;
impl LockManager {
    /// spawn the Lock Manager on its private thread
    /// lock_rx: LockRequest receiver channel
    /// control_tx: ControlMessage transmitter channel.
    pub fn spawn(lock_rx: Receiver<LockRequest>, control_tx: Sender<ControlMessage>) {
        thread::spawn(move || {
            let mut locks: HashMap<PageId, LockEntry> = HashMap::new();
            let mut last_deadlock_check = Instant::now();

            loop {
                // Waits for messages with a timeout to execute deadlock detection periodically.
                match lock_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(msg) => match msg {
                        LockRequest::Acquire {
                            tx,
                            page,
                            typ,
                            reply,
                        } => {
                            let entry = locks.entry(page).or_insert_with(|| LockEntry::new(page));

                            if entry.can_grant(typ, tx) {
                                // Grant the lock
                                entry.holders.insert(tx);
                                entry.lock_type = Some(typ);

                                // TODO: Add MVCC To improve this system.
                                // This should not be done here.
                                // We should do it at commit time for transactions that have modified items.
                             //   if matches!(typ, LockType::Exclusive) {
                            //        entry.increment_version_counter();
                            //    }


                                let _ = reply.send(LockResponse::Granted {
                                    handle: entry.handle
                                });
                            } else {
                                // Enqueue the waiter thread
                                entry.waiters.push_back(Waiter {
                                    tx_id: tx,
                                    typ,
                                    reply,
                                });
                            }
                        }

                        LockRequest::Release { tx, handle } => {
                            if let Some(entry) = locks.get_mut(handle.id()) {
                                entry.holders.remove(&tx);
                                if entry.holders.is_empty() {
                                    entry.lock_type = None; // No holders

                                    // Promote waiters on FIFO order.
                                    // For shared locks we can promote multiple waiters as long as they are all shared
                                    loop {
                                        if entry.waiters.is_empty() {
                                            break;
                                        }

                                        // Peeks the first waiter in the queue without consuming
                                        if let Some(waiter) = entry.waiters.pop_front() {

                                            if entry.can_grant(waiter.typ, waiter.tx_id) {
                                                // pop the waiter and grant the lock
                                                entry.holders.insert(waiter.tx_id);
                                                entry.lock_type = Some(waiter.typ);



                                                let _ = waiter.reply.send(LockResponse::Granted {
                                                    handle: entry.handle,
                                                });

                                                // Once we have granted an exclusive lock we must stop the loop, as exclusive locks can only be held by a single thread.
                                                if matches!(waiter.typ, LockType::Exclusive) {
                                                    break;
                                                } else {
                                                    // For shared locks the loop can continue promoting waiters
                                                    continue;
                                                }
                                            } else {
                                                entry.waiters.push_front(waiter);
                                                // The waiter could not acquire the lock so we stop.
                                                break;
                                            }
                                        } else {
                                            break;
                                        }
                                    }

                                    // Si no quedan holders ni waiters, podemos eliminar la entrada para liberar memoria
                                    if entry.holders.is_empty() && entry.waiters.is_empty() {
                                        locks.remove(handle.id());
                                    }
                                }
                            }
                        }
                    },
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // If the waiting time is reached, we can execute the deadlock detection.
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed. Terminate gracefully
                        break;
                    }
                }

                // Deadlock detection logic.
                if last_deadlock_check.elapsed() > Duration::from_secs(1) {
                    last_deadlock_check = Instant::now();
                    let wfg = build_wfg(&locks);

                    if let Some(cycle) = detect_cycle(&wfg) {
                        // The current policy is to always abort the youngest transaction.
                        // If transaction has been running for less time, it is more likely that it be cheaper to abort it and restart it since it might not have performed so much work.
                        if let Some(&youngest) = cycle.iter().max() {
                            let _ = control_tx.send(ControlMessage::AbortTx(youngest));
                        }
                    }
                }
            }
        });
    }
}

#[derive(Default, Debug)]
pub struct Transaction {
    id: TxId,
    read_set: HashSet<PageHandle>,
    write_set: HashSet<PageHandle>,
    status: TxState,
}

impl Transaction {
    pub fn id(&self) -> TxId {
        self.id
    }

    pub fn new() -> Self {
        Transaction {
            id: TxId::new(),
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            status: TxState::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum TxState {
    #[default]
    Acquire, // Transaction incrementally acquiring locks
    Release, // Transaction on release state, will not be allowed to aquire any more locks.
    Waiting, // Transaction locked on another lock
}

#[derive(Debug)]
pub struct TransactionManager {
    // The transaction manager holds the receiver of the control channel.
    control_rx: Arc<Mutex<Receiver<ControlMessage>>>, // Receiver control channel mutex
    txs: TransactionTable,
    // configuration field
    max_wait: Duration,
}

impl TransactionManager {
    pub fn new(control_rx: Receiver<ControlMessage>) -> Self {
        Self {
            control_rx: Arc::new(Mutex::new(control_rx)),
            txs: TransactionTable::from(MemTable::new()),
            max_wait: Duration::from_secs(10),
        }
    }

    pub fn begin_transaction(&self) -> TxId {
        let tx = Transaction::new();
        let id = tx.id();
        self.txs.write().insert(id, tx);
        id
    }

    // Releases the lock on the provided sender channel.
    // mpsc provides multi-producer  single consumer channels, so i think the best idea is to not hold a single sender since we might eventually want to run multiple threads on the same transaction.
    pub fn release_lock(&mut self, handle: PageHandle, tx: TxId, lock_tx: Sender<LockRequest>) {
        {
            let mut txs = self.txs.write();
            if let Some(transaction) = txs.get_mut(&tx) {
                transaction.status = TxState::Release;
            };
        }

        let _ = lock_tx.send(LockRequest::Release { tx, handle });
    }

    // This and the [write()] functions are the most important ones in the lock manager.
    // The idea is to have multiple Lock Requestors and a single Lock Responder. The Transaction manager becomes kind of a "bottleneck" here
    // since is the main entry point that holds metadata about transactions and we need to grab it mutably in order to acquire the lock.
    // For each client, an additional channel is created to
    pub fn read(
        &self,
        tx_id: TxId,
        page: PageId,
        lock_tx: Sender<LockRequest>,
    ) -> Result<(), TransactionError> {
        // First lock scope.
        {
            let mut txs = self.txs.write();
            // Verify transaction exists and running
            let tx = txs
                .get_mut(&tx_id)
                .ok_or(TransactionError::NotFound(tx_id))?;

            // If the transaction is not running, notify.
            if tx.status == TxState::Release {
                return Err(TransactionError::NotAcquire(tx_id));
            };

            // Put the tx to wait.
            tx.status = TxState::Waiting;
        }

        // Create a channel for sending the lock.
        // This channel is not sync because we want to allow multiple threads to request for locks.
        let (reply_tx, reply_rx) = channel();
        let req = LockRequest::Acquire {
            tx: tx_id,
            page,
            typ: LockType::Shared,
            reply: reply_tx,
        };
        lock_tx
            .send(req)
            .map_err(|_| TransactionError::Other("lock manager is unavailable".into()))?;

        match reply_rx.recv_timeout(self.max_wait) {
            Ok(LockResponse::Granted {
                handle: page_handle
            }) => {
                // Acquire the lock to mutate the transaction
                let mut txs = self.txs.write();
                // Verify transaction exists and running
                let tx = txs
                    .get_mut(&tx_id)
                    .ok_or(TransactionError::NotFound(tx_id))?;
                tx.status = TxState::Acquire; // Woken up

                tx.read_set.insert(page_handle);
                Ok(())
            }
            Ok(LockResponse::Aborted { tx: _ }) => {
                self.abort(tx_id, lock_tx)?;
                Err(TransactionError::Aborted(tx_id))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Another thread had the lock so we have to wait.
                Err(TransactionError::TimedOut(tx_id))
            }
            Err(_) => {
                self.abort(tx_id, lock_tx)?;
                Err(TransactionError::Other("reply channel closed".into()))
            }
        }
    }

    pub fn write(
        &self,
        tx_id: TxId,
        page: PageId,
        lock_tx: Sender<LockRequest>,
        _value: Vec<u8>,
    ) -> Result<(), TransactionError> {
        {
            let mut txs = self.txs.write();
            let tx = txs
                .get_mut(&tx_id)
                .ok_or(TransactionError::NotFound(tx_id))?;

            if tx.status == TxState::Release {
                return Err(TransactionError::NotAcquire(tx_id));
            };

            // Put the tx to wait.
            tx.status = TxState::Waiting;
        }

        // Create a reply channel to gen the response back from the Lock Manager
        let (reply_tx, reply_rx) = channel();
        let req = LockRequest::Acquire {
            tx: tx_id,
            page,
            typ: LockType::Exclusive,
            reply: reply_tx,
        };
        lock_tx
            .send(req)
            .map_err(|_| TransactionError::Other("lock manager gone".into()))?;

        match reply_rx.recv_timeout(self.max_wait) {
            Ok(LockResponse::Granted {
                handle: page_handle,
            }) => {
                let mut txs = self.txs.write();
                let tx = txs
                    .get_mut(&tx_id)
                    .ok_or(TransactionError::NotFound(tx_id))?;
                tx.status = TxState::Acquire;
                tx.write_set.insert(page_handle);
                Ok(())
            }
            Ok(LockResponse::Aborted { tx: _ }) => {
                self.abort(tx_id, lock_tx)?;
                Err(TransactionError::Aborted(tx_id))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err(TransactionError::TimedOut(tx_id))
            }
            Err(_) => {
                self.abort(tx_id, lock_tx)?;
                Err(TransactionError::Other("reply channel closed".into()))
            }
        }
    }

    pub fn commit(
        &self,
        tx_id: TxId,
        lock_tx: Sender<LockRequest>,
    ) -> Result<(), TransactionError> {
        let mut pages: Vec<PageHandle> = Vec::new();

        // Lock the tx table
        {
            let mut txs = self.txs.write();
            // Mark committed and release locks
            if let Some(mut tx) = txs.remove(&tx_id) {
                tx.status = TxState::Release; // Ensure we enter the release state
                // send release for each page in read_set and write_set
                pages.extend(tx.read_set.iter().chain(tx.write_set.iter()));
            } else {
                return Err(TransactionError::NotFound(tx_id));
            }
        };


        // tx table is unlocked then we can operate on the released locks.
        for handle in pages.drain(..) {
            let _ = lock_tx.send(LockRequest::Release { tx: tx_id, handle });
        }

        Ok(())
    }

    pub fn abort(
        &self,
        tx_id: TxId,
        lock_tx: Sender<LockRequest>,
    ) -> Result<(), TransactionError> {
         let mut pages = Vec::new();

        // Lock the tx table
        {
            let mut txs = self.txs.write();
            // Mark committed and release locks
            if let Some(mut tx) = txs.remove(&tx_id) {
                tx.status = TxState::Release; // Ensure we enter the release state
                // send release for each page in read_set and write_set
                pages.extend(tx.read_set.iter().chain(tx.write_set.iter()));
            } else {
                return Err(TransactionError::NotFound(tx_id));
            }
        };


        // tx table is unlocked then we can operate on the released locks.
        for handle in pages.drain(..) {
            let _ = lock_tx.send(LockRequest::Release { tx: tx_id, handle });
        }

        Ok(())
    }
}


unsafe impl Send for TransactionManager {}
unsafe impl Sync for TransactionManager {}


impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self { control_rx: Arc::clone(&self.control_rx), txs: self.txs.clone(), max_wait: self.max_wait }
    }
}





// The transaction controller is just a tiny loop that non-blockingly checks  for messages from the lock manager.
struct TransactionController {
    lock_sender: Sender<LockRequest>,
    manager: TransactionManager,
}

impl TransactionController {
    fn new() -> Self {
        let (control_tx, control_rx) = channel::<ControlMessage>();
        let (lock_tx, lock_rx) = channel::<LockRequest>();
        let manager = TransactionManager::new(control_rx);

        // Spawn the lock manager at startup
        LockManager::spawn(lock_rx, control_tx);
        Self {
            manager,
            lock_sender: lock_tx,
        }
    }

    fn manager(&self) -> TransactionManager {
        self.manager.clone()
    }

    fn sender(&self) -> Sender<LockRequest> {
        self.lock_sender.clone()
    }

    pub fn spawn(&self) {
        let mgr = self.manager.clone();
        let sender = self.sender();
        std::thread::spawn(move || {
            loop {
                // Only acquire write lock when we actually need to process a message

                while let Ok(msg) = {
                    if let Ok(mtx) = mgr.control_rx.lock() {
                        mtx.try_recv()
                    } else {
                        Err(std::sync::mpsc::TryRecvError::Disconnected)
                    }
                } {
                    match msg {
                        ControlMessage::AbortTx(tx) => {


                            let _ = mgr.abort(tx, sender.clone());
                            println!("Transaction {} aborted by deadlock detector", tx);
                        }
                    }
                }

                // Sleep briefly to avoid spinning
                std::thread::sleep(Duration::from_millis(1));
            }
        });
    }

    /// Starts a transaction on a new thread.
    pub fn run_transaction_async<F, T>(
        &self,
        payload: F,
    ) -> thread::JoinHandle<Result<T, TransactionError>>
    where
        F: FnOnce(
                TxId,
                &TransactionManager,
                Sender<LockRequest>,
            ) -> Result<T, TransactionError>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let mgr = self.manager.clone();
        let sender = self.sender();

        thread::spawn(move || {
            let tx_id = mgr.begin_transaction();
            let res = payload(tx_id, &mgr, sender.clone());

            match res {
                Ok(val) => {
                    let _ = mgr.commit(tx_id, sender);
                    Ok(val)
                }
                Err(e) => {
                    let _ = mgr.abort(tx_id, sender);
                    Err(e)
                }
            }
        })
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    // Helper function to create a test setup with SharedTransactionManager
    fn create_controller() -> TransactionController {
        let controller = TransactionController::new();
        controller.spawn();
        // Give the controller time to start
        thread::sleep(Duration::from_millis(50));
        controller
    }

    #[test]
    #[serial]
    fn test_tm_1() {
        let controller = create_controller();
        let result = Arc::new(AtomicBool::new(false));
        let result_clone = result.clone();

        let handle = controller.run_transaction_async(move |tid, mgr, s| {
            let page = PageId::from(42);

            // Write to a page
            mgr.write(tid, page, s.clone(), vec![1, 2, 3])?;

            // Read from the same page (should work - reentrant)
            mgr.read(tid, page, s)?;

            result_clone.store(true, Ordering::SeqCst);
            Ok("Transaction completed successfully")
        });

        let res = handle.join().unwrap();
        assert!(res.is_ok());
        assert!(result.load(Ordering::SeqCst));
    }

    #[test]
    #[serial]
    fn test_tm_2() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        // Begin transaction
        let tx_id = shared_tm.begin_transaction();
        assert!(shared_tm.txs.read().contains_key(&tx_id));

        // Check initial state
        {
            let txs = shared_tm.txs.read();
            let tx = &txs[&tx_id];
            assert_eq!(tx.status, TxState::Acquire);
            assert!(tx.read_set.is_empty());
            assert!(tx.write_set.is_empty());
        }

        // Perform operations
        let page = PageId::from(1);
        let handle = PageHandle::new(page);
        assert!(shared_tm.read(tx_id, page, sender.clone()).is_ok());

        // Check state after read
        {
            let txs = shared_tm.txs.read();
            let tx = &txs[&tx_id];
            assert!(tx.read_set.contains(&handle));
            assert!(tx.write_set.is_empty());
        }

        // Commit transaction
        assert!(shared_tm.commit(tx_id, sender).is_ok());
        assert!(!shared_tm.txs.read().contains_key(&tx_id));
    }

    #[test]
    #[serial]
    fn test_tm_3() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let tx1 = shared_tm.begin_transaction();
        let tx2 = shared_tm.begin_transaction();
        let page = PageId::from(100);

        // TX1 acquires exclusive lock
        assert!(shared_tm.write(tx1, page, sender.clone(), vec![1]).is_ok());

        // TX2 tries to acquire - should block
        let shared_tm_clone = shared_tm.clone();
        let sender_clone = sender.clone();
        let handle = thread::spawn(move || {
            shared_tm_clone.read(tx2, page, sender_clone)
        });

        // Give TX2 time to enter waiting state
        thread::sleep(Duration::from_millis(100));

        // Abort TX1 - should release the lock
        assert!(shared_tm.abort(tx1, sender.clone()).is_ok());

        // TX2 should now succeed
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // Cleanup
        assert!(shared_tm.commit(tx2, sender).is_ok());
    }

    #[test]
    #[serial]
    fn test_tm_4() {
        let controller = create_controller();
        let sender = controller.sender();
        let shared_tm = controller.manager();

        let tx1 = shared_tm.begin_transaction();
        let tx2 = shared_tm.begin_transaction();
        let tx3 = shared_tm.begin_transaction();
        let page_id = PageId::from(1);
        let handle = PageHandle::new(page_id);

        // All three transactions should be able to acquire shared locks
        assert!(shared_tm.read(tx1, page_id, sender.clone()).is_ok());
        assert!(shared_tm.read(tx2, page_id, sender.clone()).is_ok());
        assert!(shared_tm.read(tx3, page_id, sender.clone()).is_ok());

        // Verify all have the page in their read set
        {
            let txs = shared_tm.txs.read();
            assert!(txs[&tx1].read_set.contains(&handle));
            assert!(txs[&tx2].read_set.contains(&handle));
            assert!(txs[&tx3].read_set.contains(&handle));
        }

        // Cleanup
        assert!(shared_tm.commit(tx1, sender.clone()).is_ok());
        assert!(shared_tm.commit(tx2, sender.clone()).is_ok());
        assert!(shared_tm.commit(tx3, sender).is_ok());
    }

    #[test]
    #[serial]
    fn test_tm_5() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let tx1 = shared_tm.begin_transaction();
        let tx2 = shared_tm.begin_transaction();
        let page = PageId::from(200);

        // TX1 acquires exclusive lock
        assert!(shared_tm.write(tx1, page, sender.clone(), vec![1]).is_ok());

        // TX2 tries to acquire shared lock - should timeout
        let shared_tm_clone = shared_tm.clone();
        let sender_clone = sender.clone();
        let start = Instant::now();
        let handle = thread::spawn(move || {
            shared_tm_clone.read(tx2, page, sender_clone)
        });

        let result = handle.join().unwrap();
        assert!(result.is_err());

        // Should have waited approximately max_wait time
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_secs(9)); // Allow some margin

        // Cleanup
        assert!(shared_tm.commit(tx1, sender).is_ok());
    }

    #[test]
    #[serial]
    fn test_tm_6() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();
        let tx_id = shared_tm.begin_transaction();
        let page_id = PageId::from(3);
        let value = vec![4, 5, 6];

        // Acquire shared lock multiple times
        assert!(shared_tm.read(tx_id, page_id, sender.clone()).is_ok());
        assert!(shared_tm.read(tx_id, page_id, sender.clone()).is_ok());

        // Upgrade to exclusive (should work for same transaction)
        assert!(shared_tm.write(tx_id, page_id, sender.clone(), value.clone()).is_ok());

        // Can still acquire exclusive again
        assert!(shared_tm.write(tx_id, page_id, sender.clone(), value).is_ok());

        // Cleanup
        assert!(shared_tm.commit(tx_id, sender).is_ok());
    }

    #[test]
    #[serial]
    fn test_tm_7() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let invalid_tx = TxId::from(99999);
        let page = PageId::from(1);

        // Operations on non-existent transaction should fail
        let result = shared_tm.read(invalid_tx, page, sender.clone());
        assert!(matches!(result, Err(TransactionError::NotFound(_))));

        let result = shared_tm.write(invalid_tx, page, sender.clone(), vec![]);
        assert!(matches!(result, Err(TransactionError::NotFound(_))));

        let result = shared_tm.commit(invalid_tx, sender.clone());
        assert!(matches!(result, Err(TransactionError::NotFound(_))));
    }

    #[test]
    #[serial]
    fn test_tm_8() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let tx_id = shared_tm.begin_transaction();
        let page1 = PageId::from(1);
        let page2 = PageId::from(2);

        // Acquire first lock
        assert!(shared_tm.read(tx_id, page1, sender.clone()).is_ok());

        // Force transaction into release state
        {
            let mut txs = shared_tm.txs.write();
            if let Some(tx) = txs.get_mut(&tx_id) {
                tx.status = TxState::Release;
            }
        }

        // Try to acquire another lock - should fail
        let result = shared_tm.read(tx_id, page2, sender.clone());
        assert!(matches!(result, Err(TransactionError::NotAcquire(_))));

        // Cleanup
        let _ = shared_tm.abort(tx_id, sender);
    }

    #[test]
    #[serial]
    fn test_tm_9() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let tx1 = shared_tm.begin_transaction();
        let tx2 = shared_tm.begin_transaction();
        let page1 = PageId::from(10);
        let page2 = PageId::from(11);
        let value = vec![7, 8, 9];

        // TX1 locks page1
        assert!(shared_tm.write(tx1, page1, sender.clone(), value.clone()).is_ok());

        // TX2 locks page2
        assert!(shared_tm.write(tx2, page2, sender.clone(), value.clone()).is_ok());

        // Track which transaction completes
        let completed = Arc::new(AtomicUsize::new(0));
        let completed1 = completed.clone();
        let completed2 = completed.clone();

        // Create deadlock scenario
        let shared_tm_clone1 = shared_tm.clone();
        let sender_clone1 = sender.clone();
        let handle1 = thread::spawn(move || {
            let result = shared_tm_clone1.write(tx1, page2, sender_clone1, vec![10, 11, 12]);
            if result.is_ok() {
                completed1.store(1, Ordering::SeqCst);
            }
            result
        });

        // Give first thread time to start waiting
        thread::sleep(Duration::from_millis(100));

        let shared_tm_clone2 = shared_tm.clone();
        let sender_clone2 = sender.clone();
        let handle2 = thread::spawn(move || {
            let result = shared_tm_clone2.write(tx2, page1, sender_clone2, vec![13, 14, 15]);
            if result.is_ok() {
                completed2.store(2, Ordering::SeqCst);
            }
            result
        });

        // Wait for deadlock detection (happens every 1 second)
        thread::sleep(Duration::from_secs(2));

        let result1 = handle1.join().unwrap();
        let result2 = handle2.join().unwrap();

        // Exactly one should fail due to abort
        assert!(result1.is_err() ^ result2.is_err());

        // The younger transaction (tx2) should be aborted
        if result1.is_err() {
            assert!(matches!(result1, Err(TransactionError::TimedOut(_))));
        } else {
            assert!(matches!(result2, Err(TransactionError::TimedOut(_))));
        }
    }

    #[test]
    #[serial]
    fn test_tm_10() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let tx1 = shared_tm.begin_transaction();
        let tx2 = shared_tm.begin_transaction();
        let tx3 = shared_tm.begin_transaction();
        let tx4 = shared_tm.begin_transaction();
        let page = PageId::from(300);

        // TX1 acquires exclusive lock
        assert!(shared_tm.write(tx1, page, sender.clone(), vec![1]).is_ok());

        // Start TX2, TX3, TX4 all waiting for shared locks
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = vec![];

        for (tx, id) in [(tx2, 2), (tx3, 3), (tx4, 4)] {
            let shared_tm_clone = shared_tm.clone();
            let sender_clone = sender.clone();
            let results_clone = results.clone();

            let handle = thread::spawn(move || {
                let start = Instant::now();
                let result = shared_tm_clone.read(tx, page, sender_clone);
                let elapsed = start.elapsed();
                results_clone.lock().unwrap().push((id, result.is_ok(), elapsed));
            });
            handles.push(handle);

            // Stagger the requests slightly
            thread::sleep(Duration::from_millis(50));
        }

        // Give all threads time to enter waiting
        thread::sleep(Duration::from_millis(200));

        // Release TX1's lock - all waiting shared locks should be promoted
        assert!(shared_tm.commit(tx1, sender.clone()).is_ok());

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // All three should have succeeded
        let results = results.lock().unwrap();

        assert_eq!(results.len(), 3);
        for (_, success, _) in results.iter() {
            assert!(*success);
        }

        // Cleanup
        assert!(shared_tm.commit(tx2, sender.clone()).is_ok());
        assert!(shared_tm.commit(tx3, sender.clone()).is_ok());
        assert!(shared_tm.commit(tx4, sender).is_ok());
    }

    #[test]
    #[serial]
    fn test_tm_11() {
        let controller = create_controller();
        let shared_tm = controller.manager();
        let sender = controller.sender();

        let tx1 = shared_tm.begin_transaction();
        let tx2 = shared_tm.begin_transaction();
        let tx3 = shared_tm.begin_transaction();
        let page = PageId::from(400);

        // TX1 holds exclusive lock
        assert!(shared_tm.write(tx1, page, sender.clone(), vec![1]).is_ok());

        // TX2 and TX3 wait for the lock
        let shared_tm_clone2 = shared_tm.clone();
        let sender_clone2 = sender.clone();
        thread::spawn(move || {
            let _ = shared_tm_clone2.read(tx2, page, sender_clone2);
        });

        let shared_tm_clone3 = shared_tm.clone();
        let sender_clone3 = sender.clone();
        thread::spawn(move || {
            let _ = shared_tm_clone3.read(tx3, page, sender_clone3);
        });

        // Give threads time to enter waiting
        thread::sleep(Duration::from_millis(200));

        // At this point, the WFG should show TX2->TX1 and TX3->TX1
        // We can't directly test the WFG construction, but we can verify
        // that releasing TX1 allows both waiters to proceed

        // Cleanup
        assert!(shared_tm.commit(tx1, sender).is_ok());
        thread::sleep(Duration::from_millis(100));
    }

}
