use crate::{
     ObjectId, UInt64, database::errors::TransactionError, io::pager::SharedPager, transactions::worker::Worker, types::TransactionId
};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
    time::{Duration, Instant},
};

mod graph;
mod mem_table;
pub mod worker;
use graph::{WFGCycle, WaitForGraph};
use mem_table::{MemTable, TransactionTable};

pub type Version = u16;
pub type RowId = UInt64;
pub type LogicalId = (ObjectId, RowId);

#[derive(Clone, Copy, Debug)]
pub enum LockType {
    Shared,
    Exclusive,
}

#[derive(Debug)]
pub enum LockRequest {
    Acquire {
        tx: TransactionId,
        id: LogicalId,
        typ: LockType,
        reply: Sender<LockResponse>,
    },
    Release {
        tx: TransactionId,
        handle: TupleHandle,
    },
}

#[derive(Debug)]
pub enum LockResponse {
    Granted { handle: TupleHandle },
    Aborted { tx: TransactionId },
}

// Control channel between the lock manager and the transaction manager.
#[derive(Debug)]
pub enum ControlMessage {
    AbortTx(TransactionId),
}

#[derive(Debug)]
struct Waiter {
    tx_id: TransactionId,
    typ: LockType,
    reply: Sender<LockResponse>,
}

#[derive(Debug)]
struct LockEntry {
    handle: TupleHandle,
    holders: HashSet<TransactionId>,
    lock_type: Option<LockType>,
    waiters: VecDeque<Waiter>,
}

impl LockEntry {
    fn new(logical_id: LogicalId) -> Self {
        let handle = TupleHandle::new(logical_id);
        Self {
            handle,
            holders: HashSet::new(),
            lock_type: None,
            waiters: VecDeque::new(),
        }
    }

    fn can_grant(&self, typ: LockType, tx_id: TransactionId) -> bool {
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
fn build_wfg(locks: &HashMap<LogicalId, LockEntry>) -> WaitForGraph {
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
// Once all the depend-on transactions are visited, we can pop Tx1 from the [rec-stack], which will avObjectId identifying situations which do not happen to be a deadlock as so.
fn dfs(
    node: TransactionId,
    graph: &WaitForGraph,
    visited: &mut HashSet<TransactionId>,
    rec: &mut HashSet<TransactionId>,
    path: &mut Vec<TransactionId>,
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct TupleHandle {
    object_id: ObjectId,
    row_id: UInt64,
    version: Version,
}

impl TupleHandle {
    pub fn new(logical_id: LogicalId) -> Self {
        Self {
            object_id: logical_id.0,
            row_id: logical_id.1,
            version: 0,
        }
    }

    fn increment_version_counter(&mut self) {
        self.version = self.version.saturating_add(1);
    }

    fn id(&self) -> LogicalId {
        (self.object_id, self.row_id)
    }

    fn table(&self) -> &ObjectId {
        &self.object_id
    }

    fn row_id(&self) -> &RowId {
        &self.row_id
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
            let mut locks: HashMap<LogicalId, LockEntry> = HashMap::new();
            let mut last_deadlock_check = Instant::now();

            loop {
                // Waits for messages with a timeout to execute deadlock detection periodically.
                match lock_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(msg) => match msg {
                        LockRequest::Acquire {
                            tx,
                            id,
                            typ,
                            reply,
                        } => {
                            let entry = locks.entry(id).or_insert_with(|| LockEntry::new(id));

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
                                    handle: entry.handle.clone(),
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
                            if let Some(entry) = locks.get_mut(&handle.id()) {
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
                                                    handle: entry.handle.clone(),
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
                                        locks.remove(&handle.id());
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
pub struct TransactionMetadata {
    id: TransactionId,
    read_set: HashSet<TupleHandle>,
    write_set: HashSet<TupleHandle>,
    status: TxState,
}

impl TransactionMetadata {
    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn new() -> Self {
        TransactionMetadata {
            id: TransactionId::new(),
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

    pub fn create_transaction(&self) -> TransactionId {
        let tx = TransactionMetadata::new();
        let id = tx.id();
        self.txs.write().insert(id, tx);
        id
    }

    pub fn create_worker(&self, id: TransactionId, pager: SharedPager) -> Worker {
        Worker::for_transaction(id, pager)
    }

    // Releases the lock on the provided sender channel.
    // mpsc provides multi-producer  single consumer channels, so i think the best idea is to not hold a single sender since we might eventually want to run multiple threads on the same transaction.
    pub fn release_lock(
        &mut self,
        handle: TupleHandle,
        tx: TransactionId,
        lock_tx: Sender<LockRequest>,
    ) {
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
        tx_id: TransactionId,
        logical_id: LogicalId,
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
            id: logical_id,
            typ: LockType::Shared,
            reply: reply_tx,
        };
        lock_tx
            .send(req)
            .map_err(|_| TransactionError::Other("lock manager is unavailable".into()))?;

        match reply_rx.recv_timeout(self.max_wait) {
            Ok(LockResponse::Granted {
                handle: page_handle,
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
        tx_id: TransactionId,
        logical_id: LogicalId,
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
            id: logical_id,
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
        tx_id: TransactionId,
        lock_tx: Sender<LockRequest>,
    ) -> Result<(), TransactionError> {
        let mut tuples: Vec<TupleHandle> = Vec::new();

        // Lock the tx table
        {
            let mut txs = self.txs.write();
            // Mark committed and release locks
            if let Some(mut tx) = txs.remove(&tx_id) {
                for mut handle in tx.write_set.drain() {
                    handle.increment_version_counter();
                    tuples.push(handle)
                }

                tx.status = TxState::Release; // Ensure we enter the release state
                // send release for each page in read_set and write_set
                tuples.extend(tx.read_set.drain());
            } else {
                return Err(TransactionError::NotFound(tx_id));
            }
        };

        // tx table is unlocked then we can operate on the released locks.
        for handle in tuples.drain(..) {
            let _ = lock_tx.send(LockRequest::Release { tx: tx_id, handle });
        }

        Ok(())
    }

    pub fn abort(
        &self,
        tx_id: TransactionId,
        lock_tx: Sender<LockRequest>,
    ) -> Result<(), TransactionError> {
        let mut tuples = Vec::new();

        // Lock the tx table
        {
            let mut txs = self.txs.write();
            // Mark committed and release locks
            if let Some(mut tx) = txs.remove(&tx_id) {
                tx.status = TxState::Release; // Ensure we enter the release state
                // send release for each page in read_set and write_set
                tuples.extend(tx.read_set.drain().chain(tx.write_set.drain()));
            } else {
                return Err(TransactionError::NotFound(tx_id));
            }
        };

        // tx table is unlocked then we can operate on the released locks.
        for handle in tuples.drain(..) {
            let _ = lock_tx.send(LockRequest::Release { tx: tx_id, handle });
        }

        Ok(())
    }
}

unsafe impl Send for TransactionManager {}
unsafe impl Sync for TransactionManager {}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            control_rx: Arc::clone(&self.control_rx),
            txs: self.txs.clone(),
            max_wait: self.max_wait,
        }
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
        thread::spawn(move || {
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

                // Sleep briefly to avObjectId spinning
                thread::sleep(Duration::from_millis(1));
            }
        });
    }

    /// Starts a transaction on a new thread.
    pub fn run_transaction_async<F, T>(
        &self,
        pager: SharedPager,
        payload: F,
    ) -> thread::JoinHandle<Result<T, TransactionError>>
    where
        F: FnOnce(Worker, &TransactionManager, Sender<LockRequest>) -> Result<T, TransactionError>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let mgr = self.manager.clone();
        let sender = self.sender();

        thread::spawn(move || {
            let tx_id = mgr.create_transaction();
            let worker = mgr.create_worker(tx_id, pager);
            worker.borrow_mut().begin()?;
            let res = payload(worker.clone(), &mgr, sender.clone());

            match res {
                Ok(val) => {
                    worker.borrow_mut().commit()?;
                    let _ = mgr.commit(tx_id, sender);
                    worker.borrow_mut().end()?;
                    Ok(val)
                }
                Err(e) => {
                    worker.borrow_mut().rollback()?;
                    let _ = mgr.abort(tx_id, sender);
                    worker.borrow_mut().end()?;
                    Err(e)
                }
            }
        })
    }
}
