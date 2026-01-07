use parking_lot::RwLock;

// src/sql/executor/context.rs
use super::RuntimeResult;
use crate::{
    io::{
        logger::{Begin, Commit, Delete, End, Insert, Operation, Update},
        pager::{BtreeBuilder, SharedPager},
    },
    multithreading::coordinator::{Snapshot, TransactionHandle},
    schema::catalog::SharedCatalog,
    tree::{
        accessor::{BtreeReadAccessor, BtreeWriteAccessor},
        bplustree::Btree,
    },
    types::{ObjectId, PageId, RowId, TransactionId},
};
use std::sync::Arc;

/// Lightweight execution context for query operators.
pub(crate) struct TransactionContext {
    pager: SharedPager,
    catalog: SharedCatalog,
    handle: Arc<RwLock<TransactionHandle>>,
}

// Creates a cloned reference to the context.
impl Clone for TransactionContext {
    fn clone(&self) -> Self {
        Self {
            pager: self.pager().clone(),
            catalog: self.catalog.clone(),
            handle: Arc::clone(&self.handle),
        }
    }
}

pub(crate) struct TransactionLogger {
    tid: TransactionId,
    pager: SharedPager,
    last_lsn: Arc<RwLock<u64>>,
}

impl Clone for TransactionLogger {
    fn clone(&self) -> Self {
        Self {
            tid: self.tid,
            pager: self.pager.clone(),
            last_lsn: Arc::clone(&self.last_lsn),
        }
    }
}

impl TransactionLogger {
    pub(crate) fn new(xid: TransactionId, pager: SharedPager, last_lsn: u64) -> Self {
        Self {
            tid: xid,
            pager,
            last_lsn: Arc::new(RwLock::new(last_lsn)),
        }
    }
    pub(crate) fn log_commit(&self) -> RuntimeResult<()> {
        self.log_operation(Commit)?;
        Ok(())
    }

    pub(crate) fn log_abort(&self) -> RuntimeResult<()> {
        self.log_operation(Commit)?;
        Ok(())
    }

    pub(crate) fn log_begin(&self) -> RuntimeResult<()> {
        self.log_operation(Begin)?;
        Ok(())
    }

    pub(crate) fn log_end(&self) -> RuntimeResult<()> {
        self.log_operation(End)?;
        self.pager.write().flush_wal()?;
        Ok(())
    }

    fn log_operation<O: Operation>(&self, operation: O) -> RuntimeResult<()> {
        let new_last_lsn =
            self.pager
                .write()
                .push_to_log(operation, self.tid, Some(*self.last_lsn.read()))?;
        *self.last_lsn.write() = new_last_lsn;
        Ok(())
    }

    /// Log an insert operation
    pub(crate) fn log_insert(
        &self,
        oid: ObjectId,
        rowid: RowId,
        data: Box<[u8]>,
    ) -> RuntimeResult<()> {
        let insert = Insert::new(oid, rowid, data);
        self.log_operation(insert)
    }

    /// Log an update operation
    pub(crate) fn log_update(
        &self,
        oid: ObjectId,
        rowid: RowId,
        old_data: Box<[u8]>,
        new_data: Box<[u8]>,
    ) -> RuntimeResult<()> {
        let update = Update::new(oid, rowid, old_data, new_data);
        self.log_operation(update)
    }

    /// Log a delete operation
    pub(crate) fn log_delete(
        &self,
        oid: ObjectId,
        rowid: RowId,
        old_data: Box<[u8]>,
    ) -> RuntimeResult<()> {
        let delete = Delete::new(oid, rowid, old_data);
        self.log_operation(delete)
    }
}

impl TransactionContext {
    pub(crate) fn new(
        pager: SharedPager,
        catalog: SharedCatalog,
        handle: TransactionHandle,
    ) -> RuntimeResult<Self> {
        let context = Self {
            pager,
            catalog,
            handle: Arc::new(RwLock::new(handle)),
        };

        Ok(context)
    }

    /// Creates a new context with a handle that cannot be committed.
    pub(crate) fn create_child(&self) -> RuntimeResult<TransactionContext> {
        Self::new(
            self.pager().clone(),
            self.catalog().clone(),
            self.handle_cloned(),
        )
    }

    pub(crate) fn tid(&self) -> TransactionId {
        self.handle.read().id()
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        self.handle.read().snapshot().clone()
    }

    pub(crate) fn handle_cloned(&self) -> TransactionHandle {
        self.handle.read().clone() // Cloning the handle invalidates it access to the coordinator.
    }

    #[inline]
    pub(crate) fn catalog(&self) -> &SharedCatalog {
        &self.catalog
    }

    #[inline]
    pub(crate) fn pager(&self) -> &SharedPager {
        &self.pager
    }

    /// Build a read only btree
    pub(crate) fn build_tree(&self, root: PageId) -> Btree<BtreeReadAccessor> {
        let builder = self.tree_builder();
        builder.build_tree(root)
    }

    /// Build a mutable btree
    pub(crate) fn build_tree_mut(&self, root: PageId) -> Btree<BtreeWriteAccessor> {
        let builder = self.tree_builder();
        builder.build_tree_mut(root)
    }

    /// Create a tree builder configured with the pager
    pub(crate) fn tree_builder(&self) -> BtreeBuilder {
        let pager = self.pager.read();
        BtreeBuilder::new(pager.min_keys_per_page(), pager.num_siblings_per_side())
            .with_pager(self.pager.clone())
    }

    /// Commits the transaction: log commit, commit handle, end.
    pub(crate) fn commit_transaction(&self) -> RuntimeResult<()> {
        if let Some(mut h) = self.handle.try_write() {
            h.commit()?;
        }; // If we are not able to borrow the handle then we know there are other handles pointing to the same transaction so we should not commit yet
        Ok(())
    }

    /// Aborts the transaction: log commit, commit handle, end.
    pub(crate) fn abort_transaction(&self) -> RuntimeResult<()> {
        if let Some(mut h) = self.handle.try_write() {
            h.abort()?;
        };
        Ok(())
    }
}
