// src/sql/executor/context.rs
use crate::{
    io::{
        logger::{Begin, Commit, Delete, End, Insert, Operation, Update},
        pager::{BtreeBuilder, SharedPager},
    },
    multithreading::coordinator::{Snapshot, TransactionHandle},
    schema::catalog::SharedCatalog,
    tree::{accessor::TreeReader, bplustree::Btree},
    types::{Lsn, ObjectId, PageId, RowId, TransactionId},
};
use std::cell::Cell;

use super::RuntimeResult;

/// Lightweight execution context for query operators.
#[derive(Clone)]
pub(crate) struct TransactionContext<Acc: TreeReader> {
    accessor: Acc,
    pager: SharedPager,
    catalog: SharedCatalog,
    handle: TransactionHandle,
    pub last_lsn: Cell<Option<Lsn>>,
}

impl<Acc> TransactionContext<Acc>
where
    Acc: TreeReader + Clone,
{
    pub(crate) fn new(
        accessor: Acc,
        pager: SharedPager,
        catalog: SharedCatalog,
        handle: TransactionHandle,
    ) -> RuntimeResult<Self> {
        let context = Self {
            accessor,
            pager,
            catalog,
            handle,
            last_lsn: Cell::new(None),
        };
        context.log_operation(Begin)?;
        Ok(context)
    }

    pub(crate) fn tid(&self) -> TransactionId {
        self.handle.id()
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        self.handle.snapshot()
    }

    pub(crate) fn handle(&self) -> &TransactionHandle {
        &self.handle
    }

    pub(crate) fn pre_commit(&self) -> RuntimeResult<()> {
        self.log_operation(Commit)?;
        Ok(())
    }

    pub(crate) fn pre_abort(&self) -> RuntimeResult<()> {
        self.log_operation(Commit)?;
        Ok(())
    }

    pub(crate) fn post_commit(&self) -> RuntimeResult<()> {
        self.log_operation(End)?;
        self.pager().write().flush_wal()?;
        Ok(())
    }

    pub(crate) fn end(&self) -> RuntimeResult<()> {
        self.log_operation(End)?;
        self.pager().write().flush_wal()?;
        Ok(())
    }

    fn log_operation<O: Operation>(&self, operation: O) -> RuntimeResult<()> {
        let prev_lsn = self.last_lsn.get();
        let new_last_lsn = self
            .pager
            .write()
            .push_to_log(operation, self.tid(), prev_lsn)?;
        self.last_lsn.set(Some(new_last_lsn));
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

    #[inline]
    pub(crate) fn catalog(&self) -> &SharedCatalog {
        &self.catalog
    }

    #[inline]
    pub(crate) fn pager(&self) -> &SharedPager {
        &self.pager
    }

    #[inline]
    pub(crate) fn accessor(&self) -> &Acc {
        &self.accessor
    }

    #[inline]
    pub(crate) fn accessor_mut(&mut self) -> &mut Acc {
        &mut self.accessor
    }

    /// Build a B-tree with the context's accessor
    pub(crate) fn build_tree(&self, root: PageId) -> Btree<Acc> {
        let builder = self.tree_builder();
        builder.build_tree_with_accessor(root, self.accessor.clone())
    }

    /// Create a tree builder configured with the pager
    pub(crate) fn tree_builder(&self) -> BtreeBuilder {
        let pager = self.pager.read();
        BtreeBuilder::new(pager.min_keys_per_page(), pager.num_siblings_per_side())
            .with_pager(self.pager.clone())
    }

    /// Commits the transaction: log commit, commit handle, end.
    pub(crate) fn commit_transaction(&self) -> RuntimeResult<()> {
        self.pre_commit()?;
        self.handle().commit()?;
        self.end()?;
        Ok(())
    }

    /// Aborts the transaction: log abort, abort handle, end.
    pub(crate) fn abort_transaction(&self) -> RuntimeResult<()> {
        self.pre_abort()?;
        self.handle().abort()?;
        self.end()?;
        Ok(())
    }

    /// Aborts silently, ignoring errors (for cleanup in Drop, error paths)
    pub(crate) fn abort_silent(&self) {
        let _ = self.pre_abort();
        let _ = self.handle().abort();
        let _ = self.end();
    }
}
