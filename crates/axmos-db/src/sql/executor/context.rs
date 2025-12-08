// src/sql/executor/context.rs
use crate::{
    database::SharedCatalog,
    structures::{bplustree::BPlusTree, comparator::NumericComparator},
    transactions::{Snapshot, worker::Worker},
    types::{PageId, TransactionId},
};

use std::io::Result as IoResult;

/// Lightweight execution context for query operators.
/// Created by WorkerPool::execution_context()
#[derive(Clone)]
pub(crate) struct ExecutionContext {
    worker: Worker,
    catalog: SharedCatalog,
    snapshot: Snapshot,
    transaction_id: TransactionId,
}

impl ExecutionContext {
    pub(crate) fn new(
        worker: Worker,
        catalog: SharedCatalog,
        snapshot: Snapshot,
        transaction_id: TransactionId,
    ) -> Self {
        Self {
            worker,
            catalog,
            snapshot,
            transaction_id,
        }
    }

    #[inline]
    pub(crate) fn worker(&self) -> &Worker {
        &self.worker
    }

    #[inline]
    pub(crate) fn catalog(&self) -> &SharedCatalog {
        &self.catalog
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    #[inline]
    pub(crate) fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[inline]
    pub(crate) fn table(&self, root: PageId) -> IoResult<BPlusTree<NumericComparator>> {
        self.catalog.table_btree(root, self.worker.clone())
    }
}
