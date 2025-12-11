// src/sql/executor/context.rs
use crate::{
    database::SharedCatalog,
    io::pager::SharedPager,
    structures::{bplustree::BPlusTree, comparator::NumericComparator},
    transactions::{Snapshot, accessor::RcPageAccessor, logger::Logger},
    types::{PageId, TransactionId},
};

use std::io::Result as IoResult;

/// Lightweight execution context for query operators.
#[derive(Clone)]
pub(crate) struct ExecutionContext {
    accessor: RcPageAccessor,
    logger: Logger,
    catalog: SharedCatalog,
    snapshot: Snapshot,
    transaction_id: TransactionId,
}

impl ExecutionContext {
    pub(crate) fn new(
        accessor: RcPageAccessor,
        pager: SharedPager,
        catalog: SharedCatalog,
        snapshot: Snapshot,
        transaction_id: TransactionId,
        logger: Logger,
    ) -> Self {
        Self {
            accessor,
            logger,
            catalog,
            snapshot,
            transaction_id,
        }
    }

    #[inline]
    pub(crate) fn accessor(&self) -> &RcPageAccessor {
        &self.accessor
    }

    #[inline]
    pub(crate) fn logger(&self) -> &Logger {
        &self.logger
    }

    #[inline]
    pub(crate) fn logger_mut(&mut self) -> &mut Logger {
        &mut self.logger
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
        self.catalog.table_btree(root, self.accessor.clone())
    }
}
