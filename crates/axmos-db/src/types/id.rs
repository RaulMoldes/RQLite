use crate::{
    types::sized_types::UInt64,
    id_type
};

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    sync::atomic::Ordering
};

id_type!(LogId, __GLOBAL_LOG_COUNT, "LogId");
id_type!(PageId, __GLOBAL_PAGE_COUNT, "PageId");
id_type!(TransactionId, __GLOBAL_TX_COUNT, "TransactionId");
id_type!(ObjectId, __GLOBAL_OBJ_COUNT, "ObjectId");

id_type!(WorkerId, __GLOBAL_WORKER_COUNT, "WorkerId");

id_type!(BlockId, __GLOBAL_BLOCK_COUNT, "BlockId");

pub fn initialize_block_count(val: u64) {
    __GLOBAL_BLOCK_COUNT.store(val, Ordering::Relaxed);
}

pub fn initialize_atomics() {
    __GLOBAL_LOG_COUNT.store(0, Ordering::Relaxed);
    __GLOBAL_PAGE_COUNT.store(0, Ordering::Relaxed);
    __GLOBAL_TX_COUNT.store(0, Ordering::Relaxed);
    __GLOBAL_OBJ_COUNT.store(0, Ordering::Relaxed);
    __GLOBAL_WORKER_COUNT.store(0, Ordering::Relaxed);
}

pub fn get_next_object() -> u64 {
    __GLOBAL_OBJ_COUNT.load(Ordering::Relaxed)
}

pub const PAGE_ZERO: PageId = PageId(0);
pub const META_TABLE_ROOT: PageId = PAGE_ZERO;
pub const META_INDEX_ROOT: PageId = PageId(1);



pub type RowId = UInt64;

/// Logical identifier for a tuple (table + row)
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy)]
pub struct LogicalId(ObjectId, RowId);

impl Display for LogicalId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
