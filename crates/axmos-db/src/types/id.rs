use crate::id_type;
id_type!(LogId, __GLOBAL_LOG_COUNT, "LogId");
id_type!(PageId, __GLOBAL_PAGE_COUNT, "PageId");
id_type!(TransactionId, __GLOBAL_TX_COUNT, "TransactionId");
id_type!(ObjectId, __GLOBAL_OBJ_COUNT, "ObectId");

pub fn initialize_atomics() {
    __GLOBAL_LOG_COUNT.store(1, std::sync::atomic::Ordering::Relaxed);
    __GLOBAL_PAGE_COUNT.store(1, std::sync::atomic::Ordering::Relaxed);
    __GLOBAL_TX_COUNT.store(1, std::sync::atomic::Ordering::Relaxed);
    __GLOBAL_OBJ_COUNT.store(1, std::sync::atomic::Ordering::Relaxed);
}

pub fn get_next_object() -> u64 {
    __GLOBAL_OBJ_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}

impl PageId {
    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }
}

pub const PAGE_ZERO: PageId = PageId(0);
pub const OBJECT_ZERO: ObjectId = ObjectId(0);
pub const TRANSACTION_ZERO: TransactionId = TransactionId(0);
pub const META_TABLE_ROOT: PageId = PageId(1);
pub const META_INDEX_ROOT: PageId = PageId(2);
