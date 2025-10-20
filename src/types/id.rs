use crate::define_id_type;

define_id_type!(LogId, __GLOBAL_LOG_COUNT, "LogId");
define_id_type!(PageId, __GLOBAL_PAGE_COUNT, "PageId");
define_id_type!(RowId, __GLOBAL_ROW_COUNT, "RowId");
define_id_type!(TxId, __GLOBAL_TX_COUNT, "TxId");

impl PageId {
    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }
}
