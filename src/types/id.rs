use crate::id_type;
id_type!(LogId, __GLOBAL_LOG_COUNT, "LogId");
id_type!(PageId, __GLOBAL_PAGE_COUNT, "PageId");
id_type!(RowId, __GLOBAL_ROW_COUNT, "RowId");
id_type!(TxId, __GLOBAL_TX_COUNT, "TxId");
id_type!(OId, __GLOBAL_OBJ_COUNT, "OId");

impl PageId {
    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }
}

pub const PAGE_ZERO: PageId = PageId(0);
pub const META_TABLE_ROOT: PageId = PageId(1);
