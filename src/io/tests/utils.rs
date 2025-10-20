use crate::io::wal::{LogRecordType, WalEntry, WriteAheadLog};
use crate::serialization::Serializable;
use crate::types::{LogId, PageId, TxId, VarlenaType};
use std::marker::{Send, Sync};

#[derive(Default, Debug)]
pub struct TestPage {
    id: PageId,
}

impl TestPage {
    pub fn new(id: PageId) -> Self {
        Self { id }
    }
}

impl Serializable for TestPage {
    fn read_from<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let id = PageId::read_from(reader)?;
        Ok(TestPage { id })
    }

    fn write_to<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        self.id.write_to(writer)?;
        Ok(())
    }
}

unsafe impl Send for TestPage {}
unsafe impl Sync for TestPage {}

impl crate::HeaderOps for TestPage {
    fn cell_count(&self) -> u16 {
        0u16
    }

    fn free_space_start(&self) -> u16 {
        0u16
    }

    fn set_next_overflow(&mut self, _overflowpage: PageId) {}

    fn content_start(&self) -> u16 {
        0u16
    }

    fn free_space(&self) -> u16 {
        0u16
    }

    fn id(&self) -> PageId {
        self.id
    }

    fn is_overflow(&self) -> bool {
        false
    }

    fn page_size(&self) -> u16 {
        0u16
    }

    fn type_of(&self) -> crate::PageType {
        crate::PageType::Free
    }

    fn get_next_overflow(&self) -> Option<PageId> {
        None
    }

    fn set_type(&mut self, _page_type: crate::storage::PageType) {}

    fn set_cell_count(&mut self, _count: u16) {}

    fn set_content_start_ptr(&mut self, _content_start: u16) {}

    fn set_free_space_ptr(&mut self, _ptr: u16) {}
}

pub(crate) fn create_wal_entry(id: u32, txid: u32, undo_ct: &[u8], redo_ct: &[u8]) -> WalEntry {
    WalEntry {
        lsn: LogId::from(id),
        txid: TxId::from(txid),
        prev_lsn: LogId::from(id - 1),
        page_id: PageId::from(id), // Simulated page id
        log_type: LogRecordType::Update,
        slot_id: 1,
        undo_content: VarlenaType::from_raw_bytes(undo_ct, None),
        redo_content: VarlenaType::from_raw_bytes(redo_ct, None),
    }
}

pub(crate) fn gen_wal_entries(num_entries: u32, txid: u32) -> Vec<WalEntry> {
    (1..=num_entries)
        .map(|i| {
            create_wal_entry(
                i,
                txid,
                format!("old_value_{}", i).as_bytes(),
                format!("new_value_{}", i).as_bytes(),
            )
        })
        .collect()
}

pub(crate) fn validate_wal(mut wal: WriteAheadLog, entries: Vec<WalEntry>) {
    for (i, (expected, actual)) in entries.iter().zip(wal.into_iter()).enumerate() {
        assert_eq!(
            actual.as_ref().unwrap().lsn,
            expected.lsn,
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().txid,
            expected.txid,
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().prev_lsn,
            expected.prev_lsn,
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().page_id,
            expected.page_id,
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().log_type as u8,
            expected.log_type as u8,
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().slot_id,
            expected.slot_id,
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().undo_content.as_bytes(),
            expected.undo_content.as_bytes(),
            "Mismatch at entry {}",
            i + 1
        );
        assert_eq!(
            actual.as_ref().unwrap().redo_content.as_bytes(),
            expected.redo_content.as_bytes(),
            "Mismatch at entry {}",
            i + 1
        );
    }
}
