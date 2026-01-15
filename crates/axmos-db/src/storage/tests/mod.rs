//! Tests intended to be executed with miri
//!
//! ```sh
//! cargo +nightly miri test
//! ```

#[allow(unused_imports)]
mod test_helpers {
    pub(crate) use crate::storage::cell::{CELL_HEADER_SIZE, OwnedCell, Slot};
    pub(crate) use crate::storage::core::buffer::MemBlock;
    pub(crate) use crate::storage::core::traits::{
        Allocatable, AvailableSpace, BtreeMetadata, BtreeOps, Buffer, Identifiable, WalOps,
        Writable,
    };
    pub(crate) use crate::storage::page::{
        BtreePage, BtreePageHeader, OverflowPage, OverflowPageHeader, PageZeroHeader,
    };
    pub(crate) use crate::storage::wal::{
        BlockHeader, BlockZero, BlockZeroHeader, OwnedRecord, RecordType, WAL_BLOCK_SIZE, WalBlock,
    };
    pub(crate) use crate::types::{ObjectId, PageId, TransactionId};
    pub(crate) use crate::{
        CELL_ALIGNMENT, matrix_tests, matrix_type_tests, param_tests, param_type_tests,
        param2_tests, param2_type_tests, param3_tests, record,
    };
}

use test_helpers::*;

#[macro_export]
macro_rules! test_suite {
    ($mod_name:ident { $($test_name:ident => $body:block),+ $(,)? }) => {
        #[allow(unused_imports)]
        mod $mod_name {
             use super::test_helpers::*;

            $(
                #[test]

                fn $test_name() $body
            )+
        }
    };
}

#[macro_export]
macro_rules! memblock_test_for_headers {
    ($test_fn:ident, $header:ident => [$($hdr:ty),+ $(,)?]) => {
        paste::paste! {
            $(
                #[test]
                fn [<$test_fn _ $header:snake>]() {
                    use super::test_helpers::*;
                    $test_fn::<$hdr>()
                }
            )+
        }
    };
}

#[macro_export]
macro_rules! memblock_test_for_sizes {
    ($test_fn:ident, $header:ty, $size:ident => [$($sz:expr),+ $(,)?]) => {
        paste::paste! {
            $(
                #[test]
                fn [<$test_fn _ $size _ $sz>]() {
                    use super::test_helpers::*;
                    $test_fn::<$header>($sz)
                }
            )+
        }
    };
}

#[macro_export]
macro_rules! page_test_for_sizes {
    ($test_fn:ident, $size:ident => [$($sz:expr),+ $(,)?]) => {
        paste::paste! {
            $(
                #[test]
                fn [<$test_fn _ $size _ $sz>]() {
                    use super::test_helpers::*;
                    $test_fn($sz)
                }
            )+
        }
    };
}

fn test_memblock_alloc_drop<H>() {
    const SIZE: usize = 4096;
    drop(MemBlock::<H>::new(SIZE));
}

fn test_memblock_read_write<H>() {
    const SIZE: usize = 4096;
    let mut b = MemBlock::<H>::new(SIZE);
    let d = b.data_mut();
    d[0] = 0xAB;
    d[d.len() - 1] = 0xEF;
    assert_eq!(b.data()[0], 0xAB);
    assert_eq!(b.data()[b.data().len() - 1], 0xEF);
}

fn test_memblock_clone_indep<H>() {
    const SIZE: usize = 4096;
    let mut a = MemBlock::<H>::new(SIZE);
    a.data_mut()[0] = 0xAA;
    let mut b = a.clone();
    b.data_mut()[0] = 0xBB;
    assert_eq!(a.data()[0], 0xAA);
    assert_eq!(b.data()[0], 0xBB);
}

fn test_memblock_non_null_roundtrip<H>() {
    const SIZE: usize = 4096;
    let b = MemBlock::<H>::new(SIZE);
    drop(unsafe { MemBlock::<H>::from_non_null(b.into_non_null()) });
}

fn test_memblock_capacity<H>() {
    const SIZE: usize = 4096;
    let b = MemBlock::<H>::new(SIZE);
    assert_eq!(b.size(), SIZE);
    assert_eq!(b.capacity(), SIZE - std::mem::size_of::<H>());
}

fn test_memblock_as_slice<H>() {
    const SIZE: usize = 4096;
    let mut b = MemBlock::<H>::new(SIZE);
    assert_eq!(b.as_ref().len(), SIZE);
    b.as_mut()[0] = 0xFF;
    assert_eq!(b.as_ref()[0], 0xFF);
}

fn test_memblock_size<H, S: BlockSize>() {
    let value = S::VALUE;
    let mut b = MemBlock::<H>::new(value);
    assert_eq!(b.size(), value);
    let d = b.data_mut();
    if !d.is_empty() {
        d[0] = 0xAA;
        d[d.len() - 1] = 0xBB;
    }
}

param_type_tests!(test_memblock_alloc_drop, header => [
    BtreePageHeader,
    PageZeroHeader,
    OverflowPageHeader,
    BlockHeader,
    BlockZeroHeader,
], miri_safe);

param_type_tests!(test_memblock_read_write, header => [
    BtreePageHeader,
    PageZeroHeader,
    OverflowPageHeader,
    BlockHeader,
    BlockZeroHeader,

], miri_safe);

param_type_tests!(test_memblock_clone_indep, header => [
    BtreePageHeader,
    PageZeroHeader,
    OverflowPageHeader,
    BlockHeader,
    BlockZeroHeader
], miri_safe);

param_type_tests!(test_memblock_non_null_roundtrip, header => [
    BtreePageHeader,
    PageZeroHeader,
    OverflowPageHeader,
    BlockHeader,
    BlockZeroHeader,
], miri_safe);

param_type_tests!(test_memblock_capacity, header => [
    BtreePageHeader,
    PageZeroHeader,
    OverflowPageHeader,
    BlockHeader,
    BlockZeroHeader,
], miri_safe);

param_type_tests!(test_memblock_as_slice, header => [
    BtreePageHeader,
    PageZeroHeader,
    OverflowPageHeader,
    BlockHeader,
    BlockZeroHeader,
], miri_safe);

/// We need this because of the way the macro is constructed.
trait BlockSize {
    const VALUE: usize;
}
struct BigSize;
struct MidSize;
struct SmallSize;

impl BlockSize for BigSize {
    const VALUE: usize = 16384;
}

impl BlockSize for MidSize {
    const VALUE: usize = 8192;
}

impl BlockSize for SmallSize {
    const VALUE: usize = 4096;
}

matrix_type_tests!(
    test_memblock_size,
    header => [
        BtreePageHeader,
        OverflowPageHeader,
        PageZeroHeader,
        BlockHeader,
    ],
    size => [SmallSize, MidSize, BigSize],
    miri_safe
);

fn test_memblock_cast<From, To>()
where
    From: 'static,
    To: 'static,
{
    let mut b = MemBlock::<From>::new(4096);
    b.data_mut()[50] = 0xDE;
    let c: MemBlock<To> = b.cast();
    assert_eq!(c.size(), 4096);
    assert!(c.as_ref().contains(&0xDE));
}
param2_type_tests!(test_memblock_cast, from, to => [
    (BtreePageHeader, OverflowPageHeader),
    (OverflowPageHeader, BtreePageHeader),

], miri_safe);

// Tests de Cell
fn test_cell_align(size: usize) {
    let c = OwnedCell::new(&vec![0xAB; size]);
    assert_eq!(c.total_size() % CELL_ALIGNMENT as usize, 0);
    assert_eq!(c.effective_data().len(), size);
}

param_tests!(test_cell_align, size => [
    1, 7, 8, 9, 15, 16, 17, 31, 63, 64, 65, 127, 128
], miri_safe);

fn test_cell_rt(size: usize) {
    let p: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let c = OwnedCell::new(&p);
    let mut buf = vec![0u8; c.total_size()];
    c.write_to(&mut buf);
    assert_eq!(c.effective_data(), &p[..]);
}

param_tests!(test_cell_rt, size => [
    1, 8, 16, 32, 64, 128, 256
], miri_safe);

// Tests misceláneos de Cell
test_suite!(cell_misc {
    create => {
        use super::test_helpers::*;
        let c = OwnedCell::new(&[1, 2, 3, 4, 5]);
        assert_eq!(c.effective_data(), &[1, 2, 3, 4, 5]);
        assert!(!c.is_overflow());
    },
    overflow => {
        use super::test_helpers::*;

        let c = OwnedCell::new_overflow(&[0xCC; 100], 2);
        assert!(c.is_overflow());
        assert_eq!(c.overflow_page(), Some(2));
    },
    write_to => {
        use super::test_helpers::*;
        let c = OwnedCell::new(&[1, 2, 3, 4]);
        let mut buf = vec![0u8; c.total_size()];
        assert_eq!(c.write_to(&mut buf), c.total_size());
    },
});

fn test_page_alloc(size: usize) {
    let p = BtreePage::alloc(1, size);
    assert!(p.is_empty());
    assert!(p.is_leaf());
}

fn test_page_multi(size: usize) {
    let mut p = BtreePage::alloc(1, size);
    for i in 0..10u8 {
        p.push(OwnedCell::new(&[i; 50])).unwrap();
    }
    for i in 0..10u8 {
        assert_eq!(p.cell(i as usize).effective_data()[0], i);
    }
}

fn test_page_insert_at(size: usize) {
    let mut p = BtreePage::alloc(1, size);
    for i in 0..3u8 {
        p.push(OwnedCell::new(&[i; 32])).unwrap();
    }
    p.insert(1, OwnedCell::new(&[0xFF; 32])).unwrap();
    assert_eq!(p.cell(1).effective_data()[0], 0xFF);
}

fn test_page_remove(size: usize) {
    let mut p = BtreePage::alloc(1, size);
    for i in 0..5u8 {
        p.push(OwnedCell::new(&[i; 32])).unwrap();
    }
    assert_eq!(p.remove(2).unwrap().effective_data()[0], 2);
    assert_eq!(p.cell(2).effective_data()[0], 3);
}

param_tests!(test_page_alloc, size => [4096, 8192, 16384], miri_safe);
param_tests!(test_page_multi, size => [4096, 8192, 16384], miri_safe);
param_tests!(test_page_insert_at, size => [4096, 8192, 16384], miri_safe);
param_tests!(test_page_remove, size => [4096, 8192, 16384], miri_safe);

test_suite!(pg_stress {
    sequential => {
        use super::test_helpers::*;
        let mut p = BtreePage::alloc(1, 8192);
        for i in 0u32..50 {
            if p.has_space_for(64 + CELL_HEADER_SIZE + 2) {
                p.push(OwnedCell::new(&[(i % 256) as u8; 64])).unwrap();
            }
        }
        let n = p.num_slots();
        let mut rm = 0u32;
        while p.num_slots() > n / 2 {
            p.remove(0).unwrap();
            rm += 1;
        }
        for i in 0..rm {
            if p.has_space_for(64 + CELL_HEADER_SIZE + 2) {
                p.push(OwnedCell::new(&[((0xF0u32 + i) % 256) as u8; 64])).unwrap();
            }
        }
    },
    fill_refill => {
        use super::test_helpers::*;
        let mut p = BtreePage::alloc(1, 4096);
        while p.has_space_for(32 + CELL_HEADER_SIZE + 2) {
            p.push(OwnedCell::new(&[0xAA; 32])).unwrap();
        }
        let n = p.num_slots();
        while p.num_slots() > 0 {
            p.remove(0).unwrap();
        }
        while p.has_space_for(32 + CELL_HEADER_SIZE + 2) {
            p.push(OwnedCell::new(&[0xBB; 32])).unwrap();
        }
        assert!(p.num_slots() >= n - 1);
    },
});

test_suite!(overflow_pg {
    alloc => {
        use super::test_helpers::*;
        assert!(OverflowPage::alloc(1, 4096).next().is_none());
    },
    to_overflow => {
        use super::test_helpers::*;
        let mut p = BtreePage::alloc(1, 4096);
        p.push(OwnedCell::new(&[1, 2, 3])).unwrap();
        let id = p.id();
        let o: OverflowPage = p.into();
        assert_eq!(o.page_number(), id);
    },
    to_page => {
        use super::test_helpers::*;
        let o = OverflowPage::alloc(1, 4096);
        let p: BtreePage = o.into();
        assert_eq!(p.num_slots(), 0);
    },
});

#[test]
fn test_wal_alloc_drop() {
    let alloc = WalBlock::alloc(1, WAL_BLOCK_SIZE as usize);
    drop(alloc);
}

#[test]
fn test_wal_push_read() {
    let alloc = WalBlock::alloc(1, WAL_BLOCK_SIZE as usize);
    let mut b = alloc;
    let (undo, redo) = ([0xAA; 64], [0xBB; 64]);

    let record = record!(update 1, 1, 1,1, &undo, &redo);
    let lsn = record.lsn();
    b.try_push(lsn, record).unwrap();
    let r = b.record(0);
    assert_eq!(r.undo_payload(), &undo);
    assert_eq!(r.redo_payload(), &redo);
}

test_suite!(wal_misc {
    fill_read => {
        use super::test_helpers::*;
        let mut b = WalBlock::alloc(1, WAL_BLOCK_SIZE as usize);
        let mut off = 0usize;
        let mut offs = Vec::new();
        for i in 0u32..20 {
            let r = record!(
                update i,
                i, i,i,
                &[(i % 256) as u8; 64],
                &[((i + 1) % 256) as u8; 64]
            );
            let sz = r.total_size();
            if b.available_space() < sz {
                break;
            }
            let lsn = r.lsn();
            b.try_push(lsn, r).unwrap();
            offs.push(off);
            off += sz;
        }
        for (i, &o) in offs.iter().enumerate() {
            assert_eq!(b.record(o as u64).undo_payload()[0], (i % 256) as u8);
        }
    },
    prev_lsn => {
        use super::test_helpers::*;
        let first = record!(begin 1, 1);
        let lsn = first.lsn();
        let second = record!(
            update 1, 1, 1,1, prev: Some(lsn),
            &[1],
            &[2]
        );
        assert_eq!(second.metadata().prev_lsn.unwrap(), lsn);
    },
});

test_suite!(wal_rec {
    create => {
        use super::test_helpers::*;
        let r = record!(
            update 1, 1, 1,1,
            &[1, 2, 3],
            &[4, 5, 6]
        );
        assert_eq!(r.undo_payload(), &[1, 2, 3]);
        assert_eq!(r.redo_payload(), &[4, 5, 6]);
    },
    alignment => {
        use super::test_helpers::*;
        for u in [0, 1, 7, 8, 15, 31, 63] {
            for r in [0, 1, 7, 8, 15, 31, 63] {
                let rec = record!(
                    update 1, 1, 1,1,
                    &vec![0; u],
                    &vec![0; r]
                );
                assert_eq!(rec.total_size() % 8, 0, "u={u}, r={r}");
            }
        }
    },
    write_to => {
        use super::test_helpers::*;
        let r = record!(
            update 1, 1, 1,1,
            &[0; 50],
            &[0; 50]
        );
        let mut buf = vec![0u8; r.total_size()];
        assert_eq!(r.write_to(&mut buf), r.total_size());
    },
    as_ref => {
        use super::test_helpers::*;
        let r = record!(
            update 1, 1, 1,1,
            &[1, 2],
            &[3, 4]
        );
        let rf = r.as_record_ref();
        assert_eq!(rf.undo_payload(), r.undo_payload());
    },
});
