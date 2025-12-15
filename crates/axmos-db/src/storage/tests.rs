//! Tests intended to be executed with miri
//!
//! ```sh
//! cargo +nightly miri test
//! ```

use crate::{
    CELL_ALIGNMENT,
    storage::{
        buffer::MemBlock,
        cell::{CELL_HEADER_SIZE, OwnedCell, Slot},
        page::{BtreePageHeader, OverflowPage, OverflowPageHeader, Page, PageZeroHeader},
        wal::{
            BlockHeader, BlockZero, BlockZeroHeader, OwnedRecord, RecordType, WAL_BLOCK_SIZE,
            WalBlock,
        },
    },
    types::{ObjectId, PageId, TransactionId},
};

/// Generates a module with named tests.
#[macro_export]
macro_rules! test_suite {
    ($mod_name:ident { $($test_name:ident => $body:block),+ $(,)? }) => {
        mod $mod_name {
            use super::*;
            $(
                #[test]
                fn $test_name() $body
            )+
        }
    };
}

/// Generates tests for a list of values.
#[macro_export]
macro_rules! test_for {
    ($prefix:ident, [$($val:expr),+], |$var:ident| $body:block) => {
        paste::paste! {
            $(
                #[test]
                fn [<$prefix _ $val>]() {
                    let $var = $val;
                    $body
                }
            )+
        }
    };
}

// Memblock test suite.
#[macro_export]
macro_rules! memblock_suite {
    ($header:ty, $mod_name:ident) => {
        mod $mod_name {
            use super::*;
            const SIZE: usize = 4096;


            /// Alloc and dealloc inmediately
            #[test] fn alloc_drop() { drop(MemBlock::<$header>::new(SIZE)); }


            /// Read and write data
            #[test] fn read_write() {
                let mut b = MemBlock::<$header>::new(SIZE);
                let d = b.data_mut();
                d[0] = 0xAB; d[d.len() - 1] = 0xEF;
                assert_eq!(b.data()[0], 0xAB);
                assert_eq!(b.data()[b.data().len() - 1], 0xEF);
            }

            /// Validate that clone creates a deep copy of the block.
            #[test] fn clone_indep() {
                let mut a = MemBlock::<$header>::new(SIZE);
                a.data_mut()[0] = 0xAA;
                let mut b = a.clone();
                b.data_mut()[0] = 0xBB;
                assert_eq!(a.data()[0], 0xAA);
                assert_eq!(b.data()[0], 0xBB);
            }


            /// Validate that from_non_null does not occassionate a double free
            #[test] fn non_null_roundtrip() {
                let b = MemBlock::<$header>::new(SIZE);
                drop(unsafe { MemBlock::<$header>::from_non_null(b.into_non_null()) });
            }


            /// Test the capacity method
            #[test] fn capacity() {
                let b = MemBlock::<$header>::new(SIZE);
                assert_eq!(b.size(), SIZE);
                assert_eq!(b.capacity(), SIZE - std::mem::size_of::<$header>());
            }


            /// Test as_ref implementation
            #[test] fn as_slice() {
                let mut b = MemBlock::<$header>::new(SIZE);
                assert_eq!(b.as_ref().len(), SIZE);
                b.as_mut()[0] = 0xFF;
                assert_eq!(b.as_ref()[0], 0xFF);
            }
        }
    };
    // Variant for multiple header types
    ($($header:ty => $mod_name:ident),+ $(,)?) => {
        $(memblock_suite!($header, $mod_name);)+
    };
}

/// Memblock casting tests (between headers)
#[macro_export]
macro_rules! memblock_cast {
    ($($from:ty => $to:ty : $name:ident),+ $(,)?) => {
        $(
            #[test] fn $name() {
                let mut b = MemBlock::<$from>::new(4096);
                b.data_mut()[50] = 0xDE;
                let c: MemBlock<$to> = b.cast();
                assert_eq!(c.size(), 4096);
                assert!(c.as_ref().contains(&0xDE));
            }
        )+
    };
}

/// Tests de MemBlock para múltiples tamaños.
#[macro_export]
macro_rules! memblock_sizes {
    ($header:ty, $prefix:ident, [$($size:expr),+]) => {
        paste::paste! {
            $(
                #[test] fn [<$prefix _ $size>]() {
                    let mut b = MemBlock::<$header>::new($size);
                    assert_eq!(b.size(), $size);
                    let d = b.data_mut();
                    if !d.is_empty() { d[0] = 0xAA; d[d.len() - 1] = 0xBB; }
                }
            )+
        }
    };
}

#[macro_export]
macro_rules! page_tests {
    ($($name:ident => |$page:ident| $body:block),+ $(,)?) => {
        paste::paste! {
            $(  #[allow(unused_mut)]
                #[test] fn [<$name _4k>]()  { let mut $page = Page::alloc(4096);  $body }
                #[allow(unused_mut)]
                #[test] fn [<$name _8k>]()  { let mut $page = Page::alloc(8192);  $body }
                #[allow(unused_mut)]
                #[test] fn [<$name _16k>]() { let mut $page = Page::alloc(16384); $body }
            )+
        }
    };
}

/// Generates tests for Cell for multiple sizes.
#[macro_export]
macro_rules! cell_tests {
    ($prefix:ident, [$($size:expr),+], |$s:ident| $body:block) => {
        paste::paste! {
            $( #[test] fn [<$prefix _ $size>]() { let $s: usize = $size; $body } )+
        }
    };
}

/// Suite of tests for [WalBlocks]
/// Uses the header parameter for block zero that has an anidated header.
#[macro_export]
macro_rules! wal_suite {
    ($alloc:expr, $mod_name:ident $(, header: $hdr:ident)?) => {
        mod $mod_name {
            use super::*;

            #[test] fn alloc_drop() { drop($alloc); }

            #[test] fn push_read() {
                let mut b = $alloc;
                let (undo, redo) = ([0xAA; 64], [0xBB; 64]);
                b.try_push(OwnedRecord::new(
                    TransactionId::new(), None, ObjectId::new(),
                    RecordType::Insert, &undo, &redo,
                )).unwrap();
                let r = b.record(0);
                assert_eq!(r.undo_payload(), &undo);
                assert_eq!(r.redo_payload(), &redo);
            }

            #[test] fn push_multi() {
                let mut b = $alloc;
                let lsns: Vec<_> = (0..10u8).map(|i| {
                    b.try_push(OwnedRecord::new(
                        TransactionId::new(), None, ObjectId::new(),
                        RecordType::Update, &[i; 32], &[i + 1; 32],
                    )).unwrap()
                }).collect();
                assert_eq!(b.metadata()$(.$hdr)?.block_first_lsn, Some(lsns[0]));
                assert_eq!(b.metadata()$(.$hdr)?.block_last_lsn, Some(lsns[9]));
            }

            #[test] fn space_track() {
                let mut b = $alloc;
                let init = b.available_space();
                let r = OwnedRecord::new(
                    TransactionId::new(), None, ObjectId::new(),
                    RecordType::Begin, &[], &[],
                );
                let sz = r.total_size();
                b.try_push(r).unwrap();
                assert_eq!(b.available_space(), init - sz);
            }


        }
    };
}

/// OwnedRecord test suite
#[macro_export]
macro_rules! record_suite {
    ($mod_name:ident) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn create() {
                let r = OwnedRecord::new(
                    TransactionId::new(),
                    None,
                    ObjectId::new(),
                    RecordType::Insert,
                    &[1, 2, 3],
                    &[4, 5, 6],
                );
                assert_eq!(r.undo_payload(), &[1, 2, 3]);
                assert_eq!(r.redo_payload(), &[4, 5, 6]);
            }

            #[test]
            fn alignment() {
                for u in [0, 1, 7, 8, 15, 31, 63] {
                    for r in [0, 1, 7, 8, 15, 31, 63] {
                        let rec = OwnedRecord::new(
                            TransactionId::new(),
                            None,
                            ObjectId::new(),
                            RecordType::Update,
                            &vec![0; u],
                            &vec![0; r],
                        );
                        assert_eq!(rec.total_size() % 64, 0, "u={u}, r={r}");
                    }
                }
            }

            #[test]
            fn write_to() {
                let r = OwnedRecord::new(
                    TransactionId::new(),
                    None,
                    ObjectId::new(),
                    RecordType::Abort,
                    &[0; 50],
                    &[0; 50],
                );
                let mut buf = vec![0u8; r.total_size()];
                assert_eq!(r.write_to(&mut buf), r.total_size());
            }

            #[test]
            fn as_ref() {
                let r = OwnedRecord::new(
                    TransactionId::new(),
                    None,
                    ObjectId::new(),
                    RecordType::End,
                    &[1, 2],
                    &[3, 4],
                );
                let rf = r.as_record_ref();
                assert_eq!(rf.undo_payload(), r.undo_payload());
            }

            #[test]
            fn all_types() {
                use RecordType::*;
                for t in [
                    Begin,
                    Commit,
                    Abort,
                    End,
                    Alloc,
                    Dealloc,
                    Update,
                    Delete,
                    Insert,
                    CreateTable,
                ] {
                    let r = OwnedRecord::new(
                        TransactionId::new(),
                        None,
                        ObjectId::new(),
                        t,
                        &[0],
                        &[0],
                    );
                    assert_eq!(r.metadata().log_type, t);
                }
            }
        }
    };
}

// MemBlock tests for all header variants as they are going to be used within the crate
memblock_suite!(
    BtreePageHeader   => mb_btree,
    PageZeroHeader    => mb_page_zero,
    OverflowPageHeader => mb_overflow,
    BlockHeader       => mb_block,
    BlockZeroHeader   => mb_block_zero,
    ()                => mb_unit,
);

// MemBlock casts between overflow and tree page
memblock_cast!(
    BtreePageHeader => OverflowPageHeader : cast_btree_overflow,
    OverflowPageHeader => BtreePageHeader : cast_overflow_btree,
);

// Memblock tests for multiple sizes
memblock_sizes!(BtreePageHeader, mb_btree_sz, [4096, 8192, 16384]);
memblock_sizes!(OverflowPageHeader, mb_overflow_sz, [4096, 8192, 16384]);
memblock_sizes!(PageZeroHeader, mb_btree_zero_sz, [4096, 8192, 16384]);
memblock_sizes!(BlockHeader, mb_block_sz, [4096, 8192, 40960]);

// Cell tests
cell_tests!(
    cell_align,
    [1, 7, 8, 9, 15, 16, 17, 31, 63, 64, 65, 127, 128],
    |sz| {
        let c = OwnedCell::new(&vec![0xAB; sz]);
        assert_eq!(c.total_size() % CELL_ALIGNMENT as usize, 0);
        assert_eq!(c.payload().len(), sz);
    }
);

cell_tests!(cell_rt, [1, 8, 16, 32, 64, 128, 256], |sz| {
    let p: Vec<u8> = (0..sz).map(|i| (i % 256) as u8).collect();
    let c = OwnedCell::new(&p);
    let mut buf = vec![0u8; c.total_size()];
    c.write_to(&mut buf);
    assert_eq!(c.payload(), &p[..]);
});

// Miscellaneous cell tests
test_suite!(cell_misc {
    create => {
        let c = OwnedCell::new(&[1, 2, 3, 4, 5]);
        assert_eq!(c.payload(), &[1, 2, 3, 4, 5]);
        assert!(!c.is_overflow());
    },
    overflow => {
        let id = PageId::new();
        let c = OwnedCell::new_overflow(&[0xCC; 100], id);
        assert!(c.is_overflow());
        assert_eq!(c.overflow_page(), Some(id));
    },
    write_to => {
        let c = OwnedCell::new(&[1, 2, 3, 4]);
        let mut buf = vec![0u8; c.total_size()];
        assert_eq!(c.write_to(&mut buf), c.total_size());
    },
});

// Page basic operations
page_tests!(
    pg_alloc => |p| {
        assert!(p.is_empty());
        assert!(p.is_leaf());
    },
    pg_multi => |p| {
        for i in 0..10u8 { p.push(OwnedCell::new(&[i; 50])); }
        for i in 0..10u8 { assert_eq!(p.cell(Slot(i as u16)).payload()[0], i); }
    },
    pg_insert_at => |p| {
        for i in 0..3u8 { p.push(OwnedCell::new(&[i; 32])); }
        p.insert(Slot(1), OwnedCell::new(&[0xFF; 32]));
        assert_eq!(p.cell(Slot(1)).payload()[0], 0xFF);
    },
    pg_remove => |p| {
        for i in 0..5u8 { p.push(OwnedCell::new(&[i; 32])); }
        assert_eq!(p.remove(Slot(2)).payload()[0], 2);
        assert_eq!(p.cell(Slot(2)).payload()[0], 3);
    },
    pg_replace_sm => |p| {
        p.push(OwnedCell::new(&[0xAA; 100]));
        p.replace(Slot(0), OwnedCell::new(&[0xBB; 50]));
        assert_eq!(p.cell(Slot(0)).len(), 50);
    },
    pg_replace_lg => |p| {
        p.push(OwnedCell::new(&[0xAA; 50]));
        p.push(OwnedCell::new(&[0xBB; 50]));
        p.replace(Slot(0), OwnedCell::new(&[0xCC; 100]));
        assert_eq!(p.cell(Slot(0)).payload()[0], 0xCC);
        assert_eq!(p.cell(Slot(1)).payload()[0], 0xBB);
    },
    pg_drain => |p| {
        for i in 0..10u8 { p.push(OwnedCell::new(&[i; 32])); }
        let d: Vec<_> = p.drain(3..6).collect();
        assert_eq!(d.len(), 3);
        assert_eq!(p.num_slots(), 7);
    },
    pg_cell_mut => |p| {
        p.push(OwnedCell::new(&[0; 64]));
        p.cell_mut(Slot(0)).data_mut()[0] = 0xFF;
        assert_eq!(p.cell(Slot(0)).data()[0], 0xFF);
    },
    pg_iter => |p| {
        for i in 0..5u8 { p.push(OwnedCell::new(&[i; 32])); }
        assert_eq!(p.iter_cells().count(), 5);
    },
);

// Stress tests for page
test_suite!(pg_stress {
    sequential => {
        let mut p = Page::alloc(8192);
        for i in 0u32..50 {
            if p.has_space_for(64 + CELL_HEADER_SIZE + Slot::SIZE) {
                p.push(OwnedCell::new(&[(i % 256) as u8; 64]));
            }
        }
        let n = p.num_slots();
        let mut rm = 0u32;
        while p.num_slots() > n / 2 { p.remove(Slot(0)); rm += 1; }
        for i in 0..rm {
            if p.has_space_for(64 + CELL_HEADER_SIZE + Slot::SIZE) {
                p.push(OwnedCell::new(&[((0xF0u32 + i) % 256) as u8; 64]));
            }
        }
    },
    fill_refill => {
        let mut p = Page::alloc(4096);
        while p.has_space_for(32 + CELL_HEADER_SIZE + Slot::SIZE) { p.push(OwnedCell::new(&[0xAA; 32])); }
        let n = p.num_slots();
        while p.num_slots() > 0 { p.remove(Slot(0)); }
        while p.has_space_for(32 + CELL_HEADER_SIZE + Slot::SIZE) { p.push(OwnedCell::new(&[0xBB; 32])); }
        assert!(p.num_slots() >= n - 1);
    },
});

// Overflow page
test_suite!(overflow_pg {
    alloc => { assert!(OverflowPage::alloc(4096).next().is_none()); },
    to_overflow => {
        let mut p = Page::alloc(4096);
        p.push(OwnedCell::new(&[1, 2, 3]));
        let id = p.page_number();
        let o: OverflowPage = p.into();
        assert_eq!(o.page_number(), id);
    },
    to_page => {
        let o = OverflowPage::alloc(4096);
        let p: Page = o.into();
        assert_eq!(p.num_slots(), 0);
    },
});

// WAL
wal_suite!(WalBlock::alloc(WAL_BLOCK_SIZE), wal_blk_non_zero);
wal_suite!(BlockZero::alloc(WAL_BLOCK_SIZE), wal_blk_zero, header: block_header);
record_suite!(wal_rec);

// WAL: misc
test_suite!(wal_misc {
    fill_read => {
        let mut b = WalBlock::alloc(WAL_BLOCK_SIZE);
        let mut off = 0usize;
        let mut offs = Vec::new();
        for i in 0u32..20 {
            let r = OwnedRecord::new(TransactionId::new(), None, ObjectId::new(),
                RecordType::Update, &[(i % 256) as u8; 64], &[((i+1) % 256) as u8; 64]);
            let sz = r.total_size();
            if b.available_space() < sz { break; }
            b.try_push(r).unwrap();
            offs.push(off);
            off += sz;
        }
        for (i, &o) in offs.iter().enumerate() {
            assert_eq!(b.record(o).undo_payload()[0], (i % 256) as u8);
        }
    },
    prev_lsn => {
        let tid = TransactionId::new();
        let first = OwnedRecord::new(tid, None, ObjectId::new(), RecordType::Begin, &[], &[]);
        let lsn = first.lsn();
        let second = OwnedRecord::new(tid, Some(lsn), ObjectId::new(), RecordType::Insert, &[1], &[2]);
        assert_eq!(second.metadata().prev_lsn.unwrap(), lsn);
    },
});
