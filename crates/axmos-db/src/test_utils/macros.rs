#[macro_export]
macro_rules! insert_tests {
    (
        $(
            $name:ident => {
                num_inserts: $num_inserts:expr,
                random: $random:expr
            }
        ),* $(,)?
    ) => {
        $(
            #[test]
            #[serial]
            fn $name() -> std::io::Result<()> {
                let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
                let (mut tree, f) = test_btree(comparator)?;
                let root = tree.get_root();
                let start = Position::start_pos(root);
                let keys: Vec<i32> = if $random {
                    // Generate random insertion order
                    use rand::{seq::SliceRandom};
                    let mut keys: Vec<i32> = (0..$num_inserts).collect();
                    keys.shuffle(&mut rand_chacha::ChaCha20Rng::seed_from_u64(100));
                    keys
                } else {
                    // Sequential order
                    (0..$num_inserts).collect()
                };

                // Insert keys in specified order
                for (i, key_val) in keys.iter().enumerate() {

                    let key = TestKey::new(*key_val);
                    tree.insert(root, key)?;

                }


                // Verify all keys exist (in sequential order)
                for (i, k) in keys.iter().enumerate() {

                    let key = TestKey::new(*k);
                    let retrieved = tree.search(&start, key.key_bytes(), FrameAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::Found(_)));
                    let cell = tree.get_payload(retrieved)?;
                    tree.clear_accessor_stack();
                    assert!(cell.is_some());
                    assert_eq!(cell.unwrap().as_ref(), key.as_ref());
                }

                Ok(())
            }
        )*
    };
}

#[macro_export]
macro_rules! delete_test {
   (
        $(
            $name:ident => {
                num_keys: $num_keys:expr,
                sequences: $delete_sequences:expr
            }
        ),* $(,)?
    ) => {
         $(
        #[test]
        #[serial]
        fn $name() -> std::io::Result<()> {
            let comparator = FixedSizeBytesComparator::with_type::<TestKey>();
            let (mut tree, f) = test_btree(comparator)?;
            let root = tree.get_root();
            let start = Position::start_pos(root);
            // Insert all keys
            for i in 0..$num_keys {
                let key = TestKey::new(i as i32);
                tree.insert(root, key)?;
            }

            // Check all keys exist
            for i in 0..$num_keys {
                let key = TestKey::new(i as i32);
                let retrieved = tree.search(&start, key.key_bytes(), FrameAccessMode::Read)?;
                assert!(matches!(retrieved, SearchResult::Found(_)));
                let cell = tree.get_payload(retrieved)?;
                assert!(cell.is_some());
                assert_eq!(cell.unwrap().as_ref(), key.as_ref());
                tree.clear_accessor_stack();
            }

            // Track deleted keys
            let mut deleted_keys = Vec::new();

            // Apply delete sequences
            for seq in $delete_sequences.iter() {
                for i in seq.clone().rev() {
                    let key = TestKey::new(i as i32);
                    tree.remove(root, key.key_bytes())?;
                    deleted_keys.push(i);
                }
            }

            // Check remaining keys
            for i in 0..$num_keys {
                let key = TestKey::new(i as i32);
                if deleted_keys.contains(&i) {
                    // Key should be deleted
                    let retrieved = tree.search(&start, key.key_bytes(), FrameAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::NotFound(_)));
                } else {
                    // Key should still exist
                    let retrieved = tree.search(&start, key.key_bytes(), FrameAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::Found(_)));
                    let cell = tree.get_payload(retrieved)?;
                    assert!(cell.is_some());
                    assert_eq!(cell.unwrap().as_ref(), key.as_ref());
                }
                tree.clear_accessor_stack();
            }

            Ok(())
        }

         )*
    };
}

/// Macro for testing comparators with different key types
#[macro_export]
macro_rules! assert_cmp {
    // Fixed size keys (TestKey style)
    ($comparator:expr, $lhs:expr, $rhs:expr, $expected:expr) => {
        assert_eq!(
            $comparator.compare($lhs.as_ref(), $rhs.as_ref())?,
            $expected,
            "Comparison failed: {:?} vs {:?}, expected {:?}",
            $lhs,
            $rhs,
            $expected
        );
    };
    // Variable length keys (TestVarLengthKey style)
    ($comparator:expr, $lhs:expr, $rhs:expr, $expected:expr, varlen) => {
        assert_eq!(
            $comparator.compare($lhs.as_bytes(), $rhs.as_bytes())?,
            $expected,
            "Comparison failed: {:?} vs {:?}, expected {:?}",
            $lhs.as_bytes(),
            $rhs.as_bytes(),
            $expected
        );
    };
    // Raw byte slices
    ($comparator:expr, $lhs:expr, $rhs:expr, $expected:expr, raw) => {
        assert_eq!(
            $comparator.compare($lhs, $rhs)?,
            $expected,
            "Comparison failed: {:?} vs {:?}, expected {:?}",
            $lhs,
            $rhs,
            $expected
        );
    };
}

/// Macro for testing range_bytes
#[macro_export]
macro_rules! assert_range {
    ($comparator:expr, $lhs:expr, $rhs:expr, $expected:expr, raw) => {
        let result = $comparator.range_bytes($lhs, $rhs)?;
        assert_eq!(
            &result[..],
            $expected,
            "Range failed: {:?} - {:?}, expected {:?}, got {:?}",
            $lhs,
            $rhs,
            $expected,
            &result[..]
        );
    };
    ($comparator:expr, $lhs:expr, $rhs:expr, empty) => {
        let result = $comparator.range_bytes($lhs.as_ref(), $rhs.as_ref())?;
        assert!(
            result.is_empty(),
            "Expected empty range for {:?} - {:?}, got {:?}",
            $lhs,
            $rhs,
            &result[..]
        );
    };
}

#[macro_export]
macro_rules! sql_test {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let parsed_result = $crate::test_utils::parse_sql($sql);
            assert!(parsed_result.is_ok(), "parsing failed for SQL: {}", $sql);
            assert_eq!(parsed_result.unwrap(), $expected);
        }
    };
}

#[macro_export]
macro_rules! wal_lifecycle_test {
    (
        name: $name:ident,
        tid: $tid:expr,
        oid: $oid:expr,
        batches: [ $( { count: $count:expr, undo_sizes: $undo:expr, redo_sizes: $redo:expr } ),* $(,)? ],
        $(read_ahead_multiplier: $read_ahead:expr,)?
    ) => {
        #[test]
        fn $name() {
            use tempfile::tempdir;

            let dir = tempdir().unwrap();
            let path = dir.path().join(concat!(stringify!($name), ".wal"));

            let tid: u64 = $tid;
            let oid: u64 = $oid;
            let read_ahead_multiplier: usize = wal_lifecycle_test!(@read_ahead $($read_ahead)?);

            let mut all_records_metadata: Vec<(u64, usize, usize)> = Vec::new();
            let mut seed_counter: u64 = 1;

            {
                let mut wal = WriteAheadLog::create(&path).unwrap();

                $(
                    // Push batch
                    let batch_count: usize = $count;
                    let undo_sizes: &[usize] = &$undo;
                    let redo_sizes: &[usize] = &$redo;

                    for i in 0..batch_count {
                        let undo_len = undo_sizes[i % undo_sizes.len()];
                        let redo_len = redo_sizes[i % redo_sizes.len()];
                        let seed = seed_counter;
                        seed_counter += 1;

                        let record = $crate::test_utils::make_test_record(seed, tid, oid, undo_len, redo_len);
                        wal.push(record).unwrap();
                        all_records_metadata.push((seed, undo_len, redo_len));
                    }

                    // Flush and validate
                    wal.flush().unwrap();

                    {
                        let stats = wal.stats();
                        let mut reader = WalReader::new(
                            &mut wal.file,
                            wal.block_size * read_ahead_multiplier,
                            wal.block_size,
                            stats.total_blocks,
                        ).unwrap();

                        for (s, u, r) in all_records_metadata.iter() {
                            let record_ref = reader.next_ref().unwrap()
                                .expect("expected record during batch validation");
                            $crate::test_utils::verify_record(&record_ref, *s, tid, *u, *r);
                        }

                        assert!(reader.next_ref().unwrap().is_none(), "unexpected extra records");
                    }
                )*

                let final_stats = wal.stats();
                assert_eq!(final_stats.total_entries, all_records_metadata.len() as u32);
            }

            // Reopen and validate all records
            {
                let mut wal = WriteAheadLog::open(&path).unwrap();
                let stats = wal.stats();

                assert_eq!(stats.total_entries, all_records_metadata.len() as u32);

                let mut reader = WalReader::new(
                    &mut wal.file,
                    wal.block_size * read_ahead_multiplier,
                    wal.block_size,
                    stats.total_blocks,
                ).unwrap();

                for (s, u, r) in all_records_metadata.iter() {
                    let record_ref = reader.next_ref().unwrap()
                        .expect("expected record after reopen");
                    $crate::test_utils::verify_record(&record_ref, *s, tid, *u, *r);
                }

                assert!(reader.next_ref().unwrap().is_none(), "unexpected extra records after reopen");
            }
        }
    };

    // Default read_ahead_multiplier
    (@read_ahead) => { 4 };
    (@read_ahead $val:expr) => { $val };
}

/// Macro for asserting tuple properties
#[macro_export]
macro_rules! assert_tuple {
    // Assert version
    ($tuple:expr, version == $expected:expr) => {
        assert_eq!($tuple.version(), $expected, "Tuple version mismatch");
    };

    // Assert num_keys
    ($tuple:expr, num_keys == $expected:expr) => {
        assert_eq!($tuple.num_keys(), $expected, "Tuple num_keys mismatch");
    };

    // Assert num_values
    ($tuple:expr, num_values == $expected:expr) => {
        assert_eq!($tuple.num_values(), $expected, "Tuple num_values mismatch");
    };

    // Assert key_bytes_len
    ($tuple:expr, key_bytes_len == $expected:expr) => {
        assert_eq!(
            $tuple.key_bytes_len(),
            $expected,
            "Tuple key_bytes_len mismatch"
        );
    };

    // Assert key at index equals value
    ($tuple:expr, key[$idx:expr] == $expected:expr) => {
        assert_eq!(
            $tuple.key($idx).unwrap().to_owned(),
            $expected,
            "Tuple key[{}] mismatch",
            $idx
        );
    };

    // Assert value at index equals expected
    ($tuple:expr, value[$idx:expr] == $expected:expr) => {
        assert_eq!(
            $tuple.values()[$idx],
            $expected,
            "Tuple value[{}] mismatch",
            $idx
        );
    };

    // Assert is_deleted
    ($tuple:expr, is_deleted == $expected:expr) => {
        assert_eq!($tuple.is_deleted(), $expected, "Tuple is_deleted mismatch");
    };

    // Assert is_visible
    ($tuple:expr, is_visible($xid:expr) == $expected:expr) => {
        assert_eq!(
            $tuple.is_visible($crate::types::TransactionId::from($xid as u64)),
            $expected,
            "Tuple visibility mismatch for xid {}",
            $xid
        );
    };
}

/// Macro for asserting TupleRef properties
#[macro_export]
macro_rules! assert_tuple_ref {
    // Assert version
    ($tuple_ref:expr, version == $expected:expr) => {
        assert_eq!($tuple_ref.version(), $expected, "TupleRef version mismatch");
    };

    // Assert key at index
    ($tuple_ref:expr, key[$idx:expr] == $expected:expr) => {
        assert_eq!(
            $tuple_ref.key($idx).unwrap().to_owned(),
            $expected,
            "TupleRef key[{}] mismatch",
            $idx
        );
    };

    // Assert value at index
    ($tuple_ref:expr, value[$idx:expr] == $expected:expr) => {
        assert_eq!(
            $tuple_ref.value($idx).unwrap().to_owned(),
            $expected,
            "TupleRef value[{}] mismatch",
            $idx
        );
    };

    // Assert value is null
    ($tuple_ref:expr, value[$idx:expr] is null) => {
        assert_eq!(
            $tuple_ref.value($idx).unwrap(),
            $crate::types::DataTypeRef::Null,
            "TupleRef value[{}] should be null",
            $idx
        );
    };

    // Assert version_xmin
    ($tuple_ref:expr, version_xmin($ver:expr) == $expected:expr) => {
        assert_eq!(
            $tuple_ref.version_xmin($ver).unwrap(),
            $crate::types::TransactionId::from($expected as u64),
            "TupleRef version_xmin({}) mismatch",
            $ver
        );
    };

    // Assert key_bytes equals
    ($tuple_ref:expr, key_bytes == $expected:expr) => {
        assert_eq!(
            $tuple_ref.key_bytes(),
            $expected,
            "TupleRef key_bytes mismatch"
        );
    };
}

/// Macro for asserting snapshot visibility
#[macro_export]
macro_rules! assert_visibility {
    // Tuple is visible with snapshot
    ($tuple:expr, $schema:expr, $snapshot:expr, visible) => {{
        use $crate::storage::cell::OwnedCell;
        use $crate::storage::tuple::TupleRef;

        let cell: OwnedCell = (&$tuple).into();
        let result = TupleRef::read_for_snapshot(cell.payload(), $schema, &$snapshot)
            .expect("Snapshot read failed");
        assert!(result.is_some(), "Tuple should be visible");
    }};

    // Tuple is not visible with snapshot
    ($tuple:expr, $schema:expr, $snapshot:expr, not_visible) => {{
        use $crate::storage::cell::OwnedCell;
        use $crate::storage::tuple::TupleRef;

        let cell: OwnedCell = (&$tuple).into();
        let result = TupleRef::read_for_snapshot(cell.payload(), $schema, &$snapshot)
            .expect("Snapshot read failed");
        assert!(result.is_none(), "Tuple should not be visible");
    }};

    // Tuple visible at specific version
    ($tuple:expr, $schema:expr, $snapshot:expr, version == $ver:expr) => {{
        use $crate::storage::cell::OwnedCell;
        use $crate::storage::tuple::TupleRef;

        let cell: OwnedCell = (&$tuple).into();
        let visible_version = TupleRef::find_visible_version(cell.payload(), $schema, &$snapshot)
            .expect("Find visible version failed");
        assert_eq!(visible_version, $ver, "Visible version mismatch");
    }};
}

/// Macro for asserting errors
#[macro_export]
macro_rules! assert_tuple_err {
    ($result:expr) => {
        assert!($result.is_err(), "Expected error but got Ok");
    };

    ($result:expr, $msg:expr) => {
        assert!($result.is_err(), "Expected error: {}", $msg);
    };
}
