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
                    tree.insert(root, key.as_ref())?;

                }


                // Verify all keys exist (in sequential order)
                for (i, k) in keys.iter().enumerate() {

                    let key = TestKey::new(*k);
                    let retrieved = tree.search(&start, key.as_ref(), FrameAccessMode::Read)?;
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
                tree.insert(root, key.as_ref())?;
            }

            // Check all keys exist
            for i in 0..$num_keys {
                let key = TestKey::new(i as i32);
                let retrieved = tree.search(&start, key.as_ref(), FrameAccessMode::Read)?;
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
                    tree.remove(root, key.as_ref())?;
                    deleted_keys.push(i);
                }
            }

            // Check remaining keys
            for i in 0..$num_keys {
                let key = TestKey::new(i as i32);
                if deleted_keys.contains(&i) {
                    // Key should be deleted
                    let retrieved = tree.search(&start, key.as_ref(), FrameAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::NotFound(_)));
                } else {
                    // Key should still exist
                    let retrieved = tree.search(&start, key.as_ref(), FrameAccessMode::Read)?;
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

#[macro_export]
macro_rules! assert_cmp {
    ($comparator:expr, $lhs:expr, $rhs:expr, $expected:expr) => {
        assert_eq!(
            $comparator.compare($lhs.as_ref(), $rhs.as_ref())?,
            $expected
        );
    };
    ($comparator:expr, $lhs:expr, $rhs:expr, $expected:expr, varlen) => {
        assert_eq!(
            $comparator.compare(&$lhs.as_bytes(), &$rhs.as_bytes())?,
            $expected
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
