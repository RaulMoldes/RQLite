#[macro_export]
macro_rules! insert_tests {
    (
        $(
            $name:ident => {
                page_size: $page_size:expr,
                capacity: $capacity:expr,
                num_inserts: $num_inserts:expr,
                random: $random:expr
            }
        ),* $(,)?
    ) => {
        $(
            #[test]
            #[serial]
            fn $name() -> std::io::Result<()> {
                let mut tree = create_test_btree($page_size, $capacity, 3)?;
                let root = tree.get_root();
                let start = (root, Slot(0));
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
                for key_val in &keys {
                    let key = TestKey(*key_val);
                    tree.insert(root, key.as_ref())?;
                }

                std::fs::write("tree.json", tree.json()?)?;

                // Verify all keys exist (in sequential order)
                for i in 0..$num_inserts {
                    let key = TestKey(i);
                    let retrieved = tree.search(&start, &key, NodeAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::Found(_)));
                    let cell = tree.get_content_from_result(retrieved);
                    tree.clear_stack();
                    assert!(cell.is_some());
                    assert_eq!(cell.unwrap(), key.as_ref());
                }

                Ok(())
            }
        )*
    };
}

#[macro_export]
macro_rules! delete_test {
    ($name:ident, $page_size:expr, $cache_size:expr, $min_keys:expr, $num_keys:expr, $delete_sequences:expr) => {
        #[test]
        #[serial]
        fn $name() -> std::io::Result<()> {
            let mut tree = create_test_btree($page_size, $cache_size, $min_keys)?;
            let root = tree.get_root();
            let start = (root, Slot(0));
            // Insert all keys
            for i in 0..$num_keys {
                let key = TestKey(i as i32);
                tree.insert(root, key.as_ref())?;
            }

            // Check all keys exist
            for i in 0..$num_keys {
                let key = TestKey(i as i32);
                let retrieved = tree.search(&start, &key, NodeAccessMode::Read)?;
                assert!(matches!(retrieved, SearchResult::Found(_)));
                let cell = tree.get_content_from_result(retrieved);
                assert!(cell.is_some());
                assert_eq!(cell.unwrap(), key.as_ref());
                tree.clear_stack();
            }

            // Track deleted keys
            let mut deleted_keys = Vec::new();

            // Apply delete sequences
            for seq in $delete_sequences.iter() {
                for i in seq.clone().rev() {
                    let key = TestKey(i as i32);
                    tree.remove(root, &key)?;
                    deleted_keys.push(i);
                }
            }

            // Check remaining keys
            for i in 0..$num_keys {
                let key = TestKey(i as i32);
                if deleted_keys.contains(&i) {
                    // Key should be deleted
                    let retrieved = tree.search(&start, &key, NodeAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::NotFound(_)));
                } else {
                    // Key should still exist
                    let retrieved = tree.search(&start, &key, NodeAccessMode::Read)?;
                    assert!(matches!(retrieved, SearchResult::Found(_)));
                    let cell = tree.get_content_from_result(retrieved);
                    assert!(cell.is_some());
                    assert_eq!(cell.unwrap(), key.as_ref());
                }
                tree.clear_stack();
            }

            Ok(())
        }
    };
}
