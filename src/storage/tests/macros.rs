macro_rules! test_leaf_page_ops {
    (   $test_suffix:ident,
            $page_type:ty,
            $cell_type:ty,
            $key_type:ty,
            $page_variant:expr,
            $create_key:expr,
            $create_cell:expr
        ) => {
        paste::paste! {
            #[test]
            fn [<test_insert_and_find_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                let key = $create_key(1);
                let cell = $create_cell(key.clone());

                assert!(page.insert(cell).is_ok());
                assert_eq!(page.cell_count(), 1);
                assert!(page.find(&key).is_some());
            }

            #[test]
            fn [<test_insert_multiple_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                for i in 0..10 {
                    let key = $create_key(i);
                    let cell = $create_cell(key.clone());
                    assert!(page.insert( cell).is_ok());
                }

                assert_eq!(page.cell_count(), 10);
            }

            #[test]
            fn [<test_find_nonexistent_ $test_suffix>]() {
                let page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                let key = $create_key(999);
                assert!(page.find(&key).is_none());
            }

            #[test]
            fn [<test_remove_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                let key = $create_key(1);
                let cell = $create_cell(key.clone());

                page.insert(cell).unwrap();
                assert!(page.find(&key).is_some());

                let removed = page.remove(&key);
                assert!(removed.is_some());
                assert!(page.find(&key).is_none());
            }

            #[test]
            fn [<test_remove_nonexistent_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                let key = $create_key(999);
                assert!(page.remove(&key).is_none());
            }

            #[test]
            fn [<test_insert_ordered_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                let keys = vec![5, 2, 8, 1, 9, 3];
                for k in keys {
                    let key = $create_key(k);
                    let cell = $create_cell(key.clone());
                    page.insert(cell).unwrap();
                }

                for k in [1, 2, 3, 5, 8, 9] {
                    let key = $create_key(k);
                    assert!(page.find(&key).is_some());
                }
            }


        }
    };
}

// Macro for interior pages.
macro_rules! test_interior_page_ops {
    (
            $test_suffix:ident,
            $page_type:ty,
            $cell_type:ty,
            $key_type:ty,
            $page_variant:expr,
            $create_key:expr
        ) => {
        paste::paste! {
            #[test]
            fn [<test_insert_child_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                let key = $create_key(1);
                let child = PageId::from(10);
                let cell = $cell_type::new(
                    child, key
                );
                assert!(page.insert_child(cell).is_ok());
                assert_eq!(page.cell_count(), 1);
            }

            #[test]
            fn [<test_insert_multiple_children_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                for i in 0..10 {
                    let key = $create_key(i);
                    let child = PageId::from(i + 10);
                    let cell = $cell_type::new(
                    child, key
                );
                    assert!(page.insert_child(cell).is_ok());
                }

                assert_eq!(page.cell_count(), 10);
            }

            #[test]
            fn [<test_find_child_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                page.set_rightmost_child(PageId::from(999));

                let key = $create_key(5);
                let child = PageId::from(10);
                let cell = $cell_type::new(
                    child, key
                );
                page.insert_child(cell).unwrap();

                let found = page.find_child(&$create_key(3));
                assert_eq!(found, child);

                let found_right = page.find_child(&$create_key(10));
                assert_eq!(found_right, PageId::from(999));
            }



            #[test]
            fn [<test_rightmost_child_ $test_suffix>]() {
                let mut page = <$page_type>::create(
                    PageId::from(1),
                    4096,
                    $page_variant,
                    None,
                );

                assert!(page.get_rightmost_child().is_none());

                let child = PageId::from(999);
                page.set_rightmost_child(child);

                assert_eq!(page.get_rightmost_child(), Some(child));
            }
        }
    };
}
