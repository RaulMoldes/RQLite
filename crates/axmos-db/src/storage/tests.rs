#[macro_export]
macro_rules! static_buffer_tests {
    (
        $name:ident,
        $meta_type:ty, // metadata type
        size = $size:expr,
        align = $align:expr
    ) => {
        paste::paste! {
            #[test]
            fn [<test_alloc_$name>]() {
                use std::mem;
                let buffer: BufferWithMetadata<$meta_type> =
                    BufferWithMetadata::new_unchecked($size, $align);

                assert_eq!(buffer.size(), $size);
                assert_eq!(buffer.alignment(), $align);

                let expected_usable = ($size - mem::size_of::<$meta_type>()) as u32;
                assert_eq!(
                    BufferWithMetadata::<$meta_type>::usable_space($size),
                    expected_usable
                );

                // Metadata and data should be initialized to 0
                let meta_ptr = buffer.metadata();
                let meta_bytes = unsafe {
                    std::slice::from_raw_parts(
                        meta_ptr as *const _ as *const u8,
                        mem::size_of::<$meta_type>(),
                    )
                };
                assert!(meta_bytes.iter().all(|&b| b == 0), "metadata not zeroed");

                assert!(buffer.data().iter().all(|&b| b == 0), "data not zeroed");
            }


            #[test]
            #[should_panic(expected = "buffer overflow")]
            fn [<test_buffer_overflow_$name>]() {
                let mut buf: BufferWithMetadata<$meta_type> =
                    BufferWithMetadata::new_unchecked($size, $align);
                let cap = buf.capacity();
                buf.push_bytes(&vec![0xAB; cap]);
                buf.push_bytes(&[1]); // should panic
            }

        }
    };
}

#[macro_export]
macro_rules! dynamic_buffer_tests {
    (
        $name:ident,
        $meta_type:ty, // metadata type
        size = $size:expr,
        align = $align:expr
    ) => {
        paste::paste! {

            #[test]
            fn [<test_used_bytes_$name>]() {
                let mut buf: BufferWithMetadata<$meta_type> =
                    BufferWithMetadata::new_unchecked($size, $align);

                assert_eq!(buf.len(), 0);


                buf.push_bytes(&[1, 2, 3]);
                assert_eq!(buf.len(), 3);
                assert_eq!(buf.used(), &[1, 2, 3]);

                {
                    let used_mut = buf.used_mut();
                    used_mut[1] = 9;
                }

                assert_eq!(buf.used(), &[1, 9, 3]);
            }

            #[test]
            fn [<test_consistency_$name>]() {
                let mut buf: BufferWithMetadata<$meta_type> =
                    BufferWithMetadata::new_unchecked($size, $align);
                let cap = buf.capacity();
                assert_eq!(buf.used().len(), 0);
                buf.push_bytes(&[0xAB; 10]);
                assert_eq!(buf.capacity(), cap);
                assert_eq!(buf.len(), buf.used().len());
            }

            #[test]
            #[should_panic(expected = "buffer overflow")]
            fn [<test_buffer_overflow_$name>]() {
                let mut buf: BufferWithMetadata<$meta_type> =
                    BufferWithMetadata::new_unchecked($size, $align);
                let cap = buf.capacity();
                buf.push_bytes(&vec![0xAB; cap]);
                buf.push_bytes(&[1]); // should panic
            }


            #[test]
            fn [<test_grow_capacity_$name>]() {
                let mut buf: BufferWithMetadata<$meta_type> =
                    BufferWithMetadata::new_unchecked($size, $align);

                // Fill with some data
                buf.push_bytes(&[1, 2, 3, 4]);

                let old_len = buf.len();
                let old_used = buf.used().to_vec();

                // Grow capacity
                let new_capacity = buf.capacity() + 16;
                buf.grow(new_capacity);

                assert!(buf.capacity() >= new_capacity);
                assert_eq!(buf.len(), old_len);
                assert_eq!(buf.used(), old_used.as_slice());
            }


        }
    };
}

/// Macro to create page casting tests
#[macro_export]
macro_rules! test_page_casting {
    ($test_name:ident, $variant:ident, $from:ty, $to:ident, $header:ty) => {
        #[test]
        fn $test_name() -> std::io::Result<()> {
            let mut page = MemPage::$variant(<$from>::alloc(4096));

            // Reinit to new type
            page.reinit_as::<$header>();

            // Verify conversion
            let is_target = matches!(page, MemPage::$to(_));

            assert!(is_target, "Page should be converted to target type");

            Ok(())
        }
    };
}
