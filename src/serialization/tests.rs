#[macro_export]
macro_rules! test_serializable {
 ($test_name:ident, $type:ty, [$($value:expr),+]) => {
     #[test]
    fn $test_name() {
        use std::io::Cursor;
        use $crate::serialization::Serializable;
        let test_values: Vec<$type> = vec![$($value),+];

        for (idx, original) in test_values.iter().enumerate() {
                let mut buffer = Cursor::new(Vec::new());
                    original.clone().write_to(&mut buffer).expect(&format!("Test case {}: Failed to serialize", idx));

                buffer.set_position(0);

                let deserialized = <$type>::read_from(&mut buffer).expect(&format!("Test case {}: Failed to deserialize", idx));


                assert_eq!(
                        *original, deserialized,
                    "Error at item: {}, deserialized: {} does not match original: {}",
                         idx, deserialized, original
                );
            }
        }
    };
}

#[macro_export]
macro_rules! test_ordering {
    ($test_name:ident, $type:ty, [$($unsorted:expr),+], [$($expected:expr),+]) => {
        #[test]
        fn $test_name() {
            let mut values: Vec<$type> = vec![$($unsorted),+];
            let expected: Vec<$type> = vec![$($expected),+];

            // Order the types
            values.sort();

            for (i, (actual, expected_val)) in values.iter().zip(expected.iter()).enumerate() {
                assert_eq!(
                    actual, expected_val,
                    "ERROR: Ordering mismatch at index {} — got {:?}, expected {:?}",
                    i, actual, expected_val
                );
            }
        }
    };
}
