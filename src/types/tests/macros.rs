#[macro_export]
macro_rules! test_serializable {
 ($test_name:ident, $type:ty, [$($value:expr),+]) => {
     #[test]
    fn $test_name() {
        let test_values: Vec<$type> = vec![$($value),+];

        for (idx, original) in test_values.iter().enumerate() {
                let serialized: &[u8] = original.as_ref();
                let deserialized: $type = <$type>::try_from(serialized).unwrap();

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
