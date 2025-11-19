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
