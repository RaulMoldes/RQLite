#[macro_export]
macro_rules! test_serializable {
 ($test_name:ident, $type:ty, [$($value:expr),+], $validator:expr) => {
     #[test]
    fn $test_name() {
        use std::io::Cursor;
        use $crate::serialization::Serializable;
        let test_values: Vec<$type> = vec![$($value),+];
        let validate: fn(&$type, &$type, usize) = $validator;
        for (idx, original) in test_values.iter().enumerate() {
                let mut buffer = Cursor::new(Vec::new());
                    original.clone().write_to(&mut buffer).expect(&format!("Test case {}: Failed to serialize", idx));

                buffer.set_position(0);

                let deserialized = <$type>::read_from(&mut buffer).expect(&format!("Test case {}: Failed to deserialize", idx));


                validate(original, &deserialized, idx);
            }
        }
    };
}
