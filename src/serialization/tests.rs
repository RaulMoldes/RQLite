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
                    original.write_to(&mut buffer).expect(&format!("Test case {}: Failed to serialize", idx));

                buffer.set_position(0);

                let deserialized = <$type>::read_from(&mut buffer).expect(&format!("Test case {}: Failed to deserialize", idx));


                validate(original, &deserialized, idx);
            }
        }
    };
}



#[macro_export]
macro_rules! test_frame_conversion {
    (
        $test_name:ident,
        $source_frame:ty => $target_frame:ty,
        [$(($page_id:expr, $page_type:expr, $page_size:expr, $right_most:expr)),+],
        $validator:expr
    ) => {
        #[test]
        fn $test_name() {
            use $crate::serialization::Serializable;
            use $crate::io::{create_frame};
            use std::io::Cursor;

            let test_cases = vec![
                $(($page_id, $page_type, $page_size, $right_most)),+
            ];

            let validate: fn(&$target_frame, usize) = $validator;

            for (idx, (page_id, page_type, page_size, right_most)) in test_cases.into_iter().enumerate() {

                let io_frame = create_frame(page_id, page_type, page_size, right_most)
                    .expect(&format!("Test case {}: Failed to create IOFrame", idx));

                let mut buffer = Cursor::new(Vec::new());
                io_frame.write_to(&mut buffer)
                    .expect(&format!("Test case {}: Failed to serialize IOFrame", idx));

                buffer.set_position(0);

                let deserialized_io = <$source_frame>::read_from(&mut buffer)
                    .expect(&format!("Test case {}: Failed to deserialize IOFrame", idx));

                let converted_frame: $target_frame = deserialized_io.try_into()
                    .expect(&format!("Test case {}: Failed to convert frame", idx));


                validate(&converted_frame, idx);
            }
        }
    };
}
