#[macro_export]
macro_rules! serialize_test {
    ($name:ident, $value:expr, $msg:expr) => {
        #[test]
        fn $name() {
            use std::io::Cursor;
            use $crate::database::schema::AsBytes;

            let original = $value;
            let mut buffer = Vec::new();
            original.write_to(&mut buffer).expect("Write failed");

            let mut cursor = Cursor::new(buffer);
            let deserialized = <_>::read_from(&mut cursor).expect("Read failed");

            assert_eq!(
                original, deserialized,
                concat!("Round-trip serialization failed: ", $msg)
            );
        }
    };
}
