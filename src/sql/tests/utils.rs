#[macro_export]
macro_rules! sql_test {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let parsed_result = parse_sql($sql);
            assert!(parsed_result.is_ok(), "Parsing failed for SQL: {}", $sql);
            assert_eq!(parsed_result.unwrap(), $expected);
        }
    };
}
