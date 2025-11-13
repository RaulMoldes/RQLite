mod macros;

use crate::types::{Date, DateTime};

use crate::test_ordering;

// Tests Date
test_ordering!(
    test_date_ordering,
    Date,
    [
        Date::parse_iso("2025-01-03").unwrap(),
        Date::parse_iso("2025-01-01").unwrap(),
        Date::parse_iso("2025-01-02").unwrap()
    ],
    [
        Date::parse_iso("2025-01-01").unwrap(),
        Date::parse_iso("2025-01-02").unwrap(),
        Date::parse_iso("2025-01-03").unwrap()
    ]
);

test_ordering!(
    test_datetime_ordering,
    DateTime,
    [
        DateTime::parse_iso("2025-10-20T14:30:45").unwrap(),
        DateTime::parse_iso("2023-05-01T00:00:00").unwrap(),
        DateTime::parse_iso("2024-12-31T23:59:59").unwrap(),
        DateTime::parse_iso("2025-01-01T00:00:00").unwrap()
    ],
    [
        DateTime::parse_iso("2023-05-01T00:00:00").unwrap(),
        DateTime::parse_iso("2024-12-31T23:59:59").unwrap(),
        DateTime::parse_iso("2025-01-01T00:00:00").unwrap(),
        DateTime::parse_iso("2025-10-20T14:30:45").unwrap()
    ]
);
