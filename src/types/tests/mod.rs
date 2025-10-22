use crate::types::{
    Byte, Date, DateTime, Float32, Float64, Key, PageId, RowId, Varchar, Varint, VarlenaType,
};

use crate::{test_ordering, test_serializable, TextEncoding};

test_serializable!(
    test_page_id,
    PageId,
    [
        PageId::new_key(),
        PageId::new_key(),
        PageId::new_key(),
        PageId::new_key(),
        PageId::new_key()
    ]
);

test_serializable!(
    test_row_id,
    RowId,
    [
        RowId::new_key(),
        RowId::new_key(),
        RowId::new_key(),
        RowId::new_key(),
        RowId::new_key()
    ]
);

test_serializable!(
    test_varint,
    Varint,
    [
        Varint(0),
        Varint(127),
        Varint(128),
        Varint(300),
        Varint(163),
        Varint(-62)
    ]
);

test_serializable!(
    test_float32,
    Float32,
    [
        Float32(0.0),
        Float32(core::f32::consts::PI),
        Float32(-core::f32::consts::E),
        Float32(f32::MAX),
        Float32(f32::MIN),
        Float32(f32::INFINITY),
        Float32(f32::NEG_INFINITY)
    ]
);

test_serializable!(
    test_float64,
    Float64,
    [
        Float64(0.0),
        Float64(3.64848),
        Float64(-1.23456789),
        Float64(f64::MAX),
        Float64(f64::MIN)
    ]
);

test_serializable!(
    test_byte,
    Byte,
    [Byte(0), Byte(1), Byte(127), Byte(128), Byte(255)]
);

test_serializable!(
    test_string,
    VarlenaType,
    [
        VarlenaType::from_str("Hello", TextEncoding::Utf16be),
        VarlenaType::from_str("Hello", TextEncoding::Utf16le),
        VarlenaType::from_str("Hello", TextEncoding::Utf8),
        VarlenaType::from_str("My name is Berto", TextEncoding::Utf8),
        VarlenaType::from_str("NULL", TextEncoding::Utf8)
    ]
);

test_serializable!(
    test_blob,
    VarlenaType,
    [
        VarlenaType::from_raw_bytes(&[12, 34, 56, 3], None),
        VarlenaType::from_raw_bytes(&[12, 34, 64, 4], None),
        VarlenaType::from_raw_bytes(&[12, 34, 64, 4, 45, 65, 76, 48], None)
    ]
);

test_serializable!(
    test_date,
    Date,
    [
        Date::parse_iso("2025-10-20").unwrap(),
        Date::parse_iso("2025-10-20").unwrap(),
        Date::parse_iso("2025-10-20").unwrap()
    ]
);

test_serializable!(
    test_varchar,
    Varchar,
    [
        Varchar::from_str("Hello world", TextEncoding::Utf8),
        Varchar::from_str("Hello world 2", TextEncoding::Utf16le),
        Varchar::from_str("Hello world 3", TextEncoding::Utf16be)
    ]
);

// Tests RowId
test_ordering!(
    test_rowid_ordering,
    RowId,
    [
        RowId::from(1),
        RowId::from(3),
        RowId::from(2),
        RowId::from(0)
    ],
    [
        RowId::from(0),
        RowId::from(1),
        RowId::from(2),
        RowId::from(3)
    ]
);

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

// Tests VarlenaType
test_ordering!(
    test_varlena_ordering,
    VarlenaType,
    [
        VarlenaType::from_raw_bytes(&5u32.to_be_bytes(), None),
        VarlenaType::from_raw_bytes(&1u32.to_be_bytes(), None),
        VarlenaType::from_raw_bytes(&9u32.to_be_bytes(), None)
    ],
    [
        VarlenaType::from_raw_bytes(&1u32.to_be_bytes(), None),
        VarlenaType::from_raw_bytes(&5u32.to_be_bytes(), None),
        VarlenaType::from_raw_bytes(&9u32.to_be_bytes(), None)
    ]
);

test_serializable!(
    test_datetime_serialization,
    DateTime,
    [
        DateTime::from_seconds_since_epoch(0),
        DateTime::parse_iso("2025-01-01T00:00:00").unwrap(),
        DateTime::parse_iso("2025-10-20T14:30:45").unwrap(),
        DateTime::parse_iso("1999-12-31T23:59:59").unwrap()
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
