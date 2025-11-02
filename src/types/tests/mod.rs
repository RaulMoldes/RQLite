mod macros;

use crate::types::{
    varlen_types::VarlenType, Blob, Bool, Char, Date, DateTime, Int16, Int32, Int64, Int8, PageId,
    RowId, UInt16, UInt32, UInt64, UInt8,
};

use crate::{test_ordering, test_serializable};

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

// Tests RowId
test_serializable!(
    test_rowid_serializable,
    RowId,
    [
        RowId::from(1),
        RowId::from(3),
        RowId::from(2),
        RowId::from(0)
    ]
);

test_serializable!(
    test_pageid_serializable,
    PageId,
    [
        PageId::from(1),
        PageId::from(3),
        PageId::from(2),
        PageId::from(0)
    ]
);

test_serializable!(
    test_int32_serializable,
    Int32,
    [
        Int32::from(1),
        Int32::from(3),
        Int32::from(2),
        Int32::from(0)
    ]
);

test_serializable!(
    test_int64_serializable,
    Int64,
    [
        Int64::from(1i64),
        Int64::from(3i64),
        Int64::from(2i64),
        Int64::from(0i64)
    ]
);

test_serializable!(
    test_int16_serializable,
    Int16,
    [
        Int16::from(1i16),
        Int16::from(3i16),
        Int16::from(2i16),
        Int16::from(0i16)
    ]
);

test_serializable!(
    test_int8_serializable,
    Int8,
    [
        Int8::from(1i8),
        Int8::from(3i8),
        Int8::from(2i8),
        Int8::from(0i8)
    ]
);

test_serializable!(
    test_uint32_serializable,
    UInt32,
    [
        UInt32::from(1u32),
        UInt32::from(3u32),
        UInt32::from(2u32),
        UInt32::from(0u32)
    ]
);

test_serializable!(
    test_uint64_serializable,
    UInt64,
    [
        UInt64::from(1u64),
        UInt64::from(3u64),
        UInt64::from(2u64),
        UInt64::from(0u64)
    ]
);

test_serializable!(
    test_uint16_serializable,
    UInt16,
    [
        UInt16::from(1u16),
        UInt16::from(3u16),
        UInt16::from(2u16),
        UInt16::from(0u16)
    ]
);



test_serializable!(
    test_uint8_serializable,
    UInt8,
    [
        UInt8::from(1u8),
        UInt8::from(3u8),
        UInt8::from(2u8),
        UInt8::from(0u8)
    ]
);

test_serializable!(
    test_char_serializable,
    Char,
    [
        Char::from('a'),
        Char::from('b'),
        Char::from('c'),
        Char::from('d')
    ]
);

test_serializable!(test_bool_serializable, Bool, [Bool::TRUE, Bool::FALSE]);

test_serializable!(
    test_date_serializable,
    Date,
    [
        Date::parse_iso("2025-01-03").unwrap(),
        Date::parse_iso("2025-01-01").unwrap(),
        Date::parse_iso("2025-01-02").unwrap()
    ]
);

test_serializable!(
    test_datetime_serializable,
    DateTime,
    [
        DateTime::parse_iso("2025-10-20T14:30:45").unwrap(),
        DateTime::parse_iso("2023-05-01T00:00:00").unwrap(),
        DateTime::parse_iso("2024-12-31T23:59:59").unwrap(),
        DateTime::parse_iso("2025-01-01T00:00:00").unwrap()
    ]
);

test_serializable!(
    test_varlen_serializable,
    VarlenType,
    [
        VarlenType::from_raw_unchecked(&[0, 0, 0, 1, 1], 1, 2),
        VarlenType::from_raw_unchecked("Hello".as_ref(), 1, 2),
        VarlenType::from_raw_unchecked("Goodnight".as_ref(), 1, 2)

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
