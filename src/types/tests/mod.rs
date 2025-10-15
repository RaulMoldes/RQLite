use crate::types::{Byte, Float32, Float64, Key, PageId, RowId, Varint, VarlenaType};

use crate::{test_serializable, TextEncoding};
mod utils;

use utils::*;

test_serializable!(
    test_page_id,
    PageId,
    [
        PageId::new_key(),
        PageId::new_key(),
        PageId::new_key(),
        PageId::new_key(),
        PageId::new_key()
    ],
    validate_page_id
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
    ],
    validate_row_id
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
    ],
    validate_varint
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
    ],
    validate_f32
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
    ],
    validate_f64
);

test_serializable!(
    test_byte,
    Byte,
    [Byte(0), Byte(1), Byte(127), Byte(128), Byte(255)],
    validate_byte
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
    ],
    validate_varlena
);

test_serializable!(
    test_blob,
    VarlenaType,
    [
        VarlenaType::from_raw_bytes(&[12, 34, 56, 3], None),
        VarlenaType::from_raw_bytes(&[12, 34, 64, 4], None),
        VarlenaType::from_raw_bytes(&[12, 34, 64, 4, 45, 65, 76, 48], None)
    ],
    validate_varlena
);

#[test]
fn test_row_id_ordering() {
    let mut values = [
        RowId::from(1),
        RowId::from(3),
        RowId::from(2),
        RowId::from(0),
    ];
    let values_sorted = [
        RowId::from(0),
        RowId::from(1),
        RowId::from(2),
        RowId::from(3),
    ];
    values.sort();
    for (i, val) in values_sorted.iter().enumerate() {
        assert_eq!(values[i], *val, "ERROR: Row id ordering mismatch {}", i);
    }
}

#[test]
fn test_varlena_ordering() {
    let values = vec![5u32, 1, 9, 3, 7];
    let mut u32_sorted = values.clone();
    u32_sorted.sort();

    let mut varlena_vec: Vec<VarlenaType> = values
        .iter()
        .map(|v| VarlenaType::from_raw_bytes(&v.to_be_bytes(), None))
        .collect();
    varlena_vec.sort();

    for (i, val) in u32_sorted.iter().enumerate() {
        let expected_varlena = VarlenaType::from_raw_bytes(&val.to_be_bytes(), None);
        assert_eq!(varlena_vec[i], expected_varlena);
    }
}
