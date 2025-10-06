use crate::types::{Byte, Float32, Float64, PageId, RowId, Varint, VarlenaType};

pub fn validate_varint(original: &Varint, deserialized: &Varint, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}

pub fn validate_page_id(original: &PageId, deserialized: &PageId, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}

pub fn validate_row_id(original: &RowId, deserialized: &RowId, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}

pub fn validate_f32(original: &Float32, deserialized: &Float32, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}

pub fn validate_f64(original: &Float64, deserialized: &Float64, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}

pub fn validate_byte(original: &Byte, deserialized: &Byte, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}

pub fn validate_varlena(original: &VarlenaType, deserialized: &VarlenaType, idx: usize) {
    assert_eq!(
        original, deserialized,
        "Error at item: {}, deserialized: {} does not match original: {}",
        idx, deserialized, original
    );
}
