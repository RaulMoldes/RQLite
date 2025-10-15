use crate::{IndexInteriorCell, IndexLeafCell, TableInteriorCell, TableLeafCell};

pub fn validate_index_leaf_cell(
    original: &IndexLeafCell,
    deserialized: &IndexLeafCell,
    idx: usize,
) {
    assert_eq!(
        original.payload.as_bytes(),
        deserialized.payload.as_bytes(),
        "Test case {}: payload mismatch",
        idx
    );
    assert_eq!(
        original.overflow_page, deserialized.overflow_page,
        "Test case {}: overflow_page mismatch",
        idx
    );
}

pub fn validate_index_interior_cell(
    original: &IndexInteriorCell,
    deserialized: &IndexInteriorCell,
    idx: usize,
) {
    assert_eq!(
        original.left_child_page, deserialized.left_child_page,
        "Test case {}: left_child_page mismatch",
        idx
    );
    assert_eq!(
        original.payload.as_bytes(),
        deserialized.payload.as_bytes(),
        "Test case {}: payload mismatch",
        idx
    );
    assert_eq!(
        original.overflow_page, deserialized.overflow_page,
        "Test case {}: overflow_page mismatch",
        idx
    );
}

pub fn validate_table_interior_cell(
    original: &TableInteriorCell,
    deserialized: &TableInteriorCell,
    idx: usize,
) {
    assert_eq!(
        original.left_child_page, deserialized.left_child_page,
        "Test case {}: left_child_page mismatch",
        idx
    );
    assert_eq!(
        original.key, deserialized.key,
        "Test case {}: key mismatch",
        idx
    );
}

pub fn validate_table_leaf_cell(
    original: &TableLeafCell,
    deserialized: &TableLeafCell,
    idx: usize,
) {
    assert_eq!(
        original.row_id, deserialized.row_id,
        "Test case {}: row_id mismatch",
        idx
    );
    assert_eq!(
        original.payload.as_bytes(),
        deserialized.payload.as_bytes(),
        "Test case {}: payload mismatch",
        idx
    );
    assert_eq!(
        original.overflow_page, deserialized.overflow_page,
        "Test case {}: overflow_page mismatch",
        idx
    );
}
