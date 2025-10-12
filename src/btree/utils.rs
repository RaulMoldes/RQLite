use crate::storage::Cell;
use crate::SLOT_SIZE;

pub(crate) fn size_for_cell<BTreeCellType: Cell>(cell: &BTreeCellType) -> usize {
    cell.size() + SLOT_SIZE
}

pub(crate) fn compute_total_size<BTreeCellType: Cell>(cells: &[BTreeCellType]) -> usize {
    cells.iter().map(|c| SLOT_SIZE + c.size()).sum()
}

pub(crate) fn check_valid_size<BTreeCellType: Cell>(
    cells: &[BTreeCellType],
    max_payload_fraction: f32,
    min_payload_fraction: f32,
    page_size: usize,
) -> bool {
    let total_size: usize = cells.iter().map(|c| SLOT_SIZE + c.size()).sum();
    (total_size + crate::PAGE_HEADER_SIZE <= (max_payload_fraction * page_size as f32) as usize)
        && (total_size + crate::PAGE_HEADER_SIZE
            >= (min_payload_fraction * page_size as f32) as usize)
}

pub(crate) fn split_cells_by_size<BTreeCellType: Cell>(
    mut taken_cells: Vec<BTreeCellType>,
) -> (Vec<BTreeCellType>, Vec<BTreeCellType>) {
    taken_cells.sort_by_key(|cell| cell.key());
    let sizes: Vec<usize> = taken_cells.iter().map(|cell| cell.size()).collect();
    let total_size: usize = sizes.iter().sum();
    let half_size = total_size.div_ceil(2);

    // Look for the point where the accumulated result crosses the mid point.
    let mut acc = 0;
    let split_index = sizes
        .iter()
        .position(|&s| {
            acc += s;
            acc >= half_size
        })
        .unwrap_or(taken_cells.len().div_ceil(2));

    // Divide the vector
    let (left, right) = taken_cells.split_at(split_index);
    (left.to_vec(), right.to_vec())
}




/// Splits a collection of B-Tree cells into multiple groups with approximately equal total size.
/// This function takes a list of cells that implement the [`Cell`] trait and divides them into
/// `num_splits` groups such that the **sum of the sizes** of the cells in each group is roughly
/// balanced.
/// The number of groups returned will be between 1 and `num_splits`. If fewer cells are
/// provided than requested splits, each cell will form its own group.
/// The function aims for even **total size** distribution rather than equal **cell count**.
/// The result is suitable for balancing B-Tree pages or similar structures where
/// data size uniformity matters more than item count
pub(crate) fn split_cells_by_size_evenly<BTreeCellType: Cell + Clone>(
    mut taken_cells: Vec<BTreeCellType>,
    num_splits: usize,
) -> Vec<Vec<BTreeCellType>> {

    assert!(num_splits > 0, "num_splits must be positive");

    let total_cells = taken_cells.len();
    if num_splits == 1 {
        return vec![taken_cells];
    }

    if total_cells <= num_splits {
        eprintln!(
            "WARNING: requested {num_splits} splits but only {total_cells} cells available."
        );
        return taken_cells.into_iter().map(|c| vec![c]).collect();
    }

    // Order by key
    taken_cells.sort_by_key(|cell| cell.key());

    // Compute the required size per group
    let total_size: usize = taken_cells.iter().map(|c| c.size()).sum();
    let target_size = total_size.div_ceil(num_splits); // div_ceil

    let mut result = Vec::with_capacity(num_splits);
    let mut current_group = Vec::new();
    let mut current_size = 0usize;
    let mut splits_remaining = num_splits;

    for cell in taken_cells.into_iter() {
        let cell_size = cell.size();
        let should_close = splits_remaining > 1
            && current_size > 0
            && current_size + cell_size >= target_size;

        if should_close {
            result.push(std::mem::take(&mut current_group));
            current_size = 0;
            splits_remaining -= 1;
        }

        current_size += cell_size;
        current_group.push(cell);
    }

    if !current_group.is_empty() {
        result.push(current_group);
    }

    result
}
