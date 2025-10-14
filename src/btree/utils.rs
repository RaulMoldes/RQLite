use crate::io::cache::MemoryPool;
use crate::io::disk::FileOps;
use crate::storage::OverflowPage;
use crate::io::frames::{IOFrame, PageFrame};
use crate::io::pager::Pager;
use crate::storage::Cell;
use crate::types::PageId;
use crate::{PageType, SLOT_SIZE};

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

pub(crate) fn try_get_frame<P: Send + Sync, FI: FileOps, M: MemoryPool>(
    id: PageId,
    pager: &mut Pager<FI, M>,
) -> std::io::Result<PageFrame<P>>
where
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
{
    let buf = pager.get_single(id)?;
    PageFrame::try_from(buf)
}

pub(crate) fn try_get_overflow_frame<FI: FileOps, M: MemoryPool>(
    id: PageId,
    pager: &mut Pager<FI, M>,
) -> std::io::Result<PageFrame<OverflowPage>> {
    let buf = pager.get_single(id)?;
    PageFrame::<OverflowPage>::try_from(buf)
}

pub(crate) fn allocate_page<P: Send + Sync, FI: FileOps, M: MemoryPool>(
    page_type: PageType,
    pager: &mut Pager<FI, M>,
) -> std::io::Result<PageFrame<P>>
where
    PageFrame<P>: TryFrom<IOFrame, Error = std::io::Error>,
{
    PageFrame::try_from(pager.alloc_page(page_type)?)
}

// Linear partition the cell sizes using dynamic programming.
fn linear_partition_sizes(sizes: &[usize], k: usize) -> Vec<usize> {
    let n = sizes.len();
    if n == 0 {
        return vec![];
    }
    let k = std::cmp::min(k, n);

    // prefix sum: S[0] = 0, S[i] = sum sizes[0..i)
    let mut s = vec![0usize; n + 1];
    for i in 0..n {
        s[i + 1] = s[i] + sizes[i];
    }

    // dp[i][j] = minimal possible largest sum when partitioning first i elems into j groups
    // use 1-based for i and j to simplify indexing: i in 1..=n, j in 1..=k
    let mut dp = vec![vec![usize::MAX; k + 1]; n + 1];
    let mut div = vec![vec![0usize; k + 1]; n + 1]; // divider positions for reconstruction

    // base: j = 1 -> one group: sum up to i
    for i in 1..=n {
        dp[i][1] = s[i];
        div[i][1] = 0;
    }
    // base: i = 1 -> j groups -> only possible if j==1
    dp[1][1] = s[1];

    // fill DP
    for j in 2..=k {
        for i in 1..=n {
            if j > i {
                dp[i][j] = dp[i][i]; // more groups than items -> treat as i groups
                div[i][j] = i - 1;
                continue;
            }
            // try all possible previous cut positions p: 0..i-1
            // dp[i][j] = min_p max(dp[p][j-1], sum(p..i))
            let mut best = usize::MAX;
            let mut best_p = 0usize;
            // optimize: p should be at least j-1 - but we'll iterate
            for p in (j - 1)..=i - 1 {
                let left = dp[p][j - 1];
                let right = s[i] - s[p];
                let candidate = if left > right { left } else { right };
                if candidate < best {
                    best = candidate;
                    best_p = p;
                }
            }
            dp[i][j] = best;
            div[i][j] = best_p;
        }
    }

    // reconstruct cuts for n items and k groups
    let mut cuts: Vec<usize> = Vec::with_capacity(k);
    let mut i = n;
    let mut j = k;
    while j > 0 {
        let p = div[i][j];
        cuts.push(i); // group ends at i (exclusive end index)
        i = p;
        j -= 1;
        if i == 0 {
            // remaining groups are empty (shouldn't happen if k <= n)
            while j > 0 {
                cuts.push(0);
                j -= 1;
            }
            break;
        }
    }
    cuts.reverse();
    // `cuts` are the end indices for each group: e.g. [r1, r2, ..., rn] where groups are [0..r1), [r1..r2), ...
    cuts
}

/// Computes the optimal cell distribution in a set of groups, given each cell size,
/// page size and min and max payload fractions. Used during rebalancing specially after delete operations.
pub(crate) fn redistribute_cells<BTreeCellType: Cell + Clone>(
    cells: &mut [BTreeCellType],
    max_payload_fraction: f32,
    min_payload_fraction: f32,
    page_size: usize,
) -> Vec<Vec<BTreeCellType>> {
    let n = cells.len();
    if n == 0 {
        return vec![];
    }
    cells.sort_by_key(|c| c.key());
    let sizes: Vec<usize> = cells.iter().map(|c| c.size()).collect();
    let total: usize = sizes.iter().sum();
    let max_payload = (page_size as f32 * max_payload_fraction) as usize;
    let min_payload = (page_size as f32 * min_payload_fraction) as usize;

    // try k = 1..n, prefer smallest k that yields all groups between min..max
    let mut best_groups: Option<Vec<Vec<BTreeCellType>>> = None;
    let mut best_max_load = usize::MAX;

    for k in 1..=n {
        let cuts = linear_partition_sizes(&sizes, k);
        // build groups from cuts
        let mut groups_sizes = Vec::with_capacity(cuts.len());
        let mut groups_cells: Vec<Vec<BTreeCellType>> = Vec::with_capacity(cuts.len());
        let mut start = 0usize;
        for &end in &cuts {
            let mut grp = Vec::new();
            let mut grp_sum = 0usize;
            for idx in start..end {
                grp_sum += sizes[idx];
                grp.push(cells[idx].clone());
            }
            groups_sizes.push(grp_sum);
            groups_cells.push(grp);
            start = end;
        }
        // check constraints
        let mut ok = true;
        let mut max_load = 0usize;
        for &gs in &groups_sizes {
            if gs > max_load {
                max_load = gs;
            }
            if gs > max_payload || gs < min_payload {
                ok = false;
            }
        }

        if ok {
            // prefer the smallest k that satisfies constraints
            return groups_cells;
        }

        // keep track of best by minimizing the maximum load
        if max_load < best_max_load {
            best_max_load = max_load;
            best_groups = Some(groups_cells);
        }
    }

    // if no k satisfied the min/max constraints, return best found distribution (minimized max load)
    best_groups.unwrap_or_else(|| vec![cells.to_vec()])
}
