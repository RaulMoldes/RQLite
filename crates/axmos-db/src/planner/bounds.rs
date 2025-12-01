use std::{
    cmp::{self, Ordering},
    ops::Bound,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum IndexBound<T> {
    Start(Bound<T>),
    End(Bound<T>),
}

impl<T: PartialOrd> IndexBound<T> {
    pub fn as_bound(&self) -> &Bound<T> {
        match self {
            IndexBound::Start(bound) => bound,
            IndexBound::End(bound) => bound,
        }
    }
}

impl<T: PartialEq> PartialEq for IndexBound<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IndexBound::Start(a), IndexBound::Start(b)) => bounds_eq(a, b),
            (IndexBound::End(a), IndexBound::End(b)) => bounds_eq(a, b),
            _ => false,
        }
    }
}

impl<T: PartialOrd> PartialOrd for IndexBound<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (IndexBound::Start(a), IndexBound::Start(b)) => compare_start_bounds(a, b),
            (IndexBound::End(a), IndexBound::End(b)) => compare_end_bounds(a, b),
            (IndexBound::Start(start), IndexBound::End(end)) => {
                if matches!(start, Bound::Unbounded) {
                    return Some(Ordering::Less);
                }
                compare_mixed_bounds(start, end, false)
            }
            (IndexBound::End(end), IndexBound::Start(start)) => {
                if matches!(end, Bound::Unbounded) {
                    return Some(Ordering::Greater);
                }
                compare_mixed_bounds(start, end, true)
            }
        }
    }
}

impl<T: Ord> Ord for IndexBound<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<T: Eq> Eq for IndexBound<T> {}

// Helper functions for bound comparison
fn bounds_eq<T: PartialEq>(a: &Bound<T>, b: &Bound<T>) -> bool {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => true,
        (Bound::Included(a_val), Bound::Included(b_val)) => a_val == b_val,
        (Bound::Excluded(a_val), Bound::Excluded(b_val)) => a_val == b_val,
        _ => false,
    }
}

fn compare_start_bounds<T: PartialOrd>(a: &Bound<T>, b: &Bound<T>) -> Option<Ordering> {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Some(Ordering::Equal),
        (Bound::Unbounded, _) => Some(Ordering::Less),
        (_, Bound::Unbounded) => Some(Ordering::Greater),
        (Bound::Included(a_val), Bound::Included(b_val))
        | (Bound::Excluded(a_val), Bound::Excluded(b_val)) => a_val.partial_cmp(b_val),
        (Bound::Included(a_val), Bound::Excluded(b_val)) => {
            if a_val == b_val {
                Some(Ordering::Less)
            } else {
                a_val.partial_cmp(b_val)
            }
        }
        (Bound::Excluded(a_val), Bound::Included(b_val)) => {
            if a_val == b_val {
                Some(Ordering::Greater)
            } else {
                a_val.partial_cmp(b_val)
            }
        }
    }
}

fn compare_end_bounds<T: PartialOrd>(a: &Bound<T>, b: &Bound<T>) -> Option<Ordering> {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Some(Ordering::Equal),
        (Bound::Unbounded, _) => Some(Ordering::Greater),
        (_, Bound::Unbounded) => Some(Ordering::Less),
        (Bound::Included(a_val), Bound::Included(b_val))
        | (Bound::Excluded(a_val), Bound::Excluded(b_val)) => a_val.partial_cmp(b_val),
        (Bound::Included(a_val), Bound::Excluded(b_val)) => {
            if a_val == b_val {
                Some(Ordering::Less)
            } else {
                a_val.partial_cmp(b_val)
            }
        }
        (Bound::Excluded(a_val), Bound::Included(b_val)) => {
            if a_val == b_val {
                Some(Ordering::Greater)
            } else {
                a_val.partial_cmp(b_val)
            }
        }
    }
}

fn compare_mixed_bounds<T: PartialOrd>(
    start: &Bound<T>,
    end: &Bound<T>,
    swapped: bool,
) -> Option<Ordering> {
    match (start, end) {
        (Bound::Unbounded, _) => Some(if swapped {
            Ordering::Greater
        } else {
            Ordering::Less
        }),
        (_, Bound::Unbounded) => Some(if swapped {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
            match s.partial_cmp(e)? {
                Ordering::Less => Some(Ordering::Less),
                Ordering::Greater => Some(Ordering::Greater),
                Ordering::Equal => {
                    // When values are equal, Included start > Excluded end
                    match (start, end) {
                        (Bound::Included(_), Bound::Excluded(_)) => Some(Ordering::Greater),
                        (Bound::Excluded(_), Bound::Included(_)) => Some(Ordering::Less),
                        _ => Some(Ordering::Equal),
                    }
                }
            }
        }
    }
}

pub fn range_intersection<T: Ord>(
    (start1, end1): (IndexBound<T>, IndexBound<T>),
    (start2, end2): (IndexBound<T>, IndexBound<T>),
) -> Option<(IndexBound<T>, IndexBound<T>)> {
    let intersection_start = cmp::max(start1, start2);
    let intersection_end = cmp::min(end1, end2);

    if intersection_start > intersection_end {
        return None;
    }

    Some((intersection_start, intersection_end))
}

pub fn range_union<T: Ord>(
    (start1, end1): (IndexBound<T>, IndexBound<T>),
    (start2, end2): (IndexBound<T>, IndexBound<T>),
) -> Option<(IndexBound<T>, IndexBound<T>)> {
    debug_assert!(start1 <= start2, "ranges must be sorted by start");

    if start2 > end1 {
        return None;
    }

    let union_start = cmp::min(start1, start2);
    let union_end = cmp::max(end1, end2);

    Some((union_start, union_end))
}
