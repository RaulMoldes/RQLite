//! Properties in the Cascades optimizer.
//!
//! Taken from the article by microsoft: [Extensible query optimizers in practice]
//! ```text
//!
//! Examples of logical properties of an expression include its relational algebraic expression and the cardinality of the expression.
//!
//! For example, A ▷◁ B and B ▷◁ A both produce the join result of A and B, and they have the same logical properties (e.g.,cardinality).
//!
//! Examples of physical properties of an expression include the sort order of the output of the expression and degree of parallelism, i.e., the number of threads used to execute the expression .
//!
//! ```
use std::collections::HashSet;

use crate::{DataType, database::schema::Schema, sql::binder::ast::BoundExpression};

use super::physical::OrderingSpec;

/// Logical properties - derived from the logical structure of the plan
#[derive(Debug, Clone, Default)]
pub struct LogicalProperties {
    /// Output schema
    pub schema: Schema,
    /// Estimated cardinality (row count)
    pub cardinality: f64,
    /// Whether the output is known to have unique rows
    pub unique: bool,
    /// Columns that uniquely identify rows
    pub unique_columns: Option<Vec<usize>>,
    /// Functional dependencies (column A determines column B)
    pub functional_deps: Vec<FunctionalDependency>,
    /// Output ordering (if known from the logical plan)
    pub ordering: Option<Vec<OrderingColumn>>,
}

impl LogicalProperties {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            cardinality: 0.0,
            unique: false,
            unique_columns: None,
            functional_deps: Vec::new(),
            ordering: None,
        }
    }

    pub fn with_cardinality(mut self, cardinality: f64) -> Self {
        self.cardinality = cardinality;
        self
    }

    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }
}

/// A functional dependency: lhs -> rhs
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionalDependency {
    /// Determinant columns
    pub lhs: Vec<usize>,
    /// Dependent columns
    pub rhs: Vec<usize>,
}

/// Ordering specification for a column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderingColumn {
    pub column_idx: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl OrderingColumn {
    pub fn new(column_idx: usize, ascending: bool) -> Self {
        Self {
            column_idx,
            ascending,
            nulls_first: ascending, // Default: NULLS FIRST for ASC, NULLS LAST for DESC
        }
    }

    pub fn asc(column_idx: usize) -> Self {
        Self::new(column_idx, true)
    }

    pub fn desc(column_idx: usize) -> Self {
        Self::new(column_idx, false)
    }
}

/// Physical properties
#[derive(Debug, Clone, Default)]
pub struct PhysicalProperties {
    /// Output ordering (expression, ascending)
    pub ordering: Vec<(BoundExpression, bool)>,
    /// Data distribution (for parallel/distributed plans)
    pub distribution: Distribution,
    /// Partitioning scheme
    pub partitioning: Option<Partitioning>,
}

impl PhysicalProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ordering(mut self, ordering: Vec<(BoundExpression, bool)>) -> Self {
        self.ordering = ordering;
        self
    }

    pub fn with_distribution(mut self, distribution: Distribution) -> Self {
        self.distribution = distribution;
        self
    }

    /// Check if this satisfies the required properties
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        // Check ordering satisfaction
        if !required.ordering.is_empty() {
            if self.ordering.len() < required.ordering.len() {
                return false;
            }
            // The provided ordering must match required column indices
            for (i, req_spec) in required.ordering.iter().enumerate() {
                if let Some((prov_expr, prov_asc)) = self.ordering.get(i) {
                    // Check if the expression references the required column
                    // and has the same direction
                    if !expr_matches_column(prov_expr, req_spec.column_idx) {
                        return false;
                    }
                    if *prov_asc != req_spec.ascending {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        // Check distribution satisfaction
        match (&required.distribution, &self.distribution) {
            (Distribution::Any, _) => true,
            (Distribution::Single, Distribution::Single) => true,
            (Distribution::Hash(req), Distribution::Hash(prov)) => req == prov,
            (Distribution::Broadcast, Distribution::Broadcast) => true,
            _ => false,
        }
    }
}

/// Check if a BoundExpression references a specific column index
fn expr_matches_column(expr: &BoundExpression, column_idx: usize) -> bool {
    match expr {
        BoundExpression::ColumnRef(col_ref) => col_ref.column_idx == column_idx,
        // Could extend to handle more complex expressions
        _ => false,
    }
}

/// Data distribution for parallel execution
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Distribution {
    #[default]
    Any,
    Single,
    Hash(Vec<usize>), // Hash partitioned by column indices
    Range(Vec<usize>),
    Broadcast,
    Replicated,
}

/// Partitioning scheme
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Partitioning {
    pub columns: Vec<usize>,
    pub num_partitions: usize,
}

/// Required properties (what the parent plan node needs the children to satisify.)
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RequiredProperties {
    /// Required ordering by column indices
    pub ordering: Vec<OrderingSpec>,
    /// Required distribution
    pub distribution: Distribution,
    /// Required partitioning
    pub partitioning: Option<Partitioning>,
}

impl RequiredProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ordering(mut self, ordering: Vec<OrderingSpec>) -> Self {
        self.ordering = ordering;
        self
    }

    pub fn with_distribution(mut self, distribution: Distribution) -> Self {
        self.distribution = distribution;
        self
    }

    /// Check if any properties are required
    pub fn is_empty(&self) -> bool {
        self.ordering.is_empty()
            && matches!(self.distribution, Distribution::Any)
            && self.partitioning.is_none()
    }

    /// Combine with another set of required properties (union)
    pub fn union(&self, other: &RequiredProperties) -> RequiredProperties {
        RequiredProperties {
            // Take the longer ordering requirement
            ordering: if self.ordering.len() > other.ordering.len() {
                self.ordering.clone()
            } else {
                other.ordering.clone()
            },
            // Take the more restrictive distribution
            distribution: match (&self.distribution, &other.distribution) {
                (Distribution::Any, d) | (d, Distribution::Any) => d.clone(),
                (d1, _) => d1.clone(), // Prefer first if both specific
            },
            // Take the first partitioning if any
            partitioning: self.partitioning.clone().or(other.partitioning.clone()),
        }
    }
}

/// Derived properties (computed from child properties).
#[derive(Debug, Clone)]
pub struct DerivedProperties {
    /// Columns that are constant (have a single value)
    pub constant_columns: HashSet<usize>,
    /// Columns that are not null
    pub not_null_columns: HashSet<usize>,
    /// Known value ranges for columns
    pub column_ranges: Vec<ColumnRange>,
}

impl Default for DerivedProperties {
    fn default() -> Self {
        Self {
            constant_columns: HashSet::new(),
            not_null_columns: HashSet::new(),
            column_ranges: Vec::new(),
        }
    }
}

/// Known range for a column
#[derive(Debug, Clone)]
pub struct ColumnRange {
    pub column_idx: usize,
    pub min: Option<DataType>,
    pub max: Option<DataType>,
    pub has_nulls: bool,
}

impl DataType {
    pub(crate) fn from_bound_expr(expr: &BoundExpression) -> Option<Self> {
        match expr {
            BoundExpression::Literal { value } => Some(value.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{sql::binder::ast::BoundColumnRef, types::DataTypeKind};
    use std::collections::HashMap;

    fn make_col_expr(column_idx: usize) -> BoundExpression {
        BoundExpression::ColumnRef(BoundColumnRef {
            table_idx: 0,
            column_idx,
            data_type: DataTypeKind::Int,
        })
    }

    #[test]
    fn test_physical_properties_satisfies() {
        let provided = PhysicalProperties {
            ordering: vec![(make_col_expr(0), true), (make_col_expr(1), false)],
            ..Default::default()
        };

        // Empty requirements are always satisfied
        let required = RequiredProperties::default();
        assert!(provided.satisfies(&required));

        // Prefix ordering is satisfied
        let required = RequiredProperties {
            ordering: vec![OrderingSpec::asc(0)],
            ..Default::default()
        };
        assert!(provided.satisfies(&required));

        // Full ordering satisfied
        let required = RequiredProperties {
            ordering: vec![OrderingSpec::asc(0), OrderingSpec::desc(1)],
            ..Default::default()
        };
        assert!(provided.satisfies(&required));

        // Wrong direction not satisfied
        let required = RequiredProperties {
            ordering: vec![OrderingSpec::desc(0)],
            ..Default::default()
        };
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn test_distribution_satisfaction() {
        let single = PhysicalProperties {
            distribution: Distribution::Single,
            ..Default::default()
        };

        // Any distribution is satisfied
        let any_req = RequiredProperties {
            distribution: Distribution::Any,
            ..Default::default()
        };
        assert!(single.satisfies(&any_req));

        // Same distribution is satisfied
        let single_req = RequiredProperties {
            distribution: Distribution::Single,
            ..Default::default()
        };
        assert!(single.satisfies(&single_req));

        // Different distribution is not satisfied
        let hash_req = RequiredProperties {
            distribution: Distribution::Hash(vec![0]),
            ..Default::default()
        };
        assert!(!single.satisfies(&hash_req));
    }

    #[test]
    fn test_required_properties_union() {
        let req1 = RequiredProperties {
            ordering: vec![OrderingSpec::asc(0)],
            distribution: Distribution::Single,
            ..Default::default()
        };

        let req2 = RequiredProperties {
            ordering: vec![OrderingSpec::asc(0), OrderingSpec::desc(1)],
            distribution: Distribution::Any,
            ..Default::default()
        };

        let combined = req1.union(&req2);
        assert_eq!(combined.ordering.len(), 2); // Takes longer ordering
        assert_eq!(combined.distribution, Distribution::Single); // Takes more restrictive
    }

    #[test]
    fn test_required_properties_hash() {
        let req = RequiredProperties {
            ordering: vec![OrderingSpec::asc(0)],
            distribution: Distribution::Single,
            partitioning: None,
        };

        // Should be usable as HashMap key
        let mut map: HashMap<RequiredProperties, f64> = HashMap::new();
        map.insert(req.clone(), 100.0);
        assert_eq!(map.get(&req), Some(&100.0));
    }
}
