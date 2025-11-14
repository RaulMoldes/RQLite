use crate::database::Database;
use crate::database::schema::Relation;
use crate::sql::ast::BinaryOperator;
use crate::sql::ast::Expr;
use crate::types::OId;

pub struct Candidate {
    index_oid: OId,
    column_name: String,
    selectivity_score: f64,
}

impl Candidate {
    pub fn get(&self) -> OId {
        self.index_oid
    }
}

pub fn find_applicable_indexes(
    table_name: &str,
    where_clause: Option<&Expr>,
    db: &Database,
) -> std::io::Result<Vec<Candidate>> {
    let mut candidates = Vec::new();

    // Get table schema to find indexed columns
    let table = db.relation(table_name)?;

    // Check each column for indexes
    for column in table.columns() {
        if let Some(index_name) = column.index() {
            // Check if this index is useful for the WHERE clause
            if let Some(where_expr) = where_clause {
                if let Ok(index) = db.relation(index_name) {
                    let score = compute_selectivity_score(where_expr, &index, &table);
                    if score > 0.0f64 {
                        candidates.push(Candidate {
                            index_oid: index.id(),
                            column_name: column.name.clone(),
                            selectivity_score: score,
                        });
                    };
                }
            }
        }
    }

    // Sort by selectivity score
    candidates.sort_by(|a, b| {
        b.selectivity_score
            .partial_cmp(&a.selectivity_score)
            .unwrap()
    });
    Ok(candidates)
}

// Ideally we would have an statistics table to compute the selectivity but for now this is not possible, so we are just hard-coding it depending on the expression found.
//
// In practice, the actual expression is not going to affect the index which is used, but will probably be better in certain conditions to not use the index and go directly with scanning the whole table.
// This is the idea behind this function.
fn compute_selectivity_score(expr: &Expr, index: &Relation, index_table: &Relation) -> f64 {
    let first_column = index.columns().first().unwrap();

    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Check if this is a direct comparison on our column
            let is_column_match = match left.as_ref() {
                Expr::QualifiedIdentifier { table, column } => {
                    column == first_column.name() && table == index_table.name()
                }
                Expr::Identifier(col) => col == first_column.name(),
                _ => false,
            };

            if is_column_match {
                match op {
                    BinaryOperator::Eq => 1.0,
                    BinaryOperator::Lt
                    | BinaryOperator::Gt
                    | BinaryOperator::Le
                    | BinaryOperator::Ge => 0.8, // Range queries
                    BinaryOperator::Like => {
                        // If it is a prefix match he selectivity score is higher
                        // Index comparisons on variable length data are made from beginning to end (check bplustree.rs for details)
                        if let Expr::String(pattern) = right.as_ref() {
                            if pattern.ends_with('%') && !pattern.starts_with('%') {
                                return 0.7;
                            }
                        }
                        0.2 // Full wildcard
                    }
                    BinaryOperator::In => 0.6,
                    _ => 0.0,
                }
            } else {
                // Check nested expressions
                match op {
                    BinaryOperator::And => {
                        // For AND, take the maximum score
                        let left_score = compute_selectivity_score(left, index, index_table);
                        let right_score = compute_selectivity_score(right, index, index_table);
                        f64::max(left_score, right_score)
                    }
                    BinaryOperator::Or => {
                        // For OR, index is less useful unless both sides use it
                        let left_score = compute_selectivity_score(left, index, index_table);
                        let right_score = compute_selectivity_score(right, index, index_table);
                        if left_score > 0.0 && right_score > 0.0 {
                            (left_score + right_score) / 2.0
                        } else {
                            0.0
                        }
                    }
                    _ => 0.0,
                }
            }
        }
        Expr::Between { expr, .. } => compute_selectivity_score(expr, index, index_table) * 0.8,
        _ => 0.0,
    }
}
