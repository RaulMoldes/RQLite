// src/sql/executor/build.rs
use crate::{
    database::errors::{BuilderError, BuilderResult},
    sql::{
        executor::{
            Executor,
            context::ExecutionContext,
            ops::{
                agg::HashAggregate, delete::Delete, filter::Filter, insert::Insert, limit::Limit,
                materialize::Materialize, project::Project, seq_scan::SeqScan, sort::QuickSort,
                top_n::TopN, update::Update, values::Values,
            },
        },
        planner::physical::{PhysValuesOp, PhysicalOperator, PhysicalPlan},
    },
};

/// Builds an executor tree from a physical plan.
pub(crate) struct ExecutorBuilder {
    ctx: ExecutionContext,
}

impl ExecutorBuilder {
    pub(crate) fn new(ctx: ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Build an executor tree from a physical plan.
    pub(crate) fn build(&self, plan: &PhysicalPlan) -> BuilderResult<Box<dyn Executor>> {
        self.build_operator(&plan.op, &plan.children)
    }

    fn build_operator(
        &self,
        op: &PhysicalOperator,
        children: &[PhysicalPlan],
    ) -> BuilderResult<Box<dyn Executor>> {
        match op {
            PhysicalOperator::SeqScan(scan_op) => {
                let executor = SeqScan::new(scan_op.clone(), self.ctx.clone());
                Ok(Box::new(executor))
            }

            PhysicalOperator::Filter(filter_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = Filter::new(child, filter_op.predicate.clone());
                Ok(Box::new(executor))
            }

            PhysicalOperator::Values(values_op) => {
                let executor = Values::new(values_op.clone());
                Ok(Box::new(executor))
            }

            PhysicalOperator::Project(project_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let expressions: Vec<_> = project_op
                    .expressions
                    .iter()
                    .map(|pe| pe.expr.clone())
                    .collect();
                let executor = Project::new(child, expressions, project_op.output_schema.clone());
                Ok(Box::new(executor))
            }

            PhysicalOperator::Insert(insert_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = Insert::new(insert_op.clone(), self.ctx.clone(), child);
                Ok(Box::new(executor))
            }

            PhysicalOperator::Update(update_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = Update::new(update_op.clone(), self.ctx.clone(), child);
                Ok(Box::new(executor))
            }

            PhysicalOperator::Materialize(materialize_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = Materialize::new(materialize_op.clone(), child);
                Ok(Box::new(executor))
            }

            PhysicalOperator::HashAggregate(agg_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = HashAggregate::new(agg_op.clone(), child);
                Ok(Box::new(executor))
            }

            PhysicalOperator::Delete(delete_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = Delete::new(delete_op.clone(), self.ctx.clone(), child);
                Ok(Box::new(executor))
            }

            PhysicalOperator::Sort(sort_op) => {
                // EnforcerSortOp uses the same Sort executor
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;

                let executor = QuickSort::new(sort_op.clone(), child);
                Ok(Box::new(executor))
            }

            PhysicalOperator::TopN(topn_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = TopN::new(topn_op.clone(), child);
                Ok(Box::new(executor))
            }

            // Limit operator
            PhysicalOperator::Limit(limit_op) => {
                if children.len() != 1 {
                    return Err(BuilderError::NumChildrenMismatch(children.len(), 1));
                }
                let child = self.build(&children[0])?;
                let executor = Limit::new(limit_op.clone(), child);
                Ok(Box::new(executor))
            }
            // Empty result operator
            PhysicalOperator::Empty(empty_op) => {
                let values_op = PhysValuesOp {
                    rows: Vec::new(),
                    schema: empty_op.schema.clone(),
                };
                let executor = Values::new(values_op);
                Ok(Box::new(executor))
            }

            _ => unimplemented!(
                "Execution build is only implemented for Project, Scan, Filter, Insert, Update, and Delete"
            ),
        }
    }
}
