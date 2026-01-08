use crate::{
    runtime::{
        Executor, RuntimeError, RuntimeResult,
        context::{ThreadContext, TransactionLogger},
        ops::{
            Delete, Filter, HashAggregate, IndexScan, Insert, Limit, Materialize, NestedLoopJoin,
            Project, QuickSort, SeqScan, Update, Values,
        },
    },
    sql::planner::physical::{PhysValuesOp, PhysicalOperator, PhysicalPlan},
};

pub(crate) type BoxedExecutor = Box<dyn Executor>;

fn require_children(
    children: &[PhysicalPlan],
    expected: usize,
    op_name: &str,
) -> RuntimeResult<()> {
    if children.len() != expected {
        return Err(RuntimeError::Other(format!(
            "{} requires exactly {} child(ren), got {}",
            op_name,
            expected,
            children.len()
        )));
    }
    Ok(())
}

pub(crate) struct ExecutorBuilder {
    ctx: ThreadContext,
    logger: TransactionLogger,
}

impl ExecutorBuilder {
    pub(crate) fn new(ctx: ThreadContext, logger: TransactionLogger) -> Self {
        Self { ctx, logger }
    }

    pub(crate) fn build(&self, plan: &PhysicalPlan) -> RuntimeResult<BoxedExecutor> {
        let mut executor = self.build_operator(&plan.op, &plan.children)?;
        executor.open()?;
        Ok(executor)
    }

    fn build_operator(
        &self,
        op: &PhysicalOperator,
        children: &[PhysicalPlan],
    ) -> RuntimeResult<BoxedExecutor> {
        let build_child = |plan: &PhysicalPlan| self.build_operator(&plan.op, &plan.children);

        match op {
            PhysicalOperator::SeqScan(scan_op) => {
                Ok(Box::new(SeqScan::new(scan_op, self.ctx.clone())?))
            }

            PhysicalOperator::IndexScan(index_op) => {
                Ok(Box::new(IndexScan::new(index_op, self.ctx.clone())?))
            }

            PhysicalOperator::Values(values_op) => Ok(Box::new(Values::new(values_op))),

            PhysicalOperator::Empty(_) => {
                let empty_op = PhysValuesOp {
                    rows: Vec::new(),
                    schema: op.output_schema().clone(),
                };
                Ok(Box::new(Values::new(&empty_op)))
            }

            PhysicalOperator::Filter(filter_op) => {
                require_children(children, 1, "Filter")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Filter::new(filter_op, child)))
            }

            PhysicalOperator::Project(project_op) => {
                require_children(children, 1, "Project")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Project::new(project_op, child)))
            }

            PhysicalOperator::NestedLoopJoin(join_op) => {
                require_children(children, 2, "NestedLoopJoin")?;
                let left = build_child(&children[0])?;
                let right = build_child(&children[1])?;
                Ok(Box::new(NestedLoopJoin::new(join_op, left, right)))
            }

            PhysicalOperator::HashJoin(_) => Err(RuntimeError::Other(
                "HashJoin executor not yet implemented".to_string(),
            )),

            PhysicalOperator::MergeJoin(_) => Err(RuntimeError::Other(
                "MergeJoin executor not yet implemented".to_string(),
            )),

            PhysicalOperator::HashAggregate(agg_op) => {
                require_children(children, 1, "HashAggregate")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(HashAggregate::new(agg_op, child)))
            }

            PhysicalOperator::Sort(sort_op) => {
                require_children(children, 1, "Sort")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(QuickSort::new(sort_op, child)))
            }

            PhysicalOperator::Limit(limit_op) => {
                require_children(children, 1, "Limit")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Limit::new(limit_op, child)))
            }

            PhysicalOperator::Distinct(_) => Err(RuntimeError::Other(
                "Distinct executor not yet implemented".to_string(),
            )),

            PhysicalOperator::Materialize(mat_op) => {
                require_children(children, 1, "Materialize")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Materialize::new(child)))
            }

            // DML operators
            PhysicalOperator::Insert(insert_op) => {
                require_children(children, 1, "Insert")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Insert::new(
                    insert_op,
                    self.ctx.clone(),
                    self.logger.clone(),
                    child,
                )))
            }

            PhysicalOperator::Update(update_op) => {
                require_children(children, 1, "Update")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Update::new(
                    update_op,
                    self.ctx.clone(),
                    self.logger.clone(),
                    child,
                )))
            }

            PhysicalOperator::Delete(delete_op) => {
                require_children(children, 1, "Delete")?;
                let child = build_child(&children[0])?;
                Ok(Box::new(Delete::new(
                    delete_op,
                    self.ctx.clone(),
                    self.logger.clone(),
                    child,
                    None,
                )))
            }
        }
    }
}
