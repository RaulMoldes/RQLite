//! Executor builder - constructs executor trees from physical plans.
//!
//! This module provides builders that convert physical plans into executable
//! operator trees. Two variants exist:
//! - `MutableExecutorBuilder`: For DML operations (INSERT/UPDATE/DELETE)
//! - `ReadOnlyExecutorBuilder`: For read-only queries (SELECT)

use crate::{
    runtime::{
        ClosedExecutor, RunningExecutor, RuntimeError, RuntimeResult,
        context::TransactionContext,
        ops::{
            ClosedDelete, ClosedFilter, ClosedHashAggregate, ClosedIndexScan, ClosedInsert,
            ClosedLimit, ClosedMaterialize, ClosedNestedLoopJoin, ClosedProject, ClosedSeqScan,
            ClosedSort, ClosedUpdate, ClosedValues,
        },
    },
    sql::planner::physical::{PhysicalOperator, PhysicalPlan},
    storage::tuple::Row,
    tree::accessor::{BtreeReadAccessor, BtreeWriteAccessor, TreeReader},
};

/// Type-erased executor trait for runtime polymorphism.
pub(crate) trait DynExecutor {
    fn next(&mut self) -> RuntimeResult<Option<Row>>;
}

/// Wrapper to convert any RunningExecutor into DynExecutor.
struct RunningWrapper<E: RunningExecutor> {
    inner: E,
}

impl<E: RunningExecutor> DynExecutor for RunningWrapper<E> {
    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        self.inner.next()
    }
}

pub(crate) type BoxedExecutor = Box<dyn DynExecutor>;

/// Adapter for dynamic child executors.
pub(crate) struct DynChildAdapter {
    inner: BoxedExecutor,
}

impl DynChildAdapter {
    fn new(executor: BoxedExecutor) -> Self {
        Self { inner: executor }
    }
}

pub(crate) struct ClosedDynChild(Option<DynChildAdapter>);
pub(crate) struct OpenDynChild(DynChildAdapter);

impl ClosedExecutor for ClosedDynChild {
    type Running = OpenDynChild;

    fn open(mut self) -> RuntimeResult<Self::Running> {
        let adapter = self
            .0
            .take()
            .ok_or_else(|| RuntimeError::Other("DynChild already opened".to_string()))?;
        Ok(OpenDynChild(adapter))
    }
}

impl RunningExecutor for OpenDynChild {
    type Closed = ClosedDynChild;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        self.0.inner.next()
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedDynChild(Some(self.0)))
    }
}

/// Wraps a child executor into a ClosedDynChild.
fn wrap_child(child: BoxedExecutor) -> ClosedDynChild {
    ClosedDynChild(Some(DynChildAdapter::new(child)))
}

/// Validates that the expected number of children are present.
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

/// Builds read-only operators that don't require a mutable context.
/// Returns None if the operator requires DML support.
fn build_readonly_operator<Acc: TreeReader + Clone + Default + 'static>(
    ctx: &TransactionContext<Acc>,
    op: &PhysicalOperator,
    children: &[PhysicalPlan],
    build_child: &dyn Fn(&PhysicalPlan) -> RuntimeResult<BoxedExecutor>,
) -> RuntimeResult<Option<BoxedExecutor>> {
    match op {
        PhysicalOperator::SeqScan(scan_op) => {
            let closed = ClosedSeqScan::new(scan_op.clone(), ctx.clone(), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::IndexScan(index_op) => {
            let closed = ClosedIndexScan::new(index_op.clone(), ctx.clone(), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Values(values_op) => {
            let closed = ClosedValues::new(values_op.clone(), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Empty(_) => {
            let closed = ClosedValues::new(
                crate::sql::planner::physical::PhysValuesOp {
                    rows: Vec::new(),
                    schema: op.output_schema().clone(),
                },
                None,
            );
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Filter(filter_op) => {
            require_children(children, 1, "Filter")?;
            let child = build_child(&children[0])?;
            let closed = ClosedFilter::new(filter_op.clone(), wrap_child(child), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Project(project_op) => {
            require_children(children, 1, "Project")?;
            let child = build_child(&children[0])?;
            let closed = ClosedProject::new(project_op.clone(), wrap_child(child), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::NestedLoopJoin(join_op) => {
            require_children(children, 2, "NestedLoopJoin")?;
            let left = build_child(&children[0])?;
            let right = build_child(&children[1])?;
            let closed = ClosedNestedLoopJoin::new(
                join_op.clone(),
                wrap_child(left),
                wrap_child(right),
                None,
            );
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
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
            let closed = ClosedHashAggregate::new(agg_op.clone(), wrap_child(child), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Sort(sort_op) => {
            require_children(children, 1, "Sort")?;
            let child = build_child(&children[0])?;
            let closed = ClosedSort::new(sort_op.clone(), wrap_child(child), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Limit(limit_op) => {
            require_children(children, 1, "Limit")?;
            let child = build_child(&children[0])?;
            let closed = ClosedLimit::new(limit_op.clone(), wrap_child(child), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        PhysicalOperator::Distinct(_) => Err(RuntimeError::Other(
            "Distinct executor not yet implemented".to_string(),
        )),

        PhysicalOperator::Materialize(mat_op) => {
            require_children(children, 1, "Materialize")?;
            let child = build_child(&children[0])?;
            let closed = ClosedMaterialize::new(mat_op.clone(), wrap_child(child), None);
            let open = closed.open()?;
            Ok(Some(Box::new(RunningWrapper { inner: open })))
        }

        // DML operators require mutable context
        PhysicalOperator::Insert(_) | PhysicalOperator::Update(_) | PhysicalOperator::Delete(_) => {
            Ok(None)
        }
    }
}

/// Builds executor trees that support DML operations.
pub struct MutableExecutorBuilder {
    ctx: TransactionContext<BtreeWriteAccessor>,
}

impl MutableExecutorBuilder {
    pub fn new(ctx: TransactionContext<BtreeWriteAccessor>) -> Self {
        Self { ctx }
    }

    /// Build and open an executor from a physical plan.
    pub fn build(&self, plan: &PhysicalPlan) -> RuntimeResult<BoxedExecutor> {
        self.build_operator(&plan.op, &plan.children)
    }

    fn build_operator(
        &self,
        op: &PhysicalOperator,
        children: &[PhysicalPlan],
    ) -> RuntimeResult<BoxedExecutor> {
        // Try to build as a read-only operator first
        let build_child = |plan: &PhysicalPlan| self.build(plan);

        if let Some(executor) = build_readonly_operator(&self.ctx, op, children, &build_child)? {
            return Ok(executor);
        }

        // Handle DML operators
        match op {
            PhysicalOperator::Insert(insert_op) => {
                require_children(children, 1, "Insert")?;
                let child = self.build(&children[0])?;
                let closed =
                    ClosedInsert::new(insert_op.clone(), self.ctx.clone(), wrap_child(child), None);
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Update(update_op) => {
                require_children(children, 1, "Update")?;
                let child = self.build(&children[0])?;
                let closed =
                    ClosedUpdate::new(update_op.clone(), self.ctx.clone(), wrap_child(child), None);
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Delete(delete_op) => {
                require_children(children, 1, "Delete")?;
                let child = self.build(&children[0])?;
                let closed =
                    ClosedDelete::new(delete_op.clone(), self.ctx.clone(), wrap_child(child), None);
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            _ => unreachable!("All operators should be handled"),
        }
    }
}

/// Builds executor trees for read-only queries.
pub struct ReadOnlyExecutorBuilder {
    ctx: TransactionContext<BtreeReadAccessor>,
}

impl ReadOnlyExecutorBuilder {
    pub fn new(ctx: TransactionContext<BtreeReadAccessor>) -> Self {
        Self { ctx }
    }

    /// Build and open an executor from a physical plan.
    pub fn build(&self, plan: &PhysicalPlan) -> RuntimeResult<BoxedExecutor> {
        self.build_operator(&plan.op, &plan.children)
    }

    fn build_operator(
        &self,
        op: &PhysicalOperator,
        children: &[PhysicalPlan],
    ) -> RuntimeResult<BoxedExecutor> {
        let build_child = |plan: &PhysicalPlan| self.build(plan);

        if let Some(executor) = build_readonly_operator(&self.ctx, op, children, &build_child)? {
            return Ok(executor);
        }

        // DML operators are not supported in read-only mode
        match op {
            PhysicalOperator::Insert(_)
            | PhysicalOperator::Update(_)
            | PhysicalOperator::Delete(_) => Err(RuntimeError::Other(
                "Cannot execute DML operators with read-only builder".to_string(),
            )),
            _ => unreachable!("All operators should be handled"),
        }
    }
}
