use crate::{
    runtime::{
        ClosedExecutor, RunningExecutor, RuntimeError, RuntimeResult,
        context::TransactionContext,
        ops::{
            ClosedFilter, ClosedInsert, ClosedLimit, ClosedProject, ClosedSeqScan, ClosedValues,
        },
    },
    sql::planner::physical::{PhysicalOperator, PhysicalPlan},
    storage::tuple::Row,
    tree::accessor::TreeWriter,
};

/// Type-erased executor trait for runtime polymorphism
pub(crate) trait DynExecutor {
    fn next(&mut self) -> RuntimeResult<Option<Row>>;
}

/// Wrapper to convert any RunningExecutor into DynExecutor
struct RunningWrapper<E: RunningExecutor> {
    inner: E,
}

impl<E: RunningExecutor> DynExecutor for RunningWrapper<E> {
    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        self.inner.next()
    }
}

pub(crate) type BoxedExecutor = Box<dyn DynExecutor>;

/// Adapter types for dynamic child executors
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

/// Builds executor trees from physical plans
pub(crate) struct ExecutorBuilder<Acc>
where
    Acc: TreeWriter + Clone + Default,
{
    ctx: TransactionContext<Acc>,
}

impl<Acc> ExecutorBuilder<Acc>
where
    Acc: TreeWriter + Clone + Default + 'static,
{
    pub(crate) fn new(ctx: TransactionContext<Acc>) -> Self {
        Self { ctx }
    }

    /// Build and open an executor from a physical plan
    pub(crate) fn build(&self, plan: &PhysicalPlan) -> RuntimeResult<BoxedExecutor> {
        self.build_operator(&plan.op, &plan.children)
    }

    fn build_operator(
        &self,
        op: &PhysicalOperator,
        children: &[PhysicalPlan],
    ) -> RuntimeResult<BoxedExecutor> {
        match op {
            PhysicalOperator::SeqScan(scan_op) => {
                let closed = ClosedSeqScan::new(scan_op.clone(), self.ctx.clone(), None);
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Values(values_op) => {
                let closed = ClosedValues::new(values_op.clone(), None);
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Filter(filter_op) => {
                self.require_children(children, 1, "Filter")?;
                let child = self.build(&children[0])?;
                let closed = ClosedFilter::new(
                    filter_op.clone(),
                    ClosedDynChild(Some(DynChildAdapter::new(child))),
                    None,
                );
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Project(project_op) => {
                self.require_children(children, 1, "Project")?;
                let child = self.build(&children[0])?;
                let closed = ClosedProject::new(
                    project_op.clone(),
                    ClosedDynChild(Some(DynChildAdapter::new(child))),
                    None,
                );
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Insert(insert_op) => {
                self.require_children(children, 1, "Insert")?;
                let child = self.build(&children[0])?;
                let closed = ClosedInsert::new(
                    insert_op.clone(),
                    self.ctx.clone(),
                    ClosedDynChild(Some(DynChildAdapter::new(child))),
                    None,
                );
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Limit(limit_op) => {
                self.require_children(children, 1, "Limit")?;
                let child = self.build(&children[0])?;
                let closed = ClosedLimit::new(
                    limit_op.clone(),
                    ClosedDynChild(Some(DynChildAdapter::new(child))),
                    None,
                );
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            PhysicalOperator::Empty(_) => {
                // Empty produces no rows
                let closed = ClosedValues::new(
                    crate::sql::planner::physical::PhysValuesOp {
                        rows: Vec::new(),
                        schema: op.output_schema().clone(),
                    },
                    None,
                );
                let open = closed.open()?;
                Ok(Box::new(RunningWrapper { inner: open }))
            }

            _ => Err(RuntimeError::Other(format!(
                "Operator '{}' not yet implemented in executor builder",
                op.name()
            ))),
        }
    }

    fn require_children(
        &self,
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
}
