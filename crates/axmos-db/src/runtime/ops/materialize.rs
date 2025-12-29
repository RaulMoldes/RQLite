use crate::{
    runtime::{ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult},
    sql::planner::physical::PhysMaterializeOp,
    storage::tuple::Row,
};

/// The Materialize operator buffers all input rows before producing output.
/// This breaks the pipeline and ensures the child scan completes before
/// any downstream DML operations modify the tree structure.
pub(crate) struct OpenMaterialize<Child: RunningExecutor> {
    op: PhysMaterializeOp,

    child: Option<Child>,
    closed_child: Option<Child::Closed>,

    /// Buffered rows from child
    buffer: Vec<Row>,
    /// Current position in buffer during iteration
    current_idx: usize,
    /// Execution stats
    stats: ExecutionStats,
}

pub(crate) struct ClosedMaterialize<Child: ClosedExecutor> {
    op: PhysMaterializeOp,
    child: Child,
    stats: ExecutionStats,
}

impl<Child: ClosedExecutor> ClosedMaterialize<Child> {
    pub(crate) fn new(op: PhysMaterializeOp, child: Child, stats: Option<ExecutionStats>) -> Self {
        Self {
            op,
            child,
            stats: stats.unwrap_or_default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: ClosedExecutor> ClosedExecutor for ClosedMaterialize<Child> {
    type Running = OpenMaterialize<Child::Running>;

    fn open(self) -> RuntimeResult<Self::Running> {
        let child = self.child.open()?;
        Ok(OpenMaterialize {
            op: self.op,
            child: Some(child),
            closed_child: None,
            buffer: Vec::new(),
            current_idx: 0,

            stats: self.stats,
        })
    }
}

impl<Child: RunningExecutor> OpenMaterialize<Child> {
    /// Collect all rows from child into buffer
    fn materialize_child(&mut self) -> RuntimeResult<()> {
        if let Some(mut child) = self.child.take() {
            while let Some(row) = child.next()? {
                self.stats.rows_scanned += 1;
                self.buffer.push(row);
            }

            self.closed_child = Some(child.close()?);
        }

        Ok(())
    }
}

impl<Child: RunningExecutor> RunningExecutor for OpenMaterialize<Child> {
    type Closed = ClosedMaterialize<Child::Closed>;

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.child.is_some() {
            self.materialize_child()?;
        };

        // Return buffered rows one at a time
        if self.current_idx < self.buffer.len() {
            let row = self.buffer[self.current_idx].clone();
            self.current_idx += 1;
            self.stats.rows_produced += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedMaterialize {
            op: self.op,
            child: self.closed_child.unwrap(),
            stats: self.stats,
        })
    }
}
