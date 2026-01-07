use crate::{
    runtime::{ExecutionStats, Executor, RuntimeResult},
    storage::tuple::Row,
};

/// The Materialize operator buffers all input rows before producing output.
/// This breaks the pipeline and ensures the child scan completes before
/// any downstream DML operations modify the tree structure.
pub(crate) struct Materialize<Child: Executor> {
    /// Child
    child: Child,
    /// Buffered rows from child
    buffer: Vec<Row>,
    /// Current position in buffer during iteration
    current_idx: usize,
    /// Execution stats
    stats: ExecutionStats,
}

impl<Child: Executor> Materialize<Child> {
    pub(crate) fn new(child: Child) -> Self {
        Self {
            child,
            stats: ExecutionStats::default(),
            buffer: Vec::new(),
            current_idx: 0,
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Child: Executor> Materialize<Child> {
    /// Collect all rows from child into buffer
    fn materialize_child(&mut self) -> RuntimeResult<()> {
        while let Some(row) = self.child.next()? {
            self.stats.rows_scanned += 1;
            self.buffer.push(row);
        }
        Ok(())
    }
}

impl<Child: Executor> Executor for Materialize<Child> {
    fn open(&mut self) -> RuntimeResult<()> {
        self.child.open()?;
        self.materialize_child()?;
        self.child.close()?;
        Ok(())
    }

    fn next(&mut self) -> RuntimeResult<Option<Row>> {
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

    fn close(&mut self) -> RuntimeResult<()> {
        self.child.close()?;
        Ok(())
    }
}
