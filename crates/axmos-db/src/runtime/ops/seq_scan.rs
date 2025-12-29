use crate::{
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeResult,
        context::TransactionContext, eval::ExpressionEvaluator,
    },
    sql::planner::physical::SeqScanOp,
    storage::tuple::Row,
    tree::{accessor::TreeReader, bplustree::BtreePositionalIterator},
};

use crate::schema::catalog::CatalogTrait;

pub(crate) struct OpenSeqScan<Acc: TreeReader + Clone> {
    op: SeqScanOp,
    ctx: TransactionContext<Acc>,
    cursor: BtreePositionalIterator<Acc>,
    stats: ExecutionStats,
}

pub(crate) struct ClosedSeqScan<Acc: TreeReader + Clone> {
    op: SeqScanOp,
    ctx: TransactionContext<Acc>,
    stats: ExecutionStats,
}

impl<Acc> ClosedSeqScan<Acc>
where
    Acc: TreeReader + Clone,
{
    pub(crate) fn new(
        op: SeqScanOp,
        ctx: TransactionContext<Acc>,
        stats: Option<ExecutionStats>,
    ) -> Self {
        Self {
            op,
            ctx,
            stats: stats.unwrap_or(ExecutionStats::default()),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }
}

impl<Acc> OpenSeqScan<Acc>
where
    Acc: TreeReader + Clone + Default,
{
    pub(crate) fn new(op: SeqScanOp, ctx: TransactionContext<Acc>) -> RuntimeResult<Self> {
        let tree_builder = ctx.tree_builder();
        let snapshot = ctx.snapshot();
        let table = ctx
            .catalog()
            .get_relation(op.table_id, &tree_builder, &snapshot)?;

        let root_page = table.root();
        let schema = table.schema();
        let mut table = ctx.build_tree(root_page);

        let cursor = table.iter_forward()?;
        Ok(Self {
            op,
            ctx,
            cursor,
            stats: ExecutionStats::default(),
        })
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn evaluate_predicate(&self, row: &Row) -> RuntimeResult<bool> {
        match &self.op.predicate {
            None => Ok(true),
            Some(pred) => {
                let evaluator = ExpressionEvaluator::new(row, &self.op.output_schema);
                let bool = evaluator.evaluate_as_bool(pred)?;
                Ok(bool)
            }
        }
    }
}

impl<Acc> ClosedExecutor for ClosedSeqScan<Acc>
where
    Acc: TreeReader + Clone + Default,
{
    type Running = OpenSeqScan<Acc>;
    fn open(self) -> RuntimeResult<Self::Running> {
        OpenSeqScan::new(self.op, self.ctx)
    }
}

impl<Acc> RunningExecutor for OpenSeqScan<Acc>
where
    Acc: TreeReader + Clone + Default,
{
    type Closed = ClosedSeqScan<Acc>;
    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        loop {
            let next_pos = match self.cursor.next() {
                Some(res) => res?,
                None => {
                    return Ok(None);
                }
            };

            self.stats.rows_scanned += 1;
            let snapshot = self.ctx.snapshot();

            let mut tree = self.cursor.get_tree();
            let maybe_row = tree
                .get_row_at(next_pos, &self.op.output_schema, &snapshot)?
                .filter(|r| {
                    self.evaluate_predicate(r)
                        .expect("Predicate evaluation failed")
                });

            if maybe_row.is_none() {
                continue;
            }

            self.stats.rows_produced += 1;
            return Ok(Some(maybe_row.unwrap()));
        }
    }

    fn close(mut self) -> RuntimeResult<Self::Closed> {
        self.ctx.accessor_mut().clear();
        Ok(ClosedSeqScan::new(self.op, self.ctx, Some(self.stats)))
    }
}
