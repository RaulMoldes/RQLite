use crate::core::SerializableType;
use crate::schema::Schema;
use crate::schema::catalog::CatalogTrait;
use crate::{
    TypeSystemError, UInt64,
    runtime::{
        ClosedExecutor, ExecutionStats, RunningExecutor, RuntimeError, RuntimeResult,
        context::TransactionContext, eval::ExpressionEvaluator,
    },
    sql::planner::physical::PhysIndexScanOp,
    storage::tuple::Row,
    tree::{
        accessor::TreeReader,
        bplustree::{Btree, BtreePositionalIterator, SearchResult},
    },
};

pub(crate) struct OpenIndexScan<Acc: TreeReader + Clone> {
    op: PhysIndexScanOp,
    ctx: TransactionContext<Acc>,
    table_tree: Btree<Acc>,
    index_schema: Schema,
    table_schema: Schema,
    index_cursor: Option<BtreePositionalIterator>,
    stats: ExecutionStats,
}

pub(crate) struct ClosedIndexScan<Acc: TreeReader + Clone> {
    op: PhysIndexScanOp,
    ctx: TransactionContext<Acc>,
    stats: ExecutionStats,
}

impl<Acc> ClosedIndexScan<Acc>
where
    Acc: TreeReader + Clone,
{
    pub(crate) fn new(
        op: PhysIndexScanOp,
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

impl<Acc> OpenIndexScan<Acc>
where
    Acc: TreeReader + Clone + Default,
{
    pub(crate) fn new(op: PhysIndexScanOp, ctx: TransactionContext<Acc>) -> RuntimeResult<Self> {
        let tree_builder = ctx.tree_builder();
        let snapshot = ctx.snapshot();
        let table = ctx
            .catalog()
            .get_relation(op.table_id, &tree_builder, &snapshot)?;

        let index = ctx
            .catalog()
            .get_relation(op.index_id, &tree_builder, &snapshot)?;

        let table_root_page = table.root();
        let table_schema = table.schema().clone();

        let index_root_page = index.root();
        let index_schema = index.schema().clone();

        let mut index_tree = ctx.build_tree(index_root_page);
        let table_tree = ctx.build_tree(table_root_page);

        Ok(Self {
            op,
            ctx,
            table_tree,
            index_schema,
            table_schema,
            index_cursor: index_tree.iter_forward().ok(),
            stats: ExecutionStats::default(),
        })
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    fn evaluate_index_predicate(&self, row: &Row) -> RuntimeResult<bool> {
        let evaluator = ExpressionEvaluator::new(row, &self.index_schema);

        let value = row[self.op.index_columns[0]].clone();
        let lhs = self
            .op
            .range_start
            .as_ref()
            .map(|expr| evaluator.evaluate(expr).expect("Evaluation failed"))
            .map(|l| l <= value)
            .unwrap_or(true);

        let rhs = self
            .op
            .range_end
            .as_ref()
            .map(|expr| evaluator.evaluate(expr).expect("Evaluation failed"))
            .map(|l| l >= value)
            .unwrap_or(true);

        Ok(lhs && rhs)
    }

    fn get_row_id_checked<'a>(&self, row: &'a Row) -> RuntimeResult<&'a UInt64> {
        let row_id = row[row.len() - 1]
            .as_big_u_int()
            .ok_or(RuntimeError::TypeError(
                TypeSystemError::UnexpectedDataType(row[row.len() - 1].kind()),
            ))?;
        Ok(row_id)
    }
}

impl<Acc> ClosedExecutor for ClosedIndexScan<Acc>
where
    Acc: TreeReader + Clone + Default,
{
    type Running = OpenIndexScan<Acc>;
    fn open(self) -> RuntimeResult<Self::Running> {
        OpenIndexScan::new(self.op, self.ctx)
    }
}

impl<Acc> RunningExecutor for OpenIndexScan<Acc>
where
    Acc: TreeReader + Clone + Default,
{
    type Closed = ClosedIndexScan<Acc>;
    fn next(&mut self) -> RuntimeResult<Option<Row>> {
        if self.index_cursor.is_none() {
            return Ok(None);
        };

        loop {
            let next_pos = match self
                .index_cursor
                .as_mut()
                .ok_or(RuntimeError::CursorUninitialized)?
                .next()
            {
                Some(res) => res?,
                None => {
                    return Ok(None);
                }
            };

            self.stats.rows_scanned += 1;
            let snapshot = self.ctx.snapshot();

            let mut tree = self
                .index_cursor
                .as_ref()
                .ok_or(RuntimeError::CursorUninitialized)?
                .get_tree();
            let maybe_row = tree
                .get_row_at(next_pos, &self.index_schema, &snapshot)?
                .filter(|r| {
                    self.evaluate_index_predicate(r)
                        .expect("Predicate evaluation failed")
                });

            if maybe_row.is_none() {
                continue;
            }

            let found_row = maybe_row.unwrap();
            let row_key_bytes = self.get_row_id_checked(&found_row)?.serialize()?;

            let actual_row_result = self
                .table_tree
                .search(row_key_bytes.as_ref(), &self.table_schema)?;

            if let SearchResult::Found(found_pos) = actual_row_result {
                let actual_row = tree.get_row_at(found_pos, &self.table_schema, &snapshot)?;

                if actual_row.is_none() {
                    continue;
                }

                self.stats.rows_produced += 1;
                return Ok(Some(actual_row.unwrap()));
            } else {
                continue;
            }
        }
    }

    fn close(self) -> RuntimeResult<Self::Closed> {
        Ok(ClosedIndexScan::new(self.op, self.ctx, Some(self.stats)))
    }
}
