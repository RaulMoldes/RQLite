use crate::database::schema::Database;
use crate::database::schema::Schema;

use crate::storage::tuple::TupleRef;
use crate::structures::bplustree::{BPlusTree, Comparator, IterDirection, NodeAccessMode, Payload};

struct ResultSet<'a> {
    inner: Vec<Payload<'a>>,
}

impl<'a> ResultSet<'a> {
    fn new() -> Self {
        Self { inner: Vec::new() }
    }

    fn push(&mut self, payload: Payload<'a>) {
        self.inner.push(payload);
    }
}

enum Scan<'a, T: Comparator> {
    SeqScan {
        table: BPlusTree<T>,
    },
    IndexScan {
        table: BPlusTree<T>,
        index_schema: Schema,
        index: BPlusTree<T>,
        start: &'a [u8],
        end: &'a [u8],
    },
}

impl<'a, T> Scan<'a, T>
where
    T: Comparator,
{
    fn exec(&mut self, db: &mut Database) -> std::io::Result<ResultSet> {
        match self {
            Self::SeqScan { table } => {
                let mut collected = ResultSet::new();

                for item in table.iter()? {
                    match item {
                        Ok(payload) => {
                            collected.push(payload);
                        }
                        Err(e) => return Err(e),
                    }
                }

                Ok(collected)
            }
            Self::IndexScan {
                table,
                index,
                index_schema,
                start,
                end,
            } => {
                let mut collected = ResultSet::new();

                let direction = match index.compare(start, end)? {
                    std::cmp::Ordering::Equal => IterDirection::Forward,
                    std::cmp::Ordering::Less => IterDirection::Forward,
                    std::cmp::Ordering::Greater => IterDirection::Backward,
                };

                for item in index.iter_from(start, direction)? {
                    match item {
                        Ok(payload) => {
                            match index.compare(payload.as_ref(), end)? {
                                std::cmp::Ordering::Equal => {
                                    let tuple = TupleRef::read(payload.as_ref(), index_schema)?;
                                    let value = tuple.value(index_schema, 1)?;
                                    let result = table
                                        .search_from_root(value.as_ref(), NodeAccessMode::Read)?;
                                    if let Some(payload_table) =
                                        table.get_content_from_result(result)
                                    {
                                        collected.push(payload_table);
                                    };
                                    break;
                                }
                                std::cmp::Ordering::Less => {
                                    let tuple = TupleRef::read(payload.as_ref(), index_schema)?;
                                    let value = tuple.value(index_schema, 1)?;
                                    let result = table
                                        .search_from_root(value.as_ref(), NodeAccessMode::Read)?;
                                    if let Some(payload_table) =
                                        table.get_content_from_result(result)
                                    {
                                        collected.push(payload_table);
                                    };
                                    if matches!(direction, IterDirection::Backward) {
                                        break;
                                    };
                                }
                                std::cmp::Ordering::Greater => {
                                    let tuple = TupleRef::read(payload.as_ref(), index_schema)?;
                                    let value = tuple.value(index_schema, 1)?;
                                    let result = table
                                        .search_from_root(value.as_ref(), NodeAccessMode::Read)?;
                                    if let Some(payload_table) =
                                        table.get_content_from_result(result)
                                    {
                                        collected.push(payload_table);
                                    };

                                    if matches!(direction, IterDirection::Forward) {
                                        break;
                                    };
                                }
                            };
                        }
                        Err(e) => return Err(e),
                    }
                }

                Ok(collected)
            }
        }
    }
}
