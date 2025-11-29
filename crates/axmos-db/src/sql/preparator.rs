use crate::{
    database::{
        SharedCatalog,
        schema::{Column, Relation, Schema, Table},
    },
    sql::ast::{Expr, SelectItem, SelectStatement, Statement, TableReference, Values},
    transactions::worker::Worker,
    types::DataTypeKind,
};
use std::collections::{HashMap, HashSet};

struct PreparatorCtx {
    table_aliases: HashMap<String, String>,
    subqueries: HashMap<String, Relation>,
    current_table: Option<String>,
}

pub struct Preparator {
    catalog: SharedCatalog,
    worker: Worker,
    ctx: PreparatorCtx,
}

impl Preparator {
    pub fn new(catalog: SharedCatalog, worker: Worker) -> Self {
        Self {
            catalog,
            worker,
            ctx: PreparatorCtx {
                table_aliases: HashMap::new(),
                subqueries: HashMap::new(),
                current_table: None,
            },
        }
    }

    fn prepare_table_ref(&mut self, reference: &mut TableReference) {
        match reference {
            TableReference::Table { name, alias } => {
                let obj = self.get_relation(name);
                self.ctx.current_table = Some(name.to_string());

                if let Some(aliased) = alias {
                    self.ctx.table_aliases.insert(aliased.clone(), name.clone());
                } else {
                    self.ctx.table_aliases.insert(name.clone(), name.clone());
                };
            }
            TableReference::Join {
                left,
                join_type,
                right,
                on,
            } => {
                self.prepare_table_ref(left);
                self.prepare_table_ref(right);
                self.ctx.current_table = None;

                // Prepare the ON clause if it exists
                if let Some(on_expr) = on {
                    self.prepare_expr(on_expr);
                }
            }
            TableReference::Subquery { query, alias } => {
                let schema = self.prepare_select(query.as_mut());
                let obj = Relation::TableRel(Table::new(alias, crate::types::PAGE_ZERO, schema));
                self.ctx.subqueries.insert(alias.to_string(), obj);
            }
        }
    }

    fn prepare_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::Identifier(ident) => {
                // Convert simple identifier to qualified identifier when we have a current table
                if let Some(table) = &self.ctx.current_table {
                    *expr = Expr::QualifiedIdentifier {
                        table: table.clone(),
                        column: ident.clone(),
                    };
                }
            }
            Expr::BinaryOp { left, op, right } => {
                self.prepare_expr(left);
                self.prepare_expr(right);
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.prepare_expr(expr);
                self.prepare_expr(low);
                self.prepare_expr(high);
            }
            Expr::List(items) => {
                for item in items {
                    self.prepare_expr(item);
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.prepare_expr(op);
                }
                for clause in when_clauses {
                    self.prepare_expr(&mut clause.condition.clone());
                    self.prepare_expr(&mut clause.result.clone());
                }
                if let Some(else_expr) = else_clause {
                    self.prepare_expr(else_expr);
                }
            }
            Expr::UnaryOp { op, expr } => {
                self.prepare_expr(expr);
            }
            Expr::Exists(subquery) => {
                self.prepare_select(subquery.as_mut());
            }
            Expr::Subquery(subquery) => {
                self.prepare_select(subquery.as_mut());
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                for arg in args {
                    self.prepare_expr(arg);
                }
            }
            _ => {}
        }
    }

    fn prepare_select(&mut self, stmt: &mut SelectStatement) -> Schema {
        let mut out_columns = Vec::new();

        // Save the current context
        let saved_aliases = self.ctx.table_aliases.clone();
        let saved_current = self.ctx.current_table.clone();
        let saved_subqueries = self.ctx.subqueries.clone();

        if let Some(table_reference) = stmt.from.as_mut() {
            self.prepare_table_ref(table_reference);
        };

        let mut new_items = Vec::new();

        for item in stmt.columns.drain(..) {
            match item {
                SelectItem::Star => {
                    // Build ordered list of tables to process
                    let mut processed_tables = HashSet::new();

                    // First, add the main table if it exists and isn't aliased
                    if let Some(table) = self.ctx.current_table.as_ref()
                        && !self
                            .ctx
                            .table_aliases
                            .iter()
                            .any(|(alias, real)| real == table && alias != table)
                        && processed_tables.insert(table.clone())
                    {
                        let obj = self.get_relation(table);
                        let schema = obj.schema();

                        for column in &schema.columns {
                            out_columns.push(column.clone());
                            new_items.push(SelectItem::ExprWithAlias {
                                expr: Expr::QualifiedIdentifier {
                                    table: table.clone(),
                                    column: column.name.clone(),
                                },
                                alias: None,
                            });
                        }
                    }

                    // Then process aliased tables
                    for (alias, real_table) in self.ctx.table_aliases.iter() {
                        if processed_tables.insert(real_table.clone()) {
                            let obj = self.get_relation(real_table);
                            let schema = obj.schema();

                            for column in &schema.columns {
                                out_columns.push(column.clone());
                                new_items.push(SelectItem::ExprWithAlias {
                                    expr: Expr::QualifiedIdentifier {
                                        table: alias.clone(),
                                        column: column.name.clone(),
                                    },
                                    alias: None,
                                });
                            }
                        }
                    }

                    // Finally add subqueries
                    for (alias, subq) in self.ctx.subqueries.iter() {
                        let schema = subq.schema();
                        for column in &schema.columns {
                            out_columns.push(column.clone());
                            new_items.push(SelectItem::ExprWithAlias {
                                expr: Expr::QualifiedIdentifier {
                                    table: alias.clone(),
                                    column: column.name.clone(),
                                },
                                alias: None,
                            });
                        }
                    }
                }
                SelectItem::ExprWithAlias { mut expr, alias } => {
                    // Prepare the expression (convert identifiers to qualified identifiers)
                    self.prepare_expr(&mut expr);

                    // Get column metadata
                    let (table, new_column) = match &expr {
                        Expr::Identifier(value) => {
                            let table = self.ctx.current_table.as_ref().unwrap();
                            let obj = self.get_relation(table);
                            let schema = obj.schema();
                            let col = schema.column(value).unwrap().clone();
                            (table.clone(), col)
                        }
                        Expr::QualifiedIdentifier { table, column } => {
                            let real_table = self.ctx.table_aliases.get(table).unwrap_or(table);
                            let obj = self.get_relation(real_table);
                            let schema = obj.schema();
                            let col = schema.column(column).unwrap().clone();
                            (table.clone(), col)
                        }
                        _ => {
                            // For other expressions, create a synthetic column
                            let col = Column::new_unindexed(
                                DataTypeKind::Null, // Will be determined at runtime
                                alias.as_deref().unwrap_or("?"),
                                None,
                            );
                            ("".to_string(), col)
                        }
                    };

                    // Only update the expression if it's an identifier
                    if !table.is_empty() {
                        expr = Expr::QualifiedIdentifier {
                            table,
                            column: new_column.name.clone(),
                        };
                    }

                    new_items.push(SelectItem::ExprWithAlias {
                        expr,
                        alias: alias.clone(),
                    });
                    out_columns.push(new_column);
                }
            }
        }

        stmt.columns = new_items;

        // Prepare WHERE clause
        if let Some(where_clause) = &mut stmt.where_clause {
            self.prepare_expr(where_clause);
        }

        // Prepare ORDER BY
        for order_by in &mut stmt.order_by {
            self.prepare_expr(&mut order_by.expr);
        }

        // Prepare GROUP BY
        for group_by in &mut stmt.group_by {
            self.prepare_expr(group_by);
        }

        // Restore the context
        self.ctx.table_aliases = saved_aliases;
        self.ctx.current_table = saved_current;
        self.ctx.subqueries = saved_subqueries;

        Schema::from_columns(out_columns.as_slice(), 0)
    }

    pub fn prepare_stmt(&mut self, stmt: &mut Statement) {
        match stmt {
            Statement::Select(s) => {
                self.prepare_select(s);
            }
            Statement::With(with_stmt) => {
                // Prepare CTEs
                for (alias, cte) in &mut with_stmt.ctes {
                    let schema = self.prepare_select(cte);
                    let obj =
                        Relation::TableRel(Table::new(alias, crate::types::PAGE_ZERO, schema));
                    self.ctx.subqueries.insert(alias.clone(), obj);
                }
                // Prepare main body
                self.prepare_select(&mut with_stmt.body);
            }
            Statement::Insert(s) => {
                let obj = self.get_relation(&s.table);
                let schema = obj.schema();

                // Handle INSERT INTO ... SELECT
                if let Values::Query(select_stmt) = &mut s.values {
                    // First, prepare the SELECT statement to expand * and resolve columns
                    let select_schema = self.prepare_select(select_stmt);

                    // If no explicit columns specified in INSERT, use all columns obtained from the select
                    if s.columns.is_none() {
                        s.columns = Some(
                            select_schema
                                .columns
                                .iter()
                                .map(|c| c.name.to_string())
                                .collect(),
                        );
                    }

                    // Now reorder the SELECT columns to match the INSERT column order
                    let target_columns = s.columns.as_ref().unwrap();

                    // Create a mapping of column names to their positions in the SELECT result
                    let select_col_map: HashMap<String, usize> = select_schema
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, col)| (col.name.clone(), i))
                        .collect();

                    // Build new SELECT items in the order required by the INSERT
                    let mut reordered_items = Vec::new();

                    for target_col in target_columns {
                        if let Some(&select_idx) = select_col_map.get(target_col) {
                            // Get the corresponding item from the prepared SELECT
                            if select_idx < select_stmt.columns.len() {
                                reordered_items.push(select_stmt.columns[select_idx].clone());
                            }
                        } else {
                            // Column not found in SELECT.
                            panic!("Column {target_col} not found in SELECT result");
                        }
                    }

                    // Update the SELECT statement with reordered columns
                    select_stmt.columns = reordered_items;
                } else if let Values::Values(expr_list) = &mut s.values {
                    // Handle regular INSERT INTO ... VALUES
                    if let Some(stmt_cols) = s.columns.as_mut() {
                        let stmt_set: HashSet<_> = stmt_cols.iter().cloned().collect();
                        let index_map: HashMap<_, _> = stmt_cols
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (c.clone(), i))
                            .collect();

                        let new_order: Vec<_> = schema
                            .columns
                            .iter()
                            .filter(|c| stmt_set.contains(&c.name))
                            .map(|c| c.name.clone())
                            .collect();

                        *stmt_cols = new_order.clone();

                        for row in expr_list.iter_mut() {
                            let mut new_row = Vec::with_capacity(row.len());

                            for col_name in &new_order {
                                if let Some(&idx) = index_map.get(col_name)
                                    && idx < row.len()
                                {
                                    new_row.push(row[idx].clone());
                                }
                            }

                            *row = new_row;
                        }
                    }
                }
            }
            Statement::Delete(stmt) => {
                // Set up context for DELETE
                self.ctx
                    .table_aliases
                    .insert(stmt.table.clone(), stmt.table.clone());
                self.ctx.current_table = Some(stmt.table.clone());

                if let Some(where_clause) = &mut stmt.where_clause {
                    self.prepare_expr(where_clause);
                }
            }
            Statement::Update(stmt) => {
                // Set up context for UPDATE
                self.ctx
                    .table_aliases
                    .insert(stmt.table.clone(), stmt.table.clone());
                self.ctx.current_table = Some(stmt.table.clone());

                if let Some(where_clause) = &mut stmt.where_clause {
                    self.prepare_expr(where_clause);
                }

                // Prepare SET clauses
                for set_clause in &mut stmt.set_clauses {
                    self.prepare_expr(&mut set_clause.value);
                }
            }
            _ => {
                // Other statements don't need preparation
            }
        }
    }

    fn get_relation(&self, obj_name: &str) -> Relation {
        if let Some(subquery) = self.ctx.subqueries.get(obj_name) {
            return subquery.clone();
        };

        self.catalog
            .get_relation(obj_name, self.worker.clone())
            .unwrap()
    }
}

#[cfg(test)]
mod sql_prepare_tests {
    use crate::database::{Database, schema::Schema};
    use crate::io::pager::{Pager, SharedPager};
    use crate::sql::ast::{
        BinaryOperator, Expr, InsertStatement, JoinType, SelectItem, SelectStatement, Statement,
        TableReference, Values, WithStatement,
    };
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
    use crate::sql::preparator::Preparator;
    use crate::types::DataTypeKind;

    use crate::{AxmosDBConfig, IncrementalVaccum, TextEncoding};
    use serial_test::serial;
    use std::path::Path;

    fn create_db(
        page_size: u32,
        capacity: u16,
        path: impl AsRef<Path>,
    ) -> std::io::Result<Database> {
        let config = AxmosDBConfig {
            page_size,
            cache_size: Some(capacity),
            incremental_vacuum_mode: IncrementalVaccum::Disabled,
            min_keys: 3,
            text_encoding: TextEncoding::Utf8,
        };

        let pager = Pager::from_config(config, &path).unwrap();

        let db = Database::new(SharedPager::from(pager), 3, 2)?;

        // Create a users table
        let mut users_schema = Schema::new();
        users_schema.add_column("id", DataTypeKind::Int, true, true, false);
        users_schema.add_column("name", DataTypeKind::Text, false, false, false);
        users_schema.add_column("email", DataTypeKind::Text, false, true, false);
        users_schema.add_column("age", DataTypeKind::Int, false, false, true);
        users_schema.add_column("created_at", DataTypeKind::DateTime, false, false, false);
        let worker = db.main_worker_cloned();
        db.catalog()
            .create_table("users", users_schema, worker.clone())?;

        let mut posts_schema = Schema::new();
        posts_schema.add_column("id", DataTypeKind::Int, true, true, false);
        posts_schema.add_column("user_id", DataTypeKind::Int, false, false, false);
        posts_schema.add_column("title", DataTypeKind::Text, false, false, false);
        posts_schema.add_column("content", DataTypeKind::Blob, false, false, true);
        posts_schema.add_column("published", DataTypeKind::Boolean, false, false, false);

        db.catalog().create_table("posts", posts_schema, worker)?;

        Ok(db)
    }

    fn parse_and_prepare(sql: &str, db: &Database) -> Statement {
        let lexer = Lexer::new(sql);
        let mut parser = Parser::new(lexer);
        let mut statement = parser.parse().unwrap();

        let mut analyzer = Preparator::new(db.catalog(), db.main_worker_cloned());
        analyzer.prepare_stmt(&mut statement);
        statement
    }

    #[test]
    #[serial]
    fn test_preparator_1() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (name, id, email) VALUES ('John', 1, 'john@example.com')";

        let stmt = parse_and_prepare(sql, &db);
        assert_eq!(
            stmt,
            Statement::Insert(InsertStatement {
                table: "users".to_string(),
                columns: Some(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "email".to_string(),
                ],),
                values: Values::Values(vec![vec![
                    Expr::Number(1.0,),
                    Expr::String("John".to_string(),),
                    Expr::String("john@example.com".to_string(),),
                ],],),
            }),
        )
    }

    #[test]
    #[serial]
    fn test_preparator_2() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users (email, name, id) VALUES ('john@example.com','John', 1)";

        let stmt = parse_and_prepare(sql, &db);
        assert_eq!(
            stmt,
            Statement::Insert(InsertStatement {
                table: "users".to_string(),
                columns: Some(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "email".to_string(),
                ],),
                values: Values::Values(vec![vec![
                    Expr::Number(1.0,),
                    Expr::String("John".to_string(),),
                    Expr::String("john@example.com".to_string(),),
                ],],),
            }),
        )
    }

    #[test]
    #[serial]
    fn test_preparator_3() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "SELECT * FROM users";

        let stmt = parse_and_prepare(sql, &db);
        assert_eq!(
            stmt,
            Statement::Select(SelectStatement {
                distinct: false,
                columns: vec![
                    SelectItem::ExprWithAlias {
                        expr: Expr::QualifiedIdentifier {
                            table: "users".to_string(),
                            column: "id".to_string(),
                        },
                        alias: None,
                    },
                    SelectItem::ExprWithAlias {
                        expr: Expr::QualifiedIdentifier {
                            table: "users".to_string(),
                            column: "name".to_string(),
                        },
                        alias: None,
                    },
                    SelectItem::ExprWithAlias {
                        expr: Expr::QualifiedIdentifier {
                            table: "users".to_string(),
                            column: "email".to_string(),
                        },
                        alias: None,
                    },
                    SelectItem::ExprWithAlias {
                        expr: Expr::QualifiedIdentifier {
                            table: "users".to_string(),
                            column: "age".to_string(),
                        },
                        alias: None,
                    },
                    SelectItem::ExprWithAlias {
                        expr: Expr::QualifiedIdentifier {
                            table: "users".to_string(),
                            column: "created_at".to_string(),
                        },
                        alias: None,
                    },
                ],
                from: Some(TableReference::Table {
                    name: "users".to_string(),
                    alias: None,
                },),
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            },)
        )
    }

    #[test]
    #[serial]
    fn test_preparator_5() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users SELECT * FROM users; ";

        let stmt = parse_and_prepare(sql, &db);

        assert_eq!(
            stmt,
            Statement::Insert(InsertStatement {
                table: "users".to_string(),
                columns: Some(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "email".to_string(),
                    "age".to_string(),
                    "created_at".to_string(),
                ],),
                values: Values::Query(Box::new(SelectStatement {
                    distinct: false,
                    columns: vec![
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "users".to_string(),
                                column: "id".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "users".to_string(),
                                column: "name".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "users".to_string(),
                                column: "email".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "users".to_string(),
                                column: "age".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "users".to_string(),
                                column: "created_at".to_string(),
                            },
                            alias: None,
                        },
                    ],
                    from: Some(TableReference::Table {
                        name: "users".to_string(),
                        alias: None,
                    },),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),),
            },)
        );
    }

    #[test]
    #[serial]
    fn test_preparator_6() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "INSERT INTO users SELECT u.id, u.name FROM users u JOIN posts p ON u.id = p.user_id WHERE u.id IN (1,2,4); ";

        let stmt = parse_and_prepare(sql, &db);

        assert_eq!(
            stmt,
            Statement::Insert(InsertStatement {
                table: "users".to_string(),
                columns: Some(vec!["id".to_string(), "name".to_string(),],),
                values: Values::Query(Box::new(SelectStatement {
                    distinct: false,
                    columns: vec![
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "u".to_string(),
                                column: "id".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "u".to_string(),
                                column: "name".to_string(),
                            },
                            alias: None,
                        },
                    ],
                    from: Some(TableReference::Join {
                        left: Box::new(TableReference::Table {
                            name: "users".to_string(),
                            alias: Some("u".to_string(),),
                        }),
                        join_type: JoinType::Inner,
                        right: Box::new(TableReference::Table {
                            name: "posts".to_string(),
                            alias: Some("p".to_string(),),
                        }),
                        on: Some(Expr::BinaryOp {
                            left: Box::new(Expr::QualifiedIdentifier {
                                table: "u".to_string(),
                                column: "id".to_string(),
                            }),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::QualifiedIdentifier {
                                table: "p".to_string(),
                                column: "user_id".to_string(),
                            }),
                        },),
                    },),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::QualifiedIdentifier {
                            table: "u".to_string(),
                            column: "id".to_string(),
                        }),
                        op: BinaryOperator::In,
                        right: Box::new(Expr::List(vec![
                            Expr::Number(1.0,),
                            Expr::Number(2.0,),
                            Expr::Number(4.0,),
                        ],)),
                    },),
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                },)),
            },)
        );
    }

    #[test]
    #[serial]
    fn test_preparator_7() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = create_db(4096, 10, &path).unwrap();
        let sql = "WITH adults as (SELECT u.id, u.name FROM users u WHERE age > 10) SELECT * FROM adults;";

        let stmt = parse_and_prepare(sql, &db);
        assert_eq!(
            stmt,
            Statement::With(WithStatement {
                recursive: false,
                ctes: vec![(
                    "adults".to_string(),
                    SelectStatement {
                        distinct: false,
                        columns: vec![
                            SelectItem::ExprWithAlias {
                                expr: Expr::QualifiedIdentifier {
                                    table: "u".to_string(),
                                    column: "id".to_string(),
                                },
                                alias: None,
                            },
                            SelectItem::ExprWithAlias {
                                expr: Expr::QualifiedIdentifier {
                                    table: "u".to_string(),
                                    column: "name".to_string(),
                                },
                                alias: None,
                            },
                        ],
                        from: Some(TableReference::Table {
                            name: "users".to_string(),
                            alias: Some("u".to_string(),),
                        },),
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::QualifiedIdentifier {
                                table: "users".to_string(),
                                column: "age".to_string(),
                            }),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Number(10.0,)),
                        },),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                    },
                ),],
                body: Box::new(SelectStatement {
                    distinct: false,
                    columns: vec![
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "adults".to_string(),
                                column: "id".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "adults".to_string(),
                                column: "name".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "adults".to_string(),
                                column: "id".to_string(),
                            },
                            alias: None,
                        },
                        SelectItem::ExprWithAlias {
                            expr: Expr::QualifiedIdentifier {
                                table: "adults".to_string(),
                                column: "name".to_string(),
                            },
                            alias: None,
                        },
                    ],
                    from: Some(TableReference::Table {
                        name: "adults".to_string(),
                        alias: None,
                    },),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                }),
            },)
        )
    }
}
