use crate::sql::ast::*;
use crate::sql::parse_sql;
use crate::sql_test;

sql_test!(
    test_parser_select_star_from_table,
    "SELECT * FROM my_table;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
    })
);

sql_test!(
    test_parser_select_star_from_table_with_filter,
    "SELECT * FROM my_table WHERE COL1 > 2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("COL1".to_string())),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Number(2.0))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
    })
);

sql_test!(
    test_parser_select_star_from_table_with_multi_filter,
    "SELECT * FROM my_table WHERE COL1 > 2 AND COL2 < 3;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("COL1".to_string())),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Number(2.0))
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("COL2".to_string())),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Number(3.0))
            })
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
    })
);

sql_test!(
    test_parser_select_single_column,
    "SELECT col1 FROM my_table;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::Identifier("col1".to_string()),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_distinct_single_column,
    "SELECT DISTINCT col1 FROM my_table;",
    Statement::Select(SelectStatement {
        distinct: true,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::Identifier("col1".to_string()),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_multiple_columns,
    "SELECT (col1, col2, col3) FROM my_table;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_group_by,
    "SELECT (MAX(col1), col2) FROM my_table GROUP BY col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::FunctionCall {
                    name: "MAX".to_string(),
                    args: vec![Expr::Identifier("col1".to_string())],
                    distinct: false
                },
                Expr::Identifier("col2".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![Expr::Identifier("col2".to_string())],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_group_by_having_clause,
    "SELECT (MAX(col1), col2) FROM my_table GROUP BY col2 HAVING COUNT(*) < 1;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::FunctionCall {
                    name: "MAX".to_string(),
                    args: vec![Expr::Identifier("col1".to_string())],
                    distinct: false
                },
                Expr::Identifier("col2".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "my_table".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![Expr::Identifier("col2".to_string())],
        having: Some(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "COUNT".to_string(),
                args: vec![Expr::Star],
                distinct: false
            }),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Number(1.0))
        }),
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_casting_op,
    "SELECT CAST(3, STRING);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::FunctionCall {
                name: "CAST".to_string(),
                args: vec![Expr::Number(3.0), Expr::Identifier("STRING".to_string())],
                distinct: false
            },
            alias: None
        }],
        from: None,
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_order_by_limit,
    "SELECT (col1, col2, col3, col4) FROM table1 ORDER BY col1 LIMIT 100;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "table1".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: Expr::Identifier("col1".to_string()),
            asc: true
        }],
        limit: Some(100)
    })
);

sql_test!(
    test_parser_select_order_by_desc,
    "SELECT (col1, col2, col3, col4) FROM table1 ORDER BY col1 DESC LIMIT 100;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "table1".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: Expr::Identifier("col1".to_string()),
            asc: false
        }],
        limit: Some(100)
    })
);

sql_test!(
    test_parser_select_simple_join,
    "SELECT (col1, col2, col3, col4) FROM table1 a JOIN table2 b on a.col1 = b.col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Table {
                name: "table1".to_string(),
                alias: Some("a".to_string())
            }),
            join_type: JoinType::Inner,
            right: Box::new(TableReference::Table {
                name: "table2".to_string(),
                alias: Some("b".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col1".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "b".to_string(),
                    column: "col2".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_inner_join,
    "SELECT (col1, col2, col3, col4) FROM table1 a INNER JOIN table2 b on a.col1 = b.col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Table {
                name: "table1".to_string(),
                alias: Some("a".to_string())
            }),
            join_type: JoinType::Inner,
            right: Box::new(TableReference::Table {
                name: "table2".to_string(),
                alias: Some("b".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col1".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "b".to_string(),
                    column: "col2".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_left_join,
    "SELECT (col1, col2, col3, col4) FROM table1 a LEFT JOIN table2 b on a.col1 = b.col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Table {
                name: "table1".to_string(),
                alias: Some("a".to_string())
            }),
            join_type: JoinType::Left,
            right: Box::new(TableReference::Table {
                name: "table2".to_string(),
                alias: Some("b".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col1".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "b".to_string(),
                    column: "col2".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_right_join,
    "SELECT (col1, col2, col3, col4) FROM table1 a RIGHT JOIN table2 b on a.col1 = b.col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Table {
                name: "table1".to_string(),
                alias: Some("a".to_string())
            }),
            join_type: JoinType::Right,
            right: Box::new(TableReference::Table {
                name: "table2".to_string(),
                alias: Some("b".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col1".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "b".to_string(),
                    column: "col2".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_full_join,
    "SELECT (col1, col2, col3, col4) FROM table1 a FULL JOIN table2 b on a.col1 = b.col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Table {
                name: "table1".to_string(),
                alias: Some("a".to_string())
            }),
            join_type: JoinType::Full,
            right: Box::new(TableReference::Table {
                name: "table2".to_string(),
                alias: Some("b".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col1".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "b".to_string(),
                    column: "col2".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_cross_join,
    "SELECT (col1, col2, col3, col4) FROM table1 a CROSS JOIN table2 b on a.col1 = b.col2;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Table {
                name: "table1".to_string(),
                alias: Some("a".to_string())
            }),
            join_type: JoinType::Cross,
            right: Box::new(TableReference::Table {
                name: "table2".to_string(),
                alias: Some("b".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col1".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "b".to_string(),
                    column: "col2".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_multijoin,
    "SELECT (col1, col2, col3, col4) FROM table1 a JOIN table2 b on a.col1 = b.col2 JOIN table3 c on c.col3 = a.col3;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::List(vec![
                Expr::Identifier("col1".to_string()),
                Expr::Identifier("col2".to_string()),
                Expr::Identifier("col3".to_string()),
                Expr::Identifier("col4".to_string())
            ]),
            alias: None
        }],
        from: Some(TableReference::Join {
            left: Box::new(TableReference::Join {
                left: Box::new(TableReference::Table {
                    name: "table1".to_string(),
                    alias: Some("a".to_string())
                }),
                join_type: JoinType::Inner,
                right: Box::new(TableReference::Table {
                    name: "table2".to_string(),
                    alias: Some("b".to_string())
                }),
                on: Some(Expr::BinaryOp {
                    left: Box::new(Expr::QualifiedIdentifier {
                        table: "a".to_string(),
                        column: "col1".to_string()
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::QualifiedIdentifier {
                        table: "b".to_string(),
                        column: "col2".to_string()
                    })
                })
            }),
            join_type: JoinType::Inner,
            right: Box::new(TableReference::Table {
                name: "table3".to_string(),
                alias: Some("c".to_string())
            }),
            on: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "c".to_string(),
                    column: "col3".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "a".to_string(),
                    column: "col3".to_string()
                })
            })
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_subquery_simple,
    "WITH t1 AS (SELECT * FROM my_table) SELECT * FROM t1;",
    Statement::With(WithStatement {
        recursive: false,
        ctes: vec![(
            "t1".to_string(),
            SelectStatement {
                distinct: false,
                columns: vec![SelectItem::Star],
                from: Some(TableReference::Table {
                    name: "my_table".to_string(),
                    alias: None
                }),
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None
            }
        )],
        body: Box::new(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Star],
            from: Some(TableReference::Table {
                name: "t1".to_string(),
                alias: None
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None
        })
    })
);

sql_test!(
    test_parser_select_multiple_subqueries,
    "WITH t1 as (SELECT * FROM my_table), t2 as (SELECT * FROM my_other_table) SELECT * FROM t1 JOIN t2 ON t1 = t2;",
    Statement::With(WithStatement {
        recursive: false,
        ctes: vec![
            (
                "t1".to_string(),
                SelectStatement {
                    distinct: false,
                    columns: vec![SelectItem::Star],
                    from: Some(TableReference::Table {
                        name: "my_table".to_string(),
                        alias: None
                    }),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None
                }
            ),
            (
                "t2".to_string(),
                SelectStatement {
                    distinct: false,
                    columns: vec![SelectItem::Star],
                    from: Some(TableReference::Table {
                        name: "my_other_table".to_string(),
                        alias: None
                    }),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None
                }
            )
        ],
        body: Box::new(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Star],
            from: Some(TableReference::Join {
                left: Box::new(TableReference::Table {
                    name: "t1".to_string(),
                    alias: None
                }),
                join_type: JoinType::Inner,
                right: Box::new(TableReference::Table {
                    name: "t2".to_string(),
                    alias: None
                }),
                on: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("t2".to_string()))
                })
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None
        })
    })
);

sql_test!(
    test_parser_case_expression_simple,
    "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END FROM users;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::Case {
                operand: Some(Box::new(Expr::Identifier("status".to_string()))),
                when_clauses: vec![
                    WhenClause {
                        condition: Expr::String("active".to_string()),
                        result: Expr::Number(1.0)
                    },
                    WhenClause {
                        condition: Expr::String("inactive".to_string()),
                        result: Expr::Number(0.0)
                    }
                ],
                else_clause: Some(Box::new(Expr::Number(-1.0)))
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_case_expression_searched,
    "SELECT CASE WHEN age < 18 THEN 'minor' WHEN age >= 65 THEN 'senior' ELSE 'adult' END FROM people;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::Case {
                operand: None,
                when_clauses: vec![
                    WhenClause {
                        condition: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("age".to_string())),
                            op: BinaryOperator::Lt,
                            right: Box::new(Expr::Number(18.0))
                        },
                        result: Expr::String("minor".to_string())
                    },
                    WhenClause {
                        condition: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("age".to_string())),
                            op: BinaryOperator::Ge,
                            right: Box::new(Expr::Number(65.0))
                        },
                        result: Expr::String("senior".to_string())
                    }
                ],
                else_clause: Some(Box::new(Expr::String("adult".to_string())))
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "people".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_between_expression,
    "SELECT * FROM products WHERE price BETWEEN 10 AND 100;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "products".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::Between {
            expr: Box::new(Expr::Identifier("price".to_string())),
            negated: false,
            low: Box::new(Expr::Number(10.0)),
            high: Box::new(Expr::Number(100.0))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_in_expression_list,
    "SELECT * FROM users WHERE id IN (1, 2, 3);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![
                Expr::Number(1.0),
                Expr::Number(2.0),
                Expr::Number(3.0)
            ]))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_in_expression_subquery,
    "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = true);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "orders".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("user_id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::Subquery(Box::new(SelectStatement {
                distinct: false,
                columns: vec![SelectItem::ExprWithAlias {
                    expr: Expr::Identifier("id".to_string()),
                    alias: None
                }],
                from: Some(TableReference::Table {
                    name: "users".to_string(),
                    alias: None
                }),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("active".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Boolean(true))
                }),
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None
            })))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_exists_expression,
    "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::Exists(Box::new(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::ExprWithAlias {
                expr: Expr::Number(1.0),
                alias: None
            }],
            from: Some(TableReference::Table {
                name: "orders".to_string(),
                alias: None
            }),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::QualifiedIdentifier {
                    table: "orders".to_string(),
                    column: "user_id".to_string()
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::QualifiedIdentifier {
                    table: "users".to_string(),
                    column: "id".to_string()
                })
            }),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None
        }))),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_is_null_expression,
    "SELECT * FROM users WHERE email IS NULL;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("email".to_string())),
            op: BinaryOperator::Is,
            right: Box::new(Expr::Null)
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_is_not_null_expression,
    "SELECT * FROM users WHERE email IS NOT NULL;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("email".to_string())),
            op: BinaryOperator::IsNot,
            right: Box::new(Expr::Null)
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_like_expression,
    "SELECT * FROM users WHERE name LIKE 'John%';",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("name".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("John%".to_string()))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_concat_operator,
    "SELECT first_name || ' ' || last_name FROM users;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("first_name".to_string())),
                    op: BinaryOperator::Concat,
                    right: Box::new(Expr::String(" ".to_string()))
                }),
                op: BinaryOperator::Concat,
                right: Box::new(Expr::Identifier("last_name".to_string()))
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_arithmetic_operations,
    "SELECT price * 1.1 + 5 FROM products;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("price".to_string())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Number(1.1))
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Number(5.0))
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "products".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_modulo_operator,
    "SELECT id % 2 FROM users;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Modulo,
                right: Box::new(Expr::Number(2.0))
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_unary_minus,
    "SELECT -price FROM products;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier("price".to_string()))
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "products".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_not_operator,
    "SELECT * FROM users WHERE NOT active;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Identifier("active".to_string()))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_column_alias_with_as,
    "SELECT user_id AS id FROM orders;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::Identifier("user_id".to_string()),
            alias: Some("id".to_string())
        }],
        from: Some(TableReference::Table {
            name: "orders".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_column_alias_without_as,
    "SELECT user_id id FROM orders;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::Identifier("user_id".to_string()),
            alias: Some("id".to_string())
        }],
        from: Some(TableReference::Table {
            name: "orders".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_subquery_in_from,
    "SELECT * FROM (SELECT id FROM users) AS u;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Subquery {
            query: Box::new(SelectStatement {
                distinct: false,
                columns: vec![SelectItem::ExprWithAlias {
                    expr: Expr::Identifier("id".to_string()),
                    alias: None
                }],
                from: Some(TableReference::Table {
                    name: "users".to_string(),
                    alias: None
                }),
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None
            }),
            alias: "u".to_string()
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_in,
    "SELECT * FROM table1 WHERE Value IN (1,2);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "table1".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("Value".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![Expr::Number(1.0), Expr::Number(2.0)]))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_not_in,
    "SELECT * FROM table1 WHERE Value NOT IN (1,2);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "table1".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("Value".to_string())),
            op: BinaryOperator::NotIn,
            right: Box::new(Expr::List(vec![Expr::Number(1.0), Expr::Number(2.0)]))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_like,
    "SELECT * FROM table1 WHERE Value LIKE '%hello%';",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "table1".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("Value".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("%hello%".to_string()))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_select_not_like,
    "SELECT * FROM table1 WHERE Value NOT LIKE '%hello%';",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "table1".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("Value".to_string())),
            op: BinaryOperator::NotLike,
            right: Box::new(Expr::String("%hello%".to_string()))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);
sql_test!(
    test_parser_count_distinct,
    "SELECT COUNT(DISTINCT user_id) FROM orders;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::FunctionCall {
                name: "COUNT".to_string(),
                args: vec![Expr::Identifier("user_id".to_string())],
                distinct: true
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "orders".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_sum_distinct,
    "SELECT SUM(DISTINCT amount) FROM payments;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::ExprWithAlias {
            expr: Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![Expr::Identifier("amount".to_string())],
                distinct: true
            },
            alias: None
        }],
        from: Some(TableReference::Table {
            name: "payments".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_multiple_aggregates,
    "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Star],
                    distinct: false
                },
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Identifier("amount".to_string())],
                    distinct: false
                },
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "AVG".to_string(),
                    args: vec![Expr::Identifier("amount".to_string())],
                    distinct: false
                },
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "MIN".to_string(),
                    args: vec![Expr::Identifier("amount".to_string())],
                    distinct: false
                },
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "MAX".to_string(),
                    args: vec![Expr::Identifier("amount".to_string())],
                    distinct: false
                },
                alias: None
            }
        ],
        from: Some(TableReference::Table {
            name: "orders".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_order_by_multiple_columns,
    "SELECT * FROM users ORDER BY last_name ASC, first_name DESC;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![
            OrderByExpr {
                expr: Expr::Identifier("last_name".to_string()),
                asc: true
            },
            OrderByExpr {
                expr: Expr::Identifier("first_name".to_string()),
                asc: false
            }
        ],
        limit: None
    })
);

sql_test!(
    test_parser_order_by_expression,
    "SELECT * FROM products ORDER BY price * discount DESC;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "products".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("price".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Identifier("discount".to_string()))
            },
            asc: false
        }],
        limit: None
    })
);

sql_test!(
    test_parser_group_by_multiple_columns,
    "SELECT category, brand, COUNT(*) FROM products GROUP BY category, brand;",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier("category".to_string()),
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier("brand".to_string()),
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Star],
                    distinct: false
                },
                alias: None
            }
        ],
        from: Some(TableReference::Table {
            name: "products".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![
            Expr::Identifier("category".to_string()),
            Expr::Identifier("brand".to_string())
        ],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_group_by_expression,
    "SELECT YEAR(date), COUNT(*) FROM orders GROUP BY YEAR(date);",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "YEAR".to_string(),
                    args: vec![Expr::Identifier("date".to_string())],
                    distinct: false
                },
                alias: None
            },
            SelectItem::ExprWithAlias {
                expr: Expr::FunctionCall {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Star],
                    distinct: false
                },
                alias: None
            }
        ],
        from: Some(TableReference::Table {
            name: "orders".to_string(),
            alias: None
        }),
        where_clause: None,
        group_by: vec![Expr::FunctionCall {
            name: "YEAR".to_string(),
            args: vec![Expr::Identifier("date".to_string())],
            distinct: false
        }],
        having: None,
        order_by: vec![],
        limit: None
    })
);

sql_test!(
    test_parser_with_recursive,
    "WITH RECURSIVE cte AS (SELECT 1 AS n) SELECT * FROM cte;",
    Statement::With(WithStatement {
        recursive: true,
        ctes: vec![(
            "cte".to_string(),
            SelectStatement {
                distinct: false,
                columns: vec![SelectItem::ExprWithAlias {
                    expr: Expr::Number(1.0),
                    alias: Some("n".to_string())
                }],
                from: None,
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None
            }
        )],
        body: Box::new(SelectStatement {
            distinct: false,
            columns: vec![SelectItem::Star],
            from: Some(TableReference::Table {
                name: "cte".to_string(),
                alias: None
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None
        })
    })
);

sql_test!(
    test_parser_with_multiple_ctes_complex,
    "WITH
        active_users AS (SELECT * FROM users WHERE active = true),
        recent_orders AS (SELECT * FROM orders WHERE date > '2024-01-01')
    SELECT u.name, COUNT(o.id)
    FROM active_users u
    JOIN recent_orders o ON u.id = o.user_id
    GROUP BY u.name;",
    Statement::With(WithStatement {
        recursive: false,
        ctes: vec![
            (
                "active_users".to_string(),
                SelectStatement {
                    distinct: false,
                    columns: vec![SelectItem::Star],
                    from: Some(TableReference::Table {
                        name: "users".to_string(),
                        alias: None
                    }),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("active".to_string())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Boolean(true))
                    }),
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None
                }
            ),
            (
                "recent_orders".to_string(),
                SelectStatement {
                    distinct: false,
                    columns: vec![SelectItem::Star],
                    from: Some(TableReference::Table {
                        name: "orders".to_string(),
                        alias: None
                    }),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("date".to_string())),
                        op: BinaryOperator::Gt,
                        right: Box::new(Expr::String("2024-01-01".to_string()))
                    }),
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None
                }
            )
        ],
        body: Box::new(SelectStatement {
            distinct: false,
            columns: vec![
                SelectItem::ExprWithAlias {
                    expr: Expr::QualifiedIdentifier {
                        table: "u".to_string(),
                        column: "name".to_string()
                    },
                    alias: None
                },
                SelectItem::ExprWithAlias {
                    expr: Expr::FunctionCall {
                        name: "COUNT".to_string(),
                        args: vec![Expr::QualifiedIdentifier {
                            table: "o".to_string(),
                            column: "id".to_string()
                        }],
                        distinct: false
                    },
                    alias: None
                }
            ],
            from: Some(TableReference::Join {
                left: Box::new(TableReference::Table {
                    name: "active_users".to_string(),
                    alias: Some("u".to_string())
                }),
                join_type: JoinType::Inner,
                right: Box::new(TableReference::Table {
                    name: "recent_orders".to_string(),
                    alias: Some("o".to_string())
                }),
                on: Some(Expr::BinaryOp {
                    left: Box::new(Expr::QualifiedIdentifier {
                        table: "u".to_string(),
                        column: "id".to_string()
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::QualifiedIdentifier {
                        table: "o".to_string(),
                        column: "user_id".to_string()
                    })
                })
            }),
            where_clause: None,
            group_by: vec![Expr::QualifiedIdentifier {
                table: "u".to_string(),
                column: "name".to_string()
            }],
            having: None,
            order_by: vec![],
            limit: None
        })
    })
);

sql_test!(
    test_parser_nested_subqueries,
    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > 100));",
    Statement::Select(SelectStatement {
        distinct: false,
        columns: vec![SelectItem::Star],
        from: Some(TableReference::Table {
            name: "users".to_string(),
            alias: None
        }),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::Subquery(Box::new(SelectStatement {
                distinct: false,
                columns: vec![SelectItem::ExprWithAlias {
                    expr: Expr::Identifier("user_id".to_string()),
                    alias: None
                }],
                from: Some(TableReference::Table {
                    name: "orders".to_string(),
                    alias: None
                }),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("product_id".to_string())),
                    op: BinaryOperator::In,
                    right: Box::new(Expr::Subquery(Box::new(SelectStatement {
                        distinct: false,
                        columns: vec![SelectItem::ExprWithAlias {
                            expr: Expr::Identifier("id".to_string()),
                            alias: None
                        }],
                        from: Some(TableReference::Table {
                            name: "products".to_string(),
                            alias: None
                        }),
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("price".to_string())),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Number(100.0))
                        }),
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None
                    })))
                }),
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None
            })))
        }),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None
    })
);
