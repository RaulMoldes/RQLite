use crate::sql::ast::*;
use crate::sql::parse_sql;
use crate::sql_test;
use crate::types::DataTypeKind;

sql_test!(
    test_parser_create_table_basic,
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
    Statement::CreateTable(CreateTableStatement {
        table: "users".to_string(),
        columns: vec![
            ColumnDefExpr {
                name: "id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![ColumnConstraintExpr::PrimaryKey]
            },
            ColumnDefExpr {
                name: "name".to_string(),
                data_type: DataTypeKind::Text,
                constraints: vec![ColumnConstraintExpr::NotNull]
            }
        ],
        constraints: vec![]
    })
);

sql_test!(
    test_parser_create_table_with_foreign_key,
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));",
    Statement::CreateTable(CreateTableStatement {
        table: "orders".to_string(),
        columns: vec![
            ColumnDefExpr {
                name: "id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![ColumnConstraintExpr::PrimaryKey]
            },
            ColumnDefExpr {
                name: "user_id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![ColumnConstraintExpr::ForeignKey {
                    table: "users".to_string(),
                    column: "id".to_string()
                }]
            }
        ],
        constraints: vec![]
    })
);

sql_test!(
    test_parser_create_table_with_check_constraint,
    "CREATE TABLE products (id INTEGER, price FLOAT CHECK (price > 0));",
    Statement::CreateTable(CreateTableStatement {
        table: "products".to_string(),
        columns: vec![
            ColumnDefExpr {
                name: "id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "price".to_string(),
                data_type: DataTypeKind::Float,
                constraints: vec![ColumnConstraintExpr::Check(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("price".to_string())),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Number(0.0))
                })]
            }
        ],
        constraints: vec![]
    })
);

sql_test!(
    test_parser_create_table_with_default,
    "CREATE TABLE posts (id INTEGER, status TEXT DEFAULT 'draft');",
    Statement::CreateTable(CreateTableStatement {
        table: "posts".to_string(),
        columns: vec![
            ColumnDefExpr {
                name: "id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "status".to_string(),
                data_type: DataTypeKind::Text,
                constraints: vec![ColumnConstraintExpr::Default(Expr::String("draft".to_string()))]
            }
        ],
        constraints: vec![]
    })
);

sql_test!(
    test_parser_create_table_with_table_constraints,
    "CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, PRIMARY KEY (order_id, product_id));",
    Statement::CreateTable(CreateTableStatement {
        table: "order_items".to_string(),
        columns: vec![
            ColumnDefExpr {
                name: "order_id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "product_id".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![]
            }
        ],
        constraints: vec![TableConstraintExpr::PrimaryKey(vec![
            "order_id".to_string(),
            "product_id".to_string()
        ])]
    })
);

sql_test!(
    test_parser_alter_table_add_column,
    "ALTER TABLE users ADD COLUMN email TEXT;",
    Statement::AlterTable(AlterTableStatement {
        table: "users".to_string(),
        action: AlterAction::AddColumn(ColumnDefExpr {
            name: "email".to_string(),
            data_type: DataTypeKind::Text,
            constraints: vec![]
        })
    })
);

sql_test!(
    test_parser_alter_table_drop_column,
    "ALTER TABLE users DROP COLUMN email;",
    Statement::AlterTable(AlterTableStatement {
        table: "users".to_string(),
        action: AlterAction::DropColumn("email".to_string())
    })
);

sql_test!(
    test_parser_alter_table_alter_column_type,
    "ALTER TABLE products ALTER COLUMN price FLOAT;",
    Statement::AlterTable(AlterTableStatement {
        table: "products".to_string(),
        action: AlterAction::AlterColumn(AlterColumnStatement {
            name: "price".to_string(),
            action: AlterColumnAction::SetDataType(DataTypeKind::Float)
        })
    })
);

sql_test!(
    test_parser_alter_table_set_default,
    "ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'pending';",
    Statement::AlterTable(AlterTableStatement {
        table: "orders".to_string(),
        action: AlterAction::AlterColumn(AlterColumnStatement {
            name: "status".to_string(),
            action: AlterColumnAction::SetDefault(Expr::String("pending".to_string()))
        })
    })
);

sql_test!(
    test_parser_alter_table_drop_default,
    "ALTER TABLE orders ALTER COLUMN status DROP DEFAULT;",
    Statement::AlterTable(AlterTableStatement {
        table: "orders".to_string(),
        action: AlterAction::AlterColumn(AlterColumnStatement {
            name: "status".to_string(),
            action: AlterColumnAction::DropDefault
        })
    })
);

sql_test!(
    test_parser_drop_table_simple,
    "DROP TABLE users;",
    Statement::DropTable(DropTableStatement {
        table: "users".to_string(),
        if_exists: false,
        cascade: false
    })
);

sql_test!(
    test_parser_drop_table_if_exists,
    "DROP TABLE IF EXISTS temp_data;",
    Statement::DropTable(DropTableStatement {
        table: "temp_data".to_string(),
        if_exists: true,
        cascade: false
    })
);

sql_test!(
    test_parser_drop_table_cascade,
    "DROP TABLE orders CASCADE;",
    Statement::DropTable(DropTableStatement {
        table: "orders".to_string(),
        if_exists: false,
        cascade: true
    })
);

sql_test!(
    test_parser_create_index_simple,
    "CREATE INDEX idx_user_email ON users (email);",
    Statement::CreateIndex(CreateIndexStatement {
        name: "idx_user_email".to_string(),
        table: "users".to_string(),
        columns: vec![IndexColumn {
            name: "email".to_string(),
            order: None
        }],
        unique: false,
        if_not_exists: false
    })
);

sql_test!(
    test_parser_create_unique_index,
    "CREATE UNIQUE INDEX idx_user_username ON users (username);",
    Statement::CreateIndex(CreateIndexStatement {
        name: "idx_user_username".to_string(),
        table: "users".to_string(),
        columns: vec![IndexColumn {
            name: "username".to_string(),
            order: None
        }],
        unique: true,
        if_not_exists: false
    })
);

sql_test!(
    test_parser_create_index_multi_column,
    "CREATE INDEX idx_orders_composite ON orders (user_id ASC, created_at DESC);",
    Statement::CreateIndex(CreateIndexStatement {
        name: "idx_orders_composite".to_string(),
        table: "orders".to_string(),
        columns: vec![
            IndexColumn {
                name: "user_id".to_string(),
                order: Some(OrderDirection::Asc)
            },
            IndexColumn {
                name: "created_at".to_string(),
                order: Some(OrderDirection::Desc)
            }
        ],
        unique: false,
        if_not_exists: false
    })
);

sql_test!(
    test_parser_begin_transaction,
    "BEGIN;",
    Statement::Transaction(TransactionStatement::Begin)
);

sql_test!(
    test_parser_begin_transaction_explicit,
    "BEGIN TRANSACTION;",
    Statement::Transaction(TransactionStatement::Begin)
);

sql_test!(
    test_parser_commit_transaction,
    "COMMIT;",
    Statement::Transaction(TransactionStatement::Commit)
);

sql_test!(
    test_parser_rollback_transaction,
    "ROLLBACK;",
    Statement::Transaction(TransactionStatement::Rollback)
);

sql_test!(
    test_parser_data_types,
    "CREATE TABLE test_parser_types (
        col_int INTEGER,
        col_bigint BIGINT,
        col_smallint SMALLINT,
        col_float FLOAT,
        col_double DOUBLE,
        col_char CHAR,
        col_text TEXT,
        col_date DATE,
        col_time TIME,
        col_boolean BOOLEAN,
        col_blob BLOB
    );",
    Statement::CreateTable(CreateTableStatement {
        table: "test_parser_types".to_string(),
        columns: vec![
            ColumnDefExpr {
                name: "col_int".to_string(),
                data_type: DataTypeKind::Int,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_bigint".to_string(),
                data_type: DataTypeKind::BigInt,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_smallint".to_string(),
                data_type: DataTypeKind::SmallInt,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_float".to_string(),
                data_type: DataTypeKind::Float,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_double".to_string(),
                data_type: DataTypeKind::Double,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_char".to_string(),
                data_type: DataTypeKind::Char,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_text".to_string(),
                data_type: DataTypeKind::Text,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_date".to_string(),
                data_type: DataTypeKind::Date,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_time".to_string(),
                data_type: DataTypeKind::DateTime,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_boolean".to_string(),
                data_type: DataTypeKind::Boolean,
                constraints: vec![]
            },
            ColumnDefExpr {
                name: "col_blob".to_string(),
                data_type: DataTypeKind::Blob,
                constraints: vec![]
            }
        ],
        constraints: vec![]
    })
);
