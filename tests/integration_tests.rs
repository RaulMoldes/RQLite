
//! # Integration Tests for RQLite Storage Engine
//!
//! These tests verify that all components of the storage engine work together
//! correctly in realistic scenarios.
//! 
//! Currently identified issues:
//! 
//! * Overflow pages are not being created correctly when a single record exceeds the page size.
//! 
//! * Cloning the pager for each table is very inefficient and should be optimized. --> Currently i fixed it with an Arc to create a shared reference instead but i think it can be done better.
//! 
//! *  Although it has been designed to be thread-safe, the current implementation is not completely transaction-serializable. A more robust transaction management system is needed to ensure that concurrent transactions do not interfere with each other.
//! 
//! * WAL logging and journal recovery are not yet implemented, which means that the database is not durable against crashes or power failures.
//! 
//! * The catalog is also not durable yet, which means that the database schema (tables, indexes, etc.) is not persisted across restarts.
//! 
//! I am not still a database or Rust expert so I know this can be done much better. 
//! Anyway, I am happy with the current state of the engine and I will continue to improve it in the future.

use rqlite_engine::{RQLite, RQLiteConfig, Record, SqliteValue, KeyValue};
use rqlite_engine::utils::serialization::{serialize_values};
use std::collections::HashMap;
use tempfile::tempdir;

/// Test basic database lifecycle - create, use, close, reopen
#[test]
#[allow(unused_variables)]
fn test_database_lifecycle() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("lifecycle_test.db");

    // Phase 1: Create database and populate with data
    let table_id = {
        let mut db = RQLite::create(&db_path, None).unwrap();
        
        let table_id = db.create_table().unwrap();
        
        // Insert test records
        let records = vec![
            (1, Record::with_values(vec![
                SqliteValue::Integer(100),
                SqliteValue::String("Alice".to_string()),
                SqliteValue::Float(25.5),
            ])),
            (2, Record::with_values(vec![
                SqliteValue::Integer(200),
                SqliteValue::String("Bob".to_string()),
                SqliteValue::Float(30.0),
            ])),
            (3, Record::with_values(vec![
                SqliteValue::Integer(300),
                SqliteValue::String("Charlie".to_string()),
                SqliteValue::Float(35.7),
            ])),
        ];

        for (rowid, record) in &records {
            db.table_insert(table_id, *rowid, record).unwrap();
        }

        // Verify records can be found
        for (rowid, expected_record) in &records {
            let found = db.table_find(table_id, *rowid).unwrap().unwrap();
            assert_eq!(found.len(), expected_record.len());
        }

        db.flush().unwrap();
        table_id
    };

    // Phase 2: Reopen database and verify data persists
    // Note: In a complete implementation, we would load table metadata from system tables
    // For now, we just verify the database file exists and can be opened
    {
        let _db = RQLite::open(&db_path, None).unwrap();
        // In a full implementation, we would verify that table_id still exists
        // and contains the same data
    }
}

/// Test complex scenarios with multiple tables and indexes
#[test]
fn test_multi_table_complex_scenario() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("multi_table_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    // Create tables for a simple e-commerce scenario
    let users_table = db.create_table().unwrap();
    let products_table = db.create_table().unwrap();
    let orders_table = db.create_table().unwrap();

    // Create indexes
    let users_email_index = db.create_index(users_table).unwrap();
    let products_name_index = db.create_index(products_table).unwrap();
    let orders_user_index = db.create_index(orders_table).unwrap();

    // Insert users
    let users = vec![
        (1, "alice@example.com", "Alice Johnson"),
        (2, "bob@example.com", "Bob Smith"), 
        (3, "charlie@example.com", "Charlie Brown"),
    ];

    for (user_id, email, name) in &users {
        let record = Record::with_values(vec![
            SqliteValue::Integer(*user_id),
            SqliteValue::String(email.to_string()),
            SqliteValue::String(name.to_string()),
        ]);
        db.table_insert(users_table, *user_id, &record).unwrap();

        // Index by email
        let mut email_payload = Vec::new();
        serialize_values(&[SqliteValue::String(email.to_string())], &mut email_payload).unwrap();
        db.index_insert(users_email_index, &email_payload, *user_id).unwrap();
    }

    // Insert products
    let products = vec![
        (1, "Laptop", 999.99),
        (2, "Mouse", 29.99),
        (3, "Keyboard", 79.99),
        (4, "Monitor", 299.99),
    ];

    for (product_id, name, price) in &products {
        let record = Record::with_values(vec![
            SqliteValue::Integer(*product_id),
            SqliteValue::String(name.to_string()),
            SqliteValue::Float(*price),
        ]);
        db.table_insert(products_table, *product_id, &record).unwrap();

        // Index by name
        let mut name_payload = Vec::new();
        serialize_values(&[SqliteValue::String(name.to_string())], &mut name_payload).unwrap();
        db.index_insert(products_name_index, &name_payload, *product_id).unwrap();
    }

    // Insert orders
    let orders = vec![
        (1, 1, 1), // Alice bought Laptop
        (2, 1, 2), // Alice bought Mouse
        (3, 2, 3), // Bob bought Keyboard
        (4, 3, 1), // Charlie bought Laptop
        (5, 3, 4), // Charlie bought Monitor
    ];

    for (order_id, user_id, product_id) in &orders {
        let record = Record::with_values(vec![
            SqliteValue::Integer(*order_id),
            SqliteValue::Integer(*user_id),
            SqliteValue::Integer(*product_id),
        ]);
        db.table_insert(orders_table, *order_id, &record).unwrap();

        // Index by user_id
        let mut user_payload = Vec::new();
        serialize_values(&[SqliteValue::Integer(*user_id)], &mut user_payload).unwrap();
        db.index_insert(orders_user_index, &user_payload, *order_id).unwrap();
    }

    // Verify data integrity
    // Check that all users exist
    for (user_id, _, _) in &users {
        let user = db.table_find(users_table, *user_id).unwrap();
        assert!(user.is_some());
    }

    // Check that all products exist
    for (product_id, _, _) in &products {
        let product = db.table_find(products_table, *product_id).unwrap();
        assert!(product.is_some());
    }

    // Check that all orders exist
    for (order_id, _, _) in &orders {
        let order = db.table_find(orders_table, *order_id).unwrap();
        assert!(order.is_some());
    }

    // Test index searches
    // Find user by email
    let alice_email_key = KeyValue::String("alice@example.com".to_string());
    let (found, _, _) = db.index_find(users_email_index, &alice_email_key).unwrap();
    assert!(found);

    // Find product by name
    let laptop_name_key = KeyValue::String("Laptop".to_string());
    let (found, _, _) = db.index_find(products_name_index, &laptop_name_key).unwrap();
    assert!(found);

    // Find orders by user
    let user_1_key = KeyValue::Integer(1);
    let (found, _, _) = db.index_find(orders_user_index, &user_1_key).unwrap();
    assert!(found);
}

/// Test transaction rollback and commit scenarios
#[test]
fn test_transaction_scenarios() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("transaction_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    let table_id = db.create_table().unwrap();
    
    // Scenario 1: Successful transaction
    db.begin_transaction().unwrap();
    
    let record1 = Record::with_values(vec![
        SqliteValue::Integer(1),
        SqliteValue::String("Committed Record".to_string()),
    ]);
    db.table_insert(table_id, 1, &record1).unwrap();
    
    let record2 = Record::with_values(vec![
        SqliteValue::Integer(2),
        SqliteValue::String("Another Committed Record".to_string()),
    ]);
    db.table_insert(table_id, 2, &record2).unwrap();
    
    db.commit_transaction().unwrap();
    
    // Verify records exist after commit
    assert!(db.table_find(table_id, 1).unwrap().is_some());
    assert!(db.table_find(table_id, 2).unwrap().is_some());
    
    // Scenario 2: Rolled back transaction
    db.begin_transaction().unwrap();
    
    let record3 = Record::with_values(vec![
        SqliteValue::Integer(3),
        SqliteValue::String("Rolled Back Record".to_string()),
    ]);
    db.table_insert(table_id, 3, &record3).unwrap();
    
    // Verify record exists before rollback
    assert!(db.table_find(table_id, 3).unwrap().is_some());
    
    db.rollback_transaction().unwrap();
    
    // Verify record is gone after rollback
    assert!(db.table_find(table_id, 3).unwrap().is_none());
    
    // Verify previously committed records still exist
    assert!(db.table_find(table_id, 1).unwrap().is_some());
    assert!(db.table_find(table_id, 2).unwrap().is_some());
    
    // Scenario 3: Multiple operations in transaction
    db.begin_transaction().unwrap();
    
    // Insert new record
    let record4 = Record::with_values(vec![
        SqliteValue::Integer(4),
        SqliteValue::String("New Record".to_string()),
    ]);
    db.table_insert(table_id, 4, &record4).unwrap();
    
    // Update existing record (delete and insert)
    db.table_delete(table_id, 1).unwrap();
    let updated_record1 = Record::with_values(vec![
        SqliteValue::Integer(1),
        SqliteValue::String("Updated Record".to_string()),
    ]);
    db.table_insert(table_id, 1, &updated_record1).unwrap();
    
    db.commit_transaction().unwrap();
    
    // Verify all changes were committed
    assert!(db.table_find(table_id, 4).unwrap().is_some());
    let found_record1 = db.table_find(table_id, 1).unwrap().unwrap();
    match &found_record1.values[1] {
        SqliteValue::String(s) => assert_eq!(s, "Updated Record"),
        _ => panic!("Expected string"),
    }
}

/// Test large dataset operations and B-Tree splitting
#[test]
fn test_large_dataset_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("large_dataset_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    let table_id = db.create_table().unwrap();
    let index_id = db.create_index(table_id).unwrap();

    // Insert a large number of records to force B-Tree splits
    let record_count = 1000;
    let mut inserted_records = HashMap::new();

    println!("Inserting {} records...", record_count);
    
    for i in 1..=record_count {
        let record = Record::with_values(vec![
            SqliteValue::Integer(i * 10), // Use larger numbers for better distribution
            SqliteValue::String(format!("Record number {}", i)),
            SqliteValue::Blob(vec![i as u8; 100]), // Some bulk data
            SqliteValue::Float(i as f64 * 3.14159),
        ]);
        
        db.table_insert(table_id, i, &record).unwrap();
        inserted_records.insert(i, record);

        // Add to index
        let mut key_payload = Vec::new();
        serialize_values(&[SqliteValue::Integer(i * 10)], &mut key_payload).unwrap();
        db.index_insert(index_id, &key_payload, i).unwrap();

        // Periodically flush to test durability
        if i % 100 == 0 {
            db.flush().unwrap();
            println!("Flushed at record {}", i);
        }
    }

    println!("Verifying all records can be found...");
    
    // Verify all records can be found
    for i in 1..=record_count {
        let found = db.table_find(table_id, i).unwrap();
        assert!(found.is_some(), "Record {} not found", i);
        
        let record = found.unwrap();
        assert_eq!(record.len(), 4);
        
        // Verify first field (integer)
        match &record.values[0] {
            SqliteValue::Integer(val) => assert_eq!(*val, i * 10),
            _ => panic!("Expected integer for record {}", i),
        }
    }

    println!("Testing index searches...");
    
    // Test index searches
    let test_keys = [50, 250, 500, 750, 1000];
    for &key in &test_keys {
        let search_key = KeyValue::Integer(key * 10);
        let (found, _, _) = db.index_find(index_id, &search_key).unwrap();
        assert!(found, "Index key {} not found", key * 10);
    }

    println!("Testing deletions...");
    
    // Delete some records and verify B-Tree rebalancing
    let delete_keys = [100, 200, 300, 400, 500];
    for &key in &delete_keys {
        assert!(db.table_delete(table_id, key).unwrap());
        
        // Delete from index too
        let index_key = KeyValue::Integer(key * 10);
        assert!(db.index_delete(index_id, &index_key).unwrap());
    }

    // Verify deleted records are gone
    for &key in &delete_keys {
        assert!(db.table_find(table_id, key).unwrap().is_none());
        
        let index_key = KeyValue::Integer(key * 10);
        let (found, _, _) = db.index_find(index_id, &index_key).unwrap();
        assert!(!found);
    }

    // Verify remaining records still exist
    let verify_count = 100;
    for i in 1..=verify_count {
        if !delete_keys.contains(&i) {
            assert!(db.table_find(table_id, i).unwrap().is_some(), "Record {} should still exist", i);
        }
    }

    println!("Large dataset test completed successfully!");
}

/// Test error handling and edge cases
#[test]
fn test_error_handling_scenarios() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("error_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    // Test operations on non-existent tables/indexes
    let non_existent_table = 999;
    let non_existent_index = 888;

    // Table operations on non-existent table
    let dummy_record = Record::with_values(vec![SqliteValue::Integer(42)]);
    assert!(db.table_insert(non_existent_table, 1, &dummy_record).is_err());
    assert!(db.table_find(non_existent_table, 1).is_err());
    assert!(db.table_delete(non_existent_table, 1).is_err());

    // Index operations on non-existent index
    let dummy_key = KeyValue::Integer(42);
    let mut dummy_payload = Vec::new();
    serialize_values(&[SqliteValue::Integer(42)], &mut dummy_payload).unwrap();
    
    assert!(db.index_insert(non_existent_index, &dummy_payload, 1).is_err());
    assert!(db.index_find(non_existent_index, &dummy_key).is_err());
    assert!(db.index_delete(non_existent_index, &dummy_key).is_err());

    // Test duplicate insertions and deletions
    let table_id = db.create_table().unwrap();
    let record = Record::with_values(vec![SqliteValue::String("Test".to_string())]);
    
    // Insert record
    db.table_insert(table_id, 1, &record).unwrap();
    
    // Try to delete non-existent record
    assert!(!db.table_delete(table_id, 999).unwrap());
    
    // Delete existing record
    assert!(db.table_delete(table_id, 1).unwrap());
    
    // Try to delete again
    assert!(!db.table_delete(table_id, 1).unwrap());

    // Test with empty records
    let empty_record = Record::new();
    db.table_insert(table_id, 2, &empty_record).unwrap();
    
    let found = db.table_find(table_id, 2).unwrap().unwrap();
    assert_eq!(found.len(), 0);
}

/// Test different data types and serialization
#[test]
fn test_data_types_and_serialization() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("data_types_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    let table_id = db.create_table().unwrap();

    // Test all supported SQLite data types
    let test_records = vec![
        // Integer types
        Record::with_values(vec![SqliteValue::Integer(0)]),
        Record::with_values(vec![SqliteValue::Integer(1)]),
        Record::with_values(vec![SqliteValue::Integer(-1)]),
        Record::with_values(vec![SqliteValue::Integer(i64::MAX)]),
        Record::with_values(vec![SqliteValue::Integer(i64::MIN)]),
        
        // Float types
        Record::with_values(vec![SqliteValue::Float(0.0)]),
        Record::with_values(vec![SqliteValue::Float(3.14159)]),
        Record::with_values(vec![SqliteValue::Float(-2.71828)]),
        Record::with_values(vec![SqliteValue::Float(f64::MAX)]),
        Record::with_values(vec![SqliteValue::Float(f64::MIN)]),
        
        // String types
        Record::with_values(vec![SqliteValue::String("".to_string())]),
        Record::with_values(vec![SqliteValue::String("Hello, World!".to_string())]),
        Record::with_values(vec![SqliteValue::String("Unicode: 🦀🚀🌟".to_string())]),
        Record::with_values(vec![SqliteValue::String("Very long string that should test the serialization of longer text data to ensure it works correctly".to_string())]),
        
        // Blob types
        Record::with_values(vec![SqliteValue::Blob(vec![])]),
        Record::with_values(vec![SqliteValue::Blob(vec![0, 1, 2, 3, 4, 5])]),
        Record::with_values(vec![SqliteValue::Blob(vec![255; 1000])]),
        
        // Null type
        Record::with_values(vec![SqliteValue::Null]),
        
        // Mixed types
        Record::with_values(vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Mixed".to_string()),
            SqliteValue::Float(3.14),
            SqliteValue::Blob(vec![1, 2, 3]),
            SqliteValue::Null,
        ]),
    ];

    // Insert all test records
    for (i, record) in test_records.iter().enumerate() {
        let rowid = (i + 1) as i64;
        db.table_insert(table_id, rowid, record).unwrap();
    }

    // Verify all records can be found and match original data
    for (i, expected_record) in test_records.iter().enumerate() {
        let rowid = (i + 1) as i64;
        let found = db.table_find(table_id, rowid).unwrap().unwrap();
        
        assert_eq!(found.len(), expected_record.len());
        
        for (j, (found_val, expected_val)) in found.values.iter().zip(expected_record.values.iter()).enumerate() {
            match (found_val, expected_val) {
                (SqliteValue::Integer(a), SqliteValue::Integer(b)) => {
                    assert_eq!(a, b, "Integer mismatch at record {} field {}", i, j);
                }
                (SqliteValue::Float(a), SqliteValue::Float(b)) => {
                    assert!((a - b).abs() < f64::EPSILON, "Float mismatch at record {} field {}", i, j);
                }
                (SqliteValue::String(a), SqliteValue::String(b)) => {
                    assert_eq!(a, b, "String mismatch at record {} field {}", i, j);
                }
                (SqliteValue::Blob(a), SqliteValue::Blob(b)) => {
                    assert_eq!(a, b, "Blob mismatch at record {} field {}", i, j);
                }
                (SqliteValue::Null, SqliteValue::Null) => {
                    // Both null, match
                }
                _ => panic!("Type mismatch at record {} field {}: {:?} vs {:?}", i, j, found_val, expected_val),
            }
        }
    }
}

/// Test configuration options
#[test]
fn test_configuration_options() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("config_test.db");

    // Test with custom configuration
    let config = RQLiteConfig {
        page_size: 8192,
        buffer_pool_size: 500,
        reserved_space: 128,
        max_payload_fraction: 200,
        min_payload_fraction: 64,
    };

    let mut db = RQLite::create(db_path, Some(config.clone())).unwrap();

    // Verify configuration was applied
    assert_eq!(db.config().page_size, 8192);
    assert_eq!(db.config().buffer_pool_size, 500);
    assert_eq!(db.config().reserved_space, 128);

    // Test that the database still works with custom configuration
    let table_id = db.create_table().unwrap();
    
    let record = Record::with_values(vec![
        SqliteValue::Integer(42),
        SqliteValue::String("Config test".to_string()),
    ]);
    
    db.table_insert(table_id, 1, &record).unwrap();
    let found = db.table_find(table_id, 1).unwrap();
    assert!(found.is_some());
}

/// This test is currently failing (I need to check)
#[test]
fn test_memory_pressure() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("memory_pressure_test.db");
    
    // Create database with small buffer pool to force evictions
    let config = RQLiteConfig {
        page_size: 4096,
        buffer_pool_size: 10, // Very small buffer
        ..Default::default()
    };
    
    let mut db = RQLite::create(db_path, Some(config)).unwrap();
    let table_id = db.create_table().unwrap();

    // Insert many records with large payloads to force buffer evictions
    let record_count = 50;
    for i in 1..=record_count {
        let large_blob = vec![i as u8; 1000]; // 2KB per record
        let record = Record::with_values(vec![
            SqliteValue::Integer(i),
            SqliteValue::Blob(large_blob.clone()),
            SqliteValue::String(format!("Large record {}", i)),
        ]);
        
        db.table_insert(table_id, i, &record).unwrap();
        
        // Occasionally flush to test persistence under memory pressure
        if i % 10 == 0 {
            db.flush().unwrap();
        }
    }

    // Verify all records still exist despite memory pressure
    for i in 1..=record_count {
        let found = db.table_find(table_id, i).unwrap();
        assert!(found.is_some(), "Record {} lost under memory pressure", i);
        
        let record = found.unwrap();
        assert_eq!(record.len(), 3);
        
        // Verify blob data integrity
        match &record.values[1] {
            SqliteValue::Blob(blob) => {
                assert_eq!(blob.len(), 1000);
                assert_eq!(blob[0], i as u8);
            }
            _ => panic!("Expected blob"),
        }
    }
}

/// Test overflow page handling. This test simulates inserting records that exceed the page size,
/// There seems to be issues in the current btree splitting implementaton for overflow pages, because if a 
/// page exceeds the page size with a single record, it cannot be split and a new overflow page should be created.
#[test]
fn test_overflow_pages() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("overflow_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    let table_id = db.create_table().unwrap();

    // Create records with very large payloads that will require overflow pages
    let large_sizes = [5000, 5000, 5000]; // Sizes that exceed page size
    
    for (i, &size) in large_sizes.iter().enumerate() {
        let rowid = i as i64 + 1;
        let large_data = vec![(i as u8).wrapping_add(1); size];
        
        let record = Record::with_values(vec![
            SqliteValue::Integer(rowid),
            SqliteValue::Blob(large_data.clone()),
            SqliteValue::String(format!("Overflow record {} with {} bytes", i, size)),
        ]);
        
        db.table_insert(table_id, rowid, &record).unwrap();
        
        // Immediately verify the record can be read back
        let found = db.table_find(table_id, rowid).unwrap().unwrap();
        assert_eq!(found.len(), 3);
        
        match &found.values[1] {
            SqliteValue::Blob(blob) => {
                assert_eq!(blob.len(), size);
                assert_eq!(blob, &large_data);
            }
            _ => panic!("Expected blob"),
        }
    }

    // Test deletion of overflow records
    for i in 0..large_sizes.len() {
        let rowid = i as i64 + 1;
        assert!(db.table_delete(table_id, rowid).unwrap());
        assert!(db.table_find(table_id, rowid).unwrap().is_none());
    }
}

/// Test index operations with complex keys
#[test]
fn test_complex_index_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("complex_index_test.db");
    let mut db = RQLite::create(db_path, None).unwrap();

    let table_id = db.create_table().unwrap();
    let index_id = db.create_index(table_id).unwrap();

    // Test index with different key types
    let test_keys = vec![
        (1, KeyValue::Integer(100)),
        (2, KeyValue::Float(3.14159)),
        (3, KeyValue::String("apple".to_string())),
        (4, KeyValue::String("banana".to_string())),
        (5, KeyValue::Integer(-50)),
        (6, KeyValue::Float(-2.71828)),
        (7, KeyValue::String("zebra".to_string())),
        (8, KeyValue::Null),
        (9, KeyValue::Blob(vec![1, 2, 3, 4, 5])),
        (10, KeyValue::Blob(vec![255, 254, 253])),
    ];

    // Insert all keys into index
    for (rowid, key) in &test_keys {
        let mut key_payload = Vec::new();
        match key {
            KeyValue::Integer(i) => serialize_values(&[SqliteValue::Integer(*i)], &mut key_payload).unwrap(),
            KeyValue::Float(f) => serialize_values(&[SqliteValue::Float(*f)], &mut key_payload).unwrap(),
            KeyValue::String(s) => serialize_values(&[SqliteValue::String(s.clone())], &mut key_payload).unwrap(),
            KeyValue::Blob(b) => serialize_values(&[SqliteValue::Blob(b.clone())], &mut key_payload).unwrap(),
            KeyValue::Null => serialize_values(&[SqliteValue::Null], &mut key_payload).unwrap(),
        };
        
        db.index_insert(index_id, &key_payload, *rowid).unwrap();
    }

    // Test finding all keys
    for (_, key) in &test_keys {
        let (found, _, _) = db.index_find(index_id, key).unwrap();
        assert!(found, "Key {:?} not found in index", key);
    }

    // Test deleting some keys
    let keys_to_delete = [1, 3, 5, 8]; // Delete various key types
    for &rowid in &keys_to_delete {
        let key = &test_keys[rowid - 1].1;
        assert!(db.index_delete(index_id, key).unwrap());
    }

    // Verify deleted keys are gone
    for &rowid in &keys_to_delete {
        let key = &test_keys[rowid - 1].1;
        let (found, _, _) = db.index_find(index_id, key).unwrap();
        assert!(!found, "Key {:?} should have been deleted", key);
    }

    // Verify remaining keys still exist
    for (rowid, key) in &test_keys {
        if !keys_to_delete.contains(&(*rowid as usize)) {
            let (found, _, _) = db.index_find(index_id, key).unwrap();
            assert!(found, "Key {:?} should still exist", key);
        }
    }
}

/// Test database recovery scenarios
#[test]
fn test_recovery_scenarios() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("recovery_test.db");

    // Phase 1: Create database and start transaction
    {
        let mut db = RQLite::create(&db_path, None).unwrap();
        let table_id = db.create_table().unwrap();

        // Insert some committed data
        let committed_record = Record::with_values(vec![
            SqliteValue::String("Committed data".to_string()),
        ]);
        db.table_insert(table_id, 1, &committed_record).unwrap();
        db.flush().unwrap();

        // Start transaction and insert uncommitted data
        db.begin_transaction().unwrap();
        let uncommitted_record = Record::with_values(vec![
            SqliteValue::String("Uncommitted data".to_string()),
        ]);
        db.table_insert(table_id, 2, &uncommitted_record).unwrap();
        
        // Simulate crash - don't commit or rollback
        // Database goes out of scope without proper cleanup
    }

    // Phase 2: Reopen database and check state
    {
        let _db = RQLite::open(&db_path, None).unwrap();
        // In a complete implementation, we would verify that:
        // 1. Committed data is still there
        // 2. Uncommitted data is rolled back
        // 3. Database is in a consistent state
        
        // For now, just verify the database can be opened successfully
    }
}

/// Comprehensive integration test combining all features
#[test]
#[allow(unused_variables)]
fn test_comprehensive_integration() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("comprehensive_test.db");
    
    let config = RQLiteConfig {
        page_size: 4096,
        buffer_pool_size: 100,
        reserved_space: 32,
        max_payload_fraction: 255,
        min_payload_fraction: 32,
    };
    
    let mut db = RQLite::create(db_path, Some(config)).unwrap();

    // Create multiple tables and indexes
    let customers_table = db.create_table().unwrap();
    let orders_table = db.create_table().unwrap();
    let products_table = db.create_table().unwrap();
    
    let customers_email_index = db.create_index(customers_table).unwrap();
    let orders_customer_index = db.create_index(orders_table).unwrap();
    let products_name_index = db.create_index(products_table).unwrap();

    // Start a large transaction
    db.begin_transaction().unwrap();

    // Insert customers
    for i in 1..=100 {
        let record = Record::with_values(vec![
            SqliteValue::Integer(i),
            SqliteValue::String(format!("customer{}@example.com", i)),
            SqliteValue::String(format!("Customer {}", i)),
            SqliteValue::Integer(20 + (i % 50)), // Age between 20-69
        ]);
        db.table_insert(customers_table, i, &record).unwrap();

        // Index by email
        let mut email_payload = Vec::new();
        serialize_values(&[SqliteValue::String(format!("customer{}@example.com", i))], &mut email_payload).unwrap();
        db.index_insert(customers_email_index, &email_payload, i).unwrap();
    }

    // Insert products
    let product_names = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"];
    for (i, &name) in product_names.iter().enumerate() {
        let product_id = i as i64 + 1;
        let record = Record::with_values(vec![
            SqliteValue::Integer(product_id),
            SqliteValue::String(name.to_string()),
            SqliteValue::Float((i + 1) as f64 * 99.99),
        ]);
        db.table_insert(products_table, product_id, &record).unwrap();

        // Index by name
        let mut name_payload = Vec::new();
        serialize_values(&[SqliteValue::String(name.to_string())], &mut name_payload).unwrap();
        db.index_insert(products_name_index, &name_payload, product_id).unwrap();
    }

    // Insert orders (customers buying products)
    let mut order_id = 1i64;
    for customer_id in 1..=100 {
        for product_id in 1..=5 {
            if (customer_id + product_id) % 3 == 0 { // Only some combinations
                let record = Record::with_values(vec![
                    SqliteValue::Integer(order_id),
                    SqliteValue::Integer(customer_id),
                    SqliteValue::Integer(product_id),
                    SqliteValue::Integer(1 + (order_id % 5)), // Quantity 1-5
                ]);
                db.table_insert(orders_table, order_id, &record).unwrap();

                // Index by customer
                let mut customer_payload = Vec::new();
                serialize_values(&[SqliteValue::Integer(customer_id)], &mut customer_payload).unwrap();
                db.index_insert(orders_customer_index, &customer_payload, order_id).unwrap();

                order_id += 1;
            }
        }
    }

    // Commit the large transaction
    db.commit_transaction().unwrap();

    // Verify data integrity
    // Check random customers
    let test_customers = [1, 25, 50, 75, 100];
    for &customer_id in &test_customers {
        let customer = db.table_find(customers_table, customer_id).unwrap();
        assert!(customer.is_some(), "Customer {} not found", customer_id);
    }

    // Check all products
    for product_id in 1..=5 {
        let product = db.table_find(products_table, product_id).unwrap();
        assert!(product.is_some(), "Product {} not found", product_id);
    }

    // Test index searches
    let email_key = KeyValue::String("customer50@example.com".to_string());
    let (found, _, _) = db.index_find(customers_email_index, &email_key).unwrap();
    assert!(found, "Customer email not found in index");

    let product_key = KeyValue::String("Laptop".to_string());
    let (found, _, _) = db.index_find(products_name_index, &product_key).unwrap();
    assert!(found, "Product name not found in index");

    // Test customer orders lookup
    let customer_key = KeyValue::Integer(10);
    let (found, _, _) = db.index_find(orders_customer_index, &customer_key).unwrap();
    // May or may not be found depending on the modulo condition above

    // Test database statistics
    let page_count = db.page_count().unwrap();
    assert!(page_count > 1, "Should have multiple pages");

    let tables = db.list_tables();
    assert_eq!(tables.len(), 3);

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 3);

    // Final flush and close
    db.flush().unwrap();
    db.close().unwrap();

    println!("Comprehensive integration test completed successfully!");
    println!("Final database size: {} pages", page_count);
    println!("Total orders created: {}", order_id - 1);
}