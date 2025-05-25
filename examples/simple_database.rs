//! # Simple Database Example
//!
//! This example demonstrates how to use the RQLite storage engine to create
//! a simple database application. It shows basic operations like creating
//! tables, inserting records, querying data, and using indexes.
//!
//! Run this example with:
//! ```bash
//! cargo run --example simple_database
//! ```

use rqlite_engine::{RQLite, RQLiteConfig, Record, SqliteValue, KeyValue};
use rqlite_engine::utils::serialization::serialize_values;
use std::io;

/// Represents a user in our simple database
#[derive(Debug)]
struct User {
    id: i64,
    name: String,
    email: String,
    age: i64,
}

impl User {
    fn new(id: i64, name: String, email: String, age: i64) -> Self {
        User { id, name, email, age }
    }

    /// Convert User to a Record for storage
    fn to_record(&self) -> Record {
        Record::with_values(vec![
            SqliteValue::Integer(self.id),
            SqliteValue::String(self.name.clone()),
            SqliteValue::String(self.email.clone()),
            SqliteValue::Integer(self.age),
        ])
    }

    /// Create User from a Record
    fn from_record(record: &Record) -> Result<User, String> {
        if record.len() != 4 {
            return Err("Invalid record format for User".to_string());
        }

        let id = match &record.values[0] {
            SqliteValue::Integer(i) => *i,
            _ => return Err("Expected integer for user ID".to_string()),
        };

        let name = match &record.values[1] {
            SqliteValue::String(s) => s.clone(),
            _ => return Err("Expected string for user name".to_string()),
        };

        let email = match &record.values[2] {
            SqliteValue::String(s) => s.clone(),
            _ => return Err("Expected string for user email".to_string()),
        };

        let age = match &record.values[3] {
            SqliteValue::Integer(i) => *i,
            _ => return Err("Expected integer for user age".to_string()),
        };

        Ok(User::new(id, name, email, age))
    }
}

/// Simple database manager for users
struct UserDatabase {
    db: RQLite,
    users_table: u32,
    email_index: u32,
    name_index: u32,
}

impl UserDatabase {
    /// Create a new user database
    fn create(db_path: &str) -> io::Result<Self> {
        println!("Creating database at: {}", db_path);
        
        // Configure the database for our use case
        let config = RQLiteConfig {
            page_size: 4096,
            buffer_pool_size: 1000,
            reserved_space: 0,
            max_payload_fraction: 255,
            min_payload_fraction: 32,
        };

        let mut db = RQLite::create(db_path, Some(config))?;

        // Create the users table
        let users_table = db.create_table()?;
        println!("Created users table with ID: {}", users_table);

        // Create indexes for faster lookups
        let email_index = db.create_index(users_table)?;
        let name_index = db.create_index(users_table)?;
        println!("Created email index with ID: {}", email_index);
        println!("Created name index with ID: {}", name_index);

        Ok(UserDatabase {
            db,
            users_table,
            email_index,
            name_index,
        })
    }

    /// Open an existing user database
    fn open(db_path: &str) -> io::Result<Self> {
        println!("Opening existing database at: {}", db_path);
        
        let db = RQLite::open(db_path, None)?;
        
        // In a real implementation, you would load table and index IDs
        // from system tables. For this example, we use hardcoded values.
        let users_table = 1; // Assume first table created
        let email_index = 1; // Assume first index created
        let name_index = 2;  // Assume second index created

        Ok(UserDatabase {
            db,
            users_table,
            email_index,
            name_index,
        })
    }

    /// Insert a new user
    fn insert_user(&mut self, user: &User) -> io::Result<()> {
        println!("Inserting user: {} ({})", user.name, user.email);
        
        // Start a transaction for consistency
        self.db.begin_transaction()?;

        // Insert the user record
        let record = user.to_record();
        match self.db.table_insert(self.users_table, user.id, &record) {
            Ok(_) => {
                // Add to email index
                let mut email_payload = Vec::new();
                serialize_values(&[SqliteValue::String(user.email.clone())], &mut email_payload)?;
                
                if let Err(e) = self.db.index_insert(self.email_index, &email_payload, user.id) {
                    self.db.rollback_transaction()?;
                    return Err(e);
                }

                // Add to name index
                let mut name_payload = Vec::new();
                serialize_values(&[SqliteValue::String(user.name.clone())], &mut name_payload)?;
                
                if let Err(e) = self.db.index_insert(self.name_index, &name_payload, user.id) {
                    self.db.rollback_transaction()?;
                    return Err(e);
                }

                // Commit the transaction
                self.db.commit_transaction()?;
                println!("User inserted successfully");
                Ok(())
            }
            Err(e) => {
                self.db.rollback_transaction()?;
                Err(e)
            }
        }
    }

    /// Find a user by ID
    fn find_user_by_id(&self, user_id: i64) -> io::Result<Option<User>> {
        println!("Looking for user with ID: {}", user_id);
        
        match self.db.table_find(self.users_table, user_id)? {
            Some(record) => {
                match User::from_record(&record) {
                    Ok(user) => {
                        println!("Found user: {} ({})", user.name, user.email);
                        Ok(Some(user))
                    }
                    Err(e) => {
                        eprintln!("Error parsing user record: {}", e);
                        Ok(None)
                    }
                }
            }
            None => {
                println!("User with ID {} not found", user_id);
                Ok(None)
            }
        }
    }

    /// Find a user by email using the index
    fn find_user_by_email(&self, email: &str) -> io::Result<Option<User>> {
        println!("Looking for user with email: {}", email);
        
        let email_key = KeyValue::String(email.to_string());
        let (found, _, _) = self.db.index_find(self.email_index, &email_key)?;
        
        if found {
            // In a complete implementation, you would get the rowid from the index
            // and then look up the full record. For now, we'll search through all users.
            println!("Email found in index");
            
            // This is inefficient - in a real implementation, the index would return the rowid
            for user_id in 1..=1000 { // Check first 1000 user IDs
                if let Some(user) = self.find_user_by_id(user_id)? {
                    if user.email == email {
                        return Ok(Some(user));
                    }
                }
            }
        }
        
        println!("User with email {} not found", email);
        Ok(None)
    }

    /// Delete a user
    fn delete_user(&mut self, user_id: i64) -> io::Result<bool> {
        println!("Deleting user with ID: {}", user_id);
        
        // First, get the user to remove from indexes
        let user = match self.find_user_by_id(user_id)? {
            Some(user) => user,
            None => return Ok(false), // User doesn't exist
        };

        // Start transaction
        self.db.begin_transaction()?;

        // Delete from table
        match self.db.table_delete(self.users_table, user_id) {
            Ok(deleted) => {
                if deleted {
                    // Remove from email index
                    let email_key = KeyValue::String(user.email);
                    if let Err(e) = self.db.index_delete(self.email_index, &email_key) {
                        self.db.rollback_transaction()?;
                        return Err(e);
                    }

                    // Remove from name index
                    let name_key = KeyValue::String(user.name);
                    if let Err(e) = self.db.index_delete(self.name_index, &name_key) {
                        self.db.rollback_transaction()?;
                        return Err(e);
                    }

                    self.db.commit_transaction()?;
                    println!("User deleted successfully");
                    Ok(true)
                } else {
                    self.db.rollback_transaction()?;
                    Ok(false)
                }
            }
            Err(e) => {
                self.db.rollback_transaction()?;
                Err(e)
            }
        }
    }

    

    /// Get database statistics
    fn print_stats(&self) -> io::Result<()> {
        let page_count = self.db.page_count()?;
        let tables = self.db.list_tables();
        let indexes = self.db.list_indexes();
        
        println!("\n=== Database Statistics ===");
        println!("Total pages: {}", page_count);
        println!("Tables: {:?}", tables);
        println!("Indexes: {:?}", indexes);
        println!("Page size: {} bytes", self.db.config().page_size);
        println!("Buffer pool size: {} pages", self.db.config().buffer_pool_size);
        println!("===============================\n");
        
        Ok(())
    }

    /// Close the database
    fn close(self) -> io::Result<()>{
        println!("Closing database...");
        self.db.close()?;
        Ok(())



    }


  }

  fn main() -> io::Result<()> {
    // Path to the database file
    let db_path = "example.db";

    // Create a new database (or open existing one)
    let mut db = if std::path::Path::new(db_path).exists() {
        UserDatabase::open(db_path)?
    } else {
        UserDatabase::create(db_path)?
    };

    // Insert a few users
    let users = vec![
        User::new(1, "Alice".into(), "alice@example.com".into(), 30),
        User::new(2, "Bob".into(), "bob@example.com".into(), 25),
        User::new(3, "Charlie".into(), "charlie@example.com".into(), 40),
    ];

    for user in &users {
        db.insert_user(user)?;
    }

    // Look up a user by ID
    if let Some(user) = db.find_user_by_id(1)? {
        println!("User found by ID: {:?}", user);
    }

    // Look up a user by email
    if let Some(user) = db.find_user_by_email("bob@example.com")? {
        println!("User found by email: {:?}", user);
    }

    for user_id in 1..=3 {
        if let Some(user) = db.find_user_by_id(user_id)? {
            println!("User {}: {:?}", user_id, user);
        } else {
            println!("User with ID {} not found", user_id);
        }
    }
    
    // Delete a user
    if db.delete_user(2)? {
        println!("Deleted user with ID 2");
    }

    // Print stats
    db.print_stats()?;

    // Close the database
    db.close()?;

    Ok(())
}
