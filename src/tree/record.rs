//! # Record Module
//!
//! This module defines the `Record` struct, which represents a row in a SQLite database table.
//! It provides methods for creating, serializing, deserializing, and manipulating records.
//! You can call records "rows" in SQLite terminology.
//! Other database systems may call them "tuples", comming from the mathematical concept of Relational Algebra.
//! I just call them "records" to avoid confusion with the Rust `tuple` type.
//! 
use std::io::{self, Cursor, Read, Write};

use crate::utils::serialization::{deserialize_values, serialize_values, SqliteValue};

/// Represents a record in a SQLite database table.
///
/// A record is a collection of values, each corresponding to a column in the table.
// The `Record` struct is used internally by the SQLite engine to manage data storage and retrieval.
#[derive(Debug, Clone)]
pub struct Record {
    /// Values of the columns in the record.
    /// Each value corresponds to a column in the table.
    pub values: Vec<SqliteValue>,
}

impl Record {
    /// Creates a new empty record.
    pub fn new() -> Self {
        Record { values: Vec::new() }
    }

    /// Creates a new record with the specified values.
    ///
    /// # Parameters
    /// * `values` - Vector of values to initialize the record.
    ///
    /// # Returns
    /// A new `Record` instance with the specified values.
    pub fn with_values(values: Vec<SqliteValue>) -> Self {
        Record { values }
    }

    /// Serializes the record to a binary format.
    ///
    /// # Parameters
    /// * `writer` - Destination for the serialized data, implementing `Write`.
    ///
    /// # Errors
    /// Returns an error if there are issues writing the data.
    ///
    /// # Returns
    /// Number of bytes written.
    ///
    /// Note: see `serialize_values` for details on the serialization format (module `utils::serialization.rs`).
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        serialize_values(&self.values, writer)
    }

    /// Serializes the record to a byte vector.
    /// Just a different interface for convenience.
    ///
    /// # Returns
    /// A vector of bytes representing the serialized record.
    ///
    /// # Errors
    /// Returns an error if there are issues writing the data.
    ///
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Deserializes a record from a binary format.
    ///
    /// # Parameters
    /// * `reader` - Source of the serialized data, implementing `Read`.
    ///
    /// # Errors
    /// Returns an error if there are issues reading the data or if the format is invalid.
    ///
    /// # Returns
    /// A tuple with the deserialized record and the number of bytes read.
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<(Self, usize)> {
        let (values, bytes_read) = deserialize_values(reader)?;
        Ok((Record { values }, bytes_read))
    }

    /// Deserializes a record from a byte slice.
    /// I used the same approach as `serialize` to avoid unnecessary allocations.
    ///
    /// # Parameters
    /// * `data` - Slice of bytes representing the serialized record.
    ///
    /// # Errors
    /// Returns an error if there are issues reading the data or if the format is invalid.
    ///
    /// # Returns
    /// A tuple with the deserialized record and the number of bytes read.
    pub fn from_bytes(data: &[u8]) -> io::Result<(Self, usize)> {
        let mut cursor = Cursor::new(data);
        Self::deserialize(&mut cursor)
    }

    /// Adds a new value to the record.
    ///
    /// # Parameters
    /// * `value` - Value to add to the record.
    ///
    /// # Returns
    /// None
    pub fn add_value(&mut self, value: SqliteValue) {
        self.values.push(value);
    }

    /// Obtains the value at the specified index.
    ///
    /// # Parameters
    /// * `index` - Index of the value (starting from 0).
    ///
    /// # Returns
    /// An `Option` containing the value if it exists, or `None` if the index is out of range.
    ///
    /// # Note
    /// The index is zero-based, so the first value is at index 0.
    pub fn get_value(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }

    /// Sets the value at the specified index.
    ///
    /// # Parameters
    /// * `index` - Index of the value to set (starting from 0).
    ///
    /// # Returns
    /// `true` if the value was set successfully, `false` if the index is out of range.
    ///
    /// # Note
    /// The index is zero-based, so the first value is at index 0.
    pub fn set_value(&mut self, index: usize, value: SqliteValue) -> bool {
        if index >= self.values.len() {
            return false;
        }

        self.values[index] = value;
        true
    }

    /// Obtains the number of values in the record.
    ///
    /// # Returns
    /// The number of values in the record.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Verifies if the record is empty.
    ///
    /// # Returns
    /// `true` if the record is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Calculates the serialized size of the record.
    ///
    /// # Returns
    /// The size in bytes of the serialized record.
    pub fn serialized_size(&self) -> usize {
        // Size of the varint that stores the number of values
        // (see `serialize_values` for details)
        // Remember that in sqlite, integers are variable length, so we need to calculate the size of the varint
        // that stores the number of values.
        let count_size = crate::utils::varint_size(self.values.len() as i64);

        // Add the size of each value
        // (see `serialize_values` for details)
        let values_size = self
            .values
            .iter()
            .map(|v| v.serialized_size())
            .sum::<usize>();

        count_size + values_size
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::serialization::SqliteValue;
    use std::io::Cursor;

    #[test]
    fn test_record_new() {
        let record = Record::new();
        assert!(record.values.is_empty());
    }

    #[test]
    fn test_record_with_values() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::String("hello".to_string()),
            SqliteValue::Null,
        ];
        let record = Record::with_values(values.clone());

        assert_eq!(record.values.len(), 3);

        match &record.values[0] {
            SqliteValue::Integer(i) => assert_eq!(*i, 42),
            _ => panic!("Expected Integer"),
        }

        match &record.values[1] {
            SqliteValue::String(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected String"),
        }

        match &record.values[2] {
            SqliteValue::Null => {}
            _ => panic!("Expected Null"),
        }
    }

    #[test]
    fn test_record_add_value() {
        let mut record = Record::new();
        record.add_value(SqliteValue::Integer(123));
        record.add_value(SqliteValue::String("test".to_string()));

        assert_eq!(record.len(), 2);

        match &record.values[0] {
            SqliteValue::Integer(i) => assert_eq!(*i, 123),
            _ => panic!("Expected Integer"),
        }

        match &record.values[1] {
            SqliteValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_record_get_value() {
        let mut record = Record::new();
        record.add_value(SqliteValue::Integer(123));
        record.add_value(SqliteValue::String("test".to_string()));

        match record.get_value(0) {
            Some(SqliteValue::Integer(i)) => assert_eq!(*i, 123),
            _ => panic!("Expected Integer"),
        }

        match record.get_value(1) {
            Some(SqliteValue::String(s)) => assert_eq!(s, "test"),
            _ => panic!("Expected String"),
        }

        assert!(record.get_value(2).is_none());
    }

    #[test]
    fn test_record_set_value() {
        let mut record = Record::new();
        record.add_value(SqliteValue::Integer(123));

        let result = record.set_value(0, SqliteValue::Integer(456));
        assert!(result);

        match record.get_value(0) {
            Some(SqliteValue::Integer(i)) => assert_eq!(*i, 456),
            _ => panic!("Expected Integer"),
        }

        // Test index out of range
        let result = record.set_value(1, SqliteValue::Null);
        assert!(!result);
    }

    #[test]
    fn test_record_len_and_is_empty() {
        let record = Record::new();
        assert_eq!(record.len(), 0);
        assert!(record.is_empty());

        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::String("hello".to_string()),
        ];
        let record = Record::with_values(values);

        assert_eq!(record.len(), 2);
        assert!(!record.is_empty());
    }

    #[test]
    fn test_record_serialization_roundtrip() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::String("hello".to_string()),
            SqliteValue::Float(std::f64::consts::PI),
            SqliteValue::Blob(vec![1, 2, 3]),
            SqliteValue::Null,
        ];
        let record = Record::with_values(values);

        // Serialize to bytes
        let bytes = record.to_bytes().unwrap();

        // Deserialize from bytes
        let (deserialized_record, bytes_read) = Record::from_bytes(&bytes).unwrap();

        // Verify bytes read matches serialized size
        assert_eq!(bytes_read, bytes.len());

        // Verify record contents
        assert_eq!(deserialized_record.len(), record.len());

        for i in 0..record.len() {
            match (&record.values[i], &deserialized_record.values[i]) {
                (SqliteValue::Integer(a), SqliteValue::Integer(b)) => assert_eq!(a, b),
                (SqliteValue::String(a), SqliteValue::String(b)) => assert_eq!(a, b),
                (SqliteValue::Float(a), SqliteValue::Float(b)) => assert_eq!(a, b),
                (SqliteValue::Blob(a), SqliteValue::Blob(b)) => assert_eq!(a, b),
                (SqliteValue::Null, SqliteValue::Null) => {}
                _ => panic!("Values don't match"),
            }
        }
    }

    #[test]
    fn test_record_serialized_size() {
        let empty_record = Record::new();
        assert_eq!(empty_record.serialized_size(), crate::utils::varint_size(0));

        let mut record = Record::new();
        record.add_value(SqliteValue::Integer(42));

        let expected_size =
            crate::utils::varint_size(1) + SqliteValue::Integer(42).serialized_size();
        assert_eq!(record.serialized_size(), expected_size);

        record.add_value(SqliteValue::String("test".to_string()));
        let string_size = SqliteValue::String("test".to_string()).serialized_size();

        let expected_size =
            crate::utils::varint_size(2) + SqliteValue::Integer(42).serialized_size() + string_size;
        assert_eq!(record.serialized_size(), expected_size);
    }

    #[test]
    fn test_record_serialize_with_writer() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::String("hello".to_string()),
        ];
        let record = Record::with_values(values);

        let mut buffer = Vec::new();
        let bytes_written = record.serialize(&mut buffer).unwrap();

        // Check bytes written
        assert_eq!(bytes_written, buffer.len());

        // Deserialize back
        let mut cursor = Cursor::new(buffer);
        let (deserialized, _) = Record::deserialize(&mut cursor).unwrap();

        assert_eq!(deserialized.len(), record.len());
    }

    #[test]
    fn test_record_default() {
        let record = Record::default();
        assert!(record.is_empty());
    }
}
