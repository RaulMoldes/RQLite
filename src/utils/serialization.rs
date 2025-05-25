//! # Serialization Module
//!
//! This module provides functionality to serialize and deserialize SQLite values.
//! It includes the definition of SQLite data types and their corresponding values.
//!
//! ## SQLite Data Types: https://www.sqlite.org/datatype3.html
//!
//! - `NULL`: Represents a null value.
//! - `INTEGER`: Represents signed integers of various sizes (8, 16, 24, 32, 48, and 64 bits).     
//! - `FLOAT`: Represents a 64-bit floating-point number (IEEE 754).
//! - `BLOB`: Represents binary data.
//! - `STRING`: Represents UTF-8 encoded strings.
//!
//!
//! //! Notes on SQLITE Data Types:
//! - SQLite uses dynamic typing, meaning that the type of a value is determined by its content rather than its declared type.
//! - The INTEGER types are used to store signed integers of various sizes.
//! - The BLOB type is used to store binary data, while the STRING type is used for UTF-8 encoded strings.
//! - The NULL type represents a null value.
//! - The INTEGER0 and INTEGER1 types are used to represent boolean values (false and true, respectively).
//!
//! Database engines that implement rigid typing (like PostgreSQL) may not support all SQLite types.
//! Affinity rules for columns in SQLite determine how values are stored and retrieved.
//! If the declared type contains the string "INT" then it is assigned INTEGER affinity.
//! If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
//! If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
//! If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
//! Otherwise, the affinity is NUMERIC.

use super::varint::{decode_varint, encode_varint};
use std::io::{self, Read, Write};

/// SQLite data types as defined in the SQLite documentation.
/// These types are used to represent the data types in SQLite files.
/// The values are represented as integers for serialization purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteType {
    /// 0: NULL
    Null = 0,
    /// 1: 8-bit signed INTEGER.
    Integer8 = 1,
    /// 2: 16-bit signed INTEGER.
    Integer16 = 2,
    /// 3: 24-bit signed INTEGER.
    ///   (Note: SQLite does not have a 24-bit integer type, but this is used for serialization.)
    Integer24 = 3,
    /// 4: 32-bit signed INTEGER.
    Integer32 = 4,
    /// 5: 48-bit signed INTEGER.
    Integer48 = 5,
    /// 6: 64-bit signed INTEGER.
    Integer64 = 6,
    /// 7: 64 bit floating point number (IEEE 754)
    Float64 = 7,
    /// 8: 0 (0x00) 1 bit INTEGER, represents value 0. It is used to represent a boolean false.
    Integer0 = 8,
    /// 9: 1 (0x01) 1 bit INTEGER, represents value 1. It is used to represent a boolean true.
    Integer1 = 9,
    /// Reserved for future use
    Reserved10 = 10,
    Reserved11 = 11,
    /// 12: BLOB with variable length specified by the following varint
    Blob = 12,
    /// 13: STRING with variable length specified by the following varint
    ///  (Note: This is a UTF-8 encoded string.)
    String = 13,
}

/// In SQLite, the difference between NUMERIC and INTEGER is only important on a cast expression, but it does not affect how the values are stores under the hood.
///
/// Note that on my implementation I am not using the SQLite rules for type affinity.
/// Instead, I am using the types as they are defined in the SQLite documentation.
/// This is because I am not implementing a full SQLite engine, but rather storage backend.
///
/// For DateTime values, SQLite does not have a specific type. Instead, it uses built in functions that can convert from REAL, INTEGER or TEXT to DATE.
/// As I said, I am not implementing that part of the functionality.
/// Conversion from u8 to SqliteType
impl From<u8> for SqliteType {
    fn from(value: u8) -> Self {
        match value {
            0 => SqliteType::Null,
            1 => SqliteType::Integer8,
            2 => SqliteType::Integer16,
            3 => SqliteType::Integer24,
            4 => SqliteType::Integer32,
            5 => SqliteType::Integer48,
            6 => SqliteType::Integer64,
            7 => SqliteType::Float64,
            8 => SqliteType::Integer0,
            9 => SqliteType::Integer1,
            10 => SqliteType::Reserved10,
            11 => SqliteType::Reserved11,
            12 => SqliteType::Blob,
            13 => SqliteType::String,
            _ => SqliteType::Null, // Valor predeterminado para códigos no reconocidos
        }
    }
}

/// Represents a generic SQLite value.
/// This enum can hold different types of values that SQLite supports.
#[derive(Debug, Clone)]
pub enum SqliteValue {
    /// NULL
    Null,
    /// Signed integer (compressed to the smallest possible size)
    /// See `utils/varint.rs` for more details.
    Integer(i64),
    /// Floating point number (IEEE 754)
    Float(f64),
    /// Binary data
    Blob(Vec<u8>),
    /// UtF-8 encoded string
    String(String),
}

impl SqliteValue {
    /// Serializes the SQLite value to the specified writer.
    ///
    /// # Parameters
    /// * `writer` - The destination where the value will be serialized.
    ///
    /// # Errors
    /// Returns an error if there are issues writing to the destination.
    ///     
    /// # Returns
    /// The number of bytes written to the destination.
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let mut bytes_written = 0;

        match self {
            SqliteValue::Null => {
                writer.write_all(&[SqliteType::Null as u8])?;
                bytes_written += 1;
            }
            SqliteValue::Integer(value) => {
                // Determinar el tipo de entero más pequeño que puede almacenar el valor
                match *value {
                    0 => {
                        writer.write_all(&[SqliteType::Integer0 as u8])?;
                        bytes_written += 1;
                    }
                    1 => {
                        writer.write_all(&[SqliteType::Integer1 as u8])?;
                        bytes_written += 1;
                    }
                    v if v >= -128 && v <= 127 => {
                        writer.write_all(&[SqliteType::Integer8 as u8])?;
                        writer.write_all(&[(v as i8) as u8])?;
                        bytes_written += 2;
                    }
                    v if v >= -32768 && v <= 32767 => {
                        writer.write_all(&[SqliteType::Integer16 as u8])?;
                        writer.write_all(&(v as i16).to_be_bytes())?;
                        bytes_written += 3;
                    }
                    v if v >= -8388608 && v <= 8388607 => {
                        writer.write_all(&[SqliteType::Integer24 as u8])?;
                        let bytes = (v as i32).to_be_bytes();
                        writer.write_all(&bytes[1..])?; // Ignorar el byte más significativo
                        bytes_written += 4;
                    }
                    v if v >= -2147483648 && v <= 2147483647 => {
                        writer.write_all(&[SqliteType::Integer32 as u8])?;
                        writer.write_all(&(v as i32).to_be_bytes())?;
                        bytes_written += 5;
                    }
                    v if v >= -140737488355328 && v <= 140737488355327 => {
                        writer.write_all(&[SqliteType::Integer48 as u8])?;
                        let bytes = v.to_be_bytes();
                        writer.write_all(&bytes[2..])?; // Ignorar los 2 bytes más significativos
                        bytes_written += 7;
                    }
                    _ => {
                        writer.write_all(&[SqliteType::Integer64 as u8])?;
                        writer.write_all(&value.to_be_bytes())?;
                        bytes_written += 9;
                    }
                }
            }
            SqliteValue::Float(value) => {
                writer.write_all(&[SqliteType::Float64 as u8])?;
                writer.write_all(&value.to_be_bytes())?;
                bytes_written += 9;
            }
            SqliteValue::Blob(data) => {
                writer.write_all(&[SqliteType::Blob as u8])?;
                bytes_written += 1;

                // Escribir la longitud como varint
                bytes_written += encode_varint(data.len() as i64, writer)?;

                // Escribir los datos
                writer.write_all(data)?;
                bytes_written += data.len();
            }
            SqliteValue::String(text) => {
                writer.write_all(&[SqliteType::String as u8])?;
                bytes_written += 1;

                // Convertir la cadena a bytes UTF-8
                let bytes = text.as_bytes();

                // Escribir la longitud como varint
                bytes_written += encode_varint(bytes.len() as i64, writer)?;

                // Escribir los datos
                writer.write_all(bytes)?;
                bytes_written += bytes.len();
            }
        }

        Ok(bytes_written)
    }

    /// Deserializes a SQLite value from the specified reader.
    ///
    /// # Parameters
    /// * `reader` - The source from which the value will be deserialized.
    ///
    /// # Errors
    /// Returns an error if there are issues reading from the source or if the format is invalid.
    ///
    /// # Returns
    /// A tuple containing the deserialized value and the number of bytes read.
    ///
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<(SqliteValue, usize)> {
        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;
        let mut bytes_read = 1;

        let sqlite_type = SqliteType::from(type_byte[0]);

        match sqlite_type {
            SqliteType::Null => Ok((SqliteValue::Null, bytes_read)),

            SqliteType::Integer0 => Ok((SqliteValue::Integer(0), bytes_read)),

            SqliteType::Integer1 => Ok((SqliteValue::Integer(1), bytes_read)),

            SqliteType::Integer8 => {
                let mut value_byte = [0u8; 1];
                reader.read_exact(&mut value_byte)?;
                bytes_read += 1;

                let value = value_byte[0] as i8 as i64;
                Ok((SqliteValue::Integer(value), bytes_read))
            }

            SqliteType::Integer16 => {
                let mut value_bytes = [0u8; 2];
                reader.read_exact(&mut value_bytes)?;
                bytes_read += 2;

                let value = i16::from_be_bytes(value_bytes) as i64;
                Ok((SqliteValue::Integer(value), bytes_read))
            }

            SqliteType::Integer24 => {
                let mut value_bytes = [0u8; 3];
                reader.read_exact(&mut value_bytes)?;
                bytes_read += 3;

                // Extender el signo
                let msb = value_bytes[0];
                let sign_bit = msb & 0x80;
                let mut full_bytes = [0u8; 4];

                if sign_bit != 0 {
                    full_bytes[0] = 0xFF; // Extender signo negativo
                }

                full_bytes[1..4].copy_from_slice(&value_bytes);
                let value = i32::from_be_bytes(full_bytes) as i64;

                Ok((SqliteValue::Integer(value), bytes_read))
            }

            SqliteType::Integer32 => {
                let mut value_bytes = [0u8; 4];
                reader.read_exact(&mut value_bytes)?;
                bytes_read += 4;

                let value = i32::from_be_bytes(value_bytes) as i64;
                Ok((SqliteValue::Integer(value), bytes_read))
            }

            SqliteType::Integer48 => {
                let mut value_bytes = [0u8; 6];
                reader.read_exact(&mut value_bytes)?;
                bytes_read += 6;

                // Extender el signo
                let msb = value_bytes[0];
                let sign_bit = msb & 0x80;
                let mut full_bytes = [0u8; 8];

                if sign_bit != 0 {
                    full_bytes[0] = 0xFF; // Extender signo negativo
                    full_bytes[1] = 0xFF;
                }

                full_bytes[2..8].copy_from_slice(&value_bytes);
                let value = i64::from_be_bytes(full_bytes);

                Ok((SqliteValue::Integer(value), bytes_read))
            }

            SqliteType::Integer64 => {
                let mut value_bytes = [0u8; 8];
                reader.read_exact(&mut value_bytes)?;
                bytes_read += 8;

                let value = i64::from_be_bytes(value_bytes);
                Ok((SqliteValue::Integer(value), bytes_read))
            }

            SqliteType::Float64 => {
                let mut value_bytes = [0u8; 8];
                reader.read_exact(&mut value_bytes)?;
                bytes_read += 8;

                let value = f64::from_be_bytes(value_bytes);
                Ok((SqliteValue::Float(value), bytes_read))
            }

            SqliteType::Blob => {
                // Leer la longitud como varint
                let (length, varint_bytes) = decode_varint(reader)?;
                bytes_read += varint_bytes;

                if length < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "BLOB length cannot be negative",
                    ));
                }

                // Leer los datos
                let mut data = vec![0u8; length as usize];
                reader.read_exact(&mut data)?;
                bytes_read += length as usize;

                Ok((SqliteValue::Blob(data), bytes_read))
            }

            SqliteType::String => {
                // Leer la longitud como varint
                let (length, varint_bytes) = decode_varint(reader)?;
                bytes_read += varint_bytes;

                if length < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "STRING length cannot be negative",
                    ));
                }

                // Leer los datos
                let mut data = vec![0u8; length as usize];
                reader.read_exact(&mut data)?;
                bytes_read += length as usize;

                // Convertir a String UTF-8
                match String::from_utf8(data) {
                    Ok(text) => Ok((SqliteValue::String(text), bytes_read)),
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid UTF-8 sequence in STRING",
                    )),
                }
            }

            // Tipos reservados o no reconocidos
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Data type not supported: {:?}", sqlite_type),
            )),
        }
    }

    /// Utility function to get the serialized size of the SQLite value.
    /// Integer 0 and 1 occupy 1 byte, while other integers occupy 1 + varint size + data size.
    /// An extra byte is added to store the data type.
    pub fn serialized_size(&self) -> usize {
        match self {
            SqliteValue::Null => 1,
            SqliteValue::Integer(0) | SqliteValue::Integer(1) => 1,
            SqliteValue::Integer(v) => {
                if *v >= -128 && *v <= 127 {
                    2 // Tipo + 1 byte
                } else if *v >= -32768 && *v <= 32767 {
                    3 // Tipo + 2 bytes
                } else if *v >= -8388608 && *v <= 8388607 {
                    4 // Tipo + 3 bytes
                } else if *v >= -2147483648 && *v <= 2147483647 {
                    5 // Tipo + 4 bytes
                } else if *v >= -140737488355328 && *v <= 140737488355327 {
                    7 // Tipo + 6 bytes
                } else {
                    9 // Tipo + 8 bytes
                }
            }
            SqliteValue::Float(_) => 9, // Tipo + 8 bytes
            SqliteValue::Blob(data) => {
                1 + super::varint::varint_size(data.len() as i64) + data.len()
            }
            SqliteValue::String(text) => {
                let bytes = text.as_bytes();
                1 + super::varint::varint_size(bytes.len() as i64) + bytes.len()
            }
        }
    }
}

/// Serializes an slice of `SqliteValue` to the specified writer.
///
/// # Parameters
/// * `values` - Slice of SQLite values to serialize.
/// * `writer` - The destination where the values will be serialized.
///     
/// # Errors
/// Returns an error if there are issues writing to the destination.
///
/// # Returns
/// The number of bytes written to the destination.
///
pub fn serialize_values<W: Write>(values: &[SqliteValue], writer: &mut W) -> io::Result<usize> {
    let mut bytes_written = 0;

    // Escribir el número de valores como varint
    bytes_written += encode_varint(values.len() as i64, writer)?;

    // Escribir cada valor
    for value in values {
        bytes_written += value.serialize(writer)?;
    }

    Ok(bytes_written)
}

/// Deserializes an slice of values from the specified reader.
///
/// # Parameters
/// * `reader` - The source from which the values will be deserialized.
///
/// # Errors
/// Returns an error if there are issues reading from the source or if the format is invalid.
///
/// # Returns
/// A tuple containing the deserialized values and the number of bytes read.
///
pub fn deserialize_values<R: Read>(reader: &mut R) -> io::Result<(Vec<SqliteValue>, usize)> {
    // Leer el número de valores
    let (count, mut bytes_read) = decode_varint(reader)?;

    if count < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Negative count for values",
        ));
    }

    let mut values = Vec::with_capacity(count as usize);

    // Leer cada valor
    for _ in 0..count {
        let (value, value_bytes) = SqliteValue::deserialize(reader)?;
        values.push(value);
        bytes_read += value_bytes;
    }

    Ok((values, bytes_read))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_sqlite_type_from_u8() {
        assert_eq!(SqliteType::from(0), SqliteType::Null);
        assert_eq!(SqliteType::from(1), SqliteType::Integer8);
        assert_eq!(SqliteType::from(7), SqliteType::Float64);
        assert_eq!(SqliteType::from(12), SqliteType::Blob);
        assert_eq!(SqliteType::from(13), SqliteType::String);
        assert_eq!(SqliteType::from(255), SqliteType::Null); // Unrecognized type defaults to Null
    }

    #[test]
    fn test_sqlite_value_null() {
        let value = SqliteValue::Null;
        let mut buffer = Vec::new();

        let bytes_written = value.serialize(&mut buffer).unwrap();
        assert_eq!(bytes_written, 1);
        assert_eq!(buffer, vec![0]);

        let mut cursor = Cursor::new(buffer);
        let (deserialized, bytes_read) = SqliteValue::deserialize(&mut cursor).unwrap();

        match deserialized {
            SqliteValue::Null => {}
            _ => panic!("Expected null, but obtained {:?}", deserialized),
        }

        assert_eq!(bytes_read, 1);
    }

    #[test]
    fn test_sqlite_value_integer() {
        // Attempt to test various integer values, including edge cases
        let test_values = [
            0,
            1,
            42,
            -42,
            127,
            -128,
            128,
            -129,
            32767,
            -32768,
            32768,
            -32769,
            8388607,
            -8388608,
            8388608,
            -8388609,
            2147483647,
            -2147483648,
            2147483648,
            -2147483649,
            i64::MAX,
            i64::MIN,
        ];

        for &int_value in &test_values {
            let value = SqliteValue::Integer(int_value);
            let mut buffer = Vec::new();

            let bytes_written = value.serialize(&mut buffer).unwrap();
            assert!(bytes_written > 0);

            let mut cursor = Cursor::new(buffer);
            let (deserialized, bytes_read) = SqliteValue::deserialize(&mut cursor).unwrap();

            match deserialized {
                SqliteValue::Integer(v) => assert_eq!(v, int_value),
                _ => panic!(
                    "Expected INTEGER({}), but obtained {:?}",
                    int_value, deserialized
                ),
            }

            assert_eq!(bytes_read, bytes_written);
        }
    }

    #[test]
    fn test_sqlite_value_float() {
        let test_values = [
            0.0,
            1.0,
            -1.0,
            std::f64::consts::PI,
            -std::f64::consts::PI,
            f64::MAX,
            f64::MIN,
            f64::NAN,
        ];

        for &float_value in &test_values {
            let value = SqliteValue::Float(float_value);
            let mut buffer = Vec::new();

            let bytes_written = value.serialize(&mut buffer).unwrap();
            assert_eq!(bytes_written, 9);
            assert_eq!(buffer[0], SqliteType::Float64 as u8);

            let mut cursor = Cursor::new(buffer);
            let (deserialized, bytes_read) = SqliteValue::deserialize(&mut cursor).unwrap();

            match deserialized {
                SqliteValue::Float(v) => {
                    if float_value.is_nan() {
                        assert!(v.is_nan());
                    } else {
                        assert_eq!(v, float_value);
                    }
                }
                _ => panic!(
                    "Expected FLOAT({}), but obtained {:?}",
                    float_value, deserialized
                ),
            }

            assert_eq!(bytes_read, 9);
        }
    }

    #[test]
    fn test_sqlite_value_blob() {
        let test_blobs = [vec![], vec![1, 2, 3], vec![255; 1000]];

        for blob in &test_blobs {
            let value = SqliteValue::Blob(blob.clone());
            let mut buffer = Vec::new();

            let bytes_written = value.serialize(&mut buffer).unwrap();
            assert!(bytes_written >= 1);
            assert_eq!(buffer[0], SqliteType::Blob as u8);

            let mut cursor = Cursor::new(buffer);
            let (deserialized, bytes_read) = SqliteValue::deserialize(&mut cursor).unwrap();

            match deserialized {
                SqliteValue::Blob(data) => assert_eq!(data, *blob),
                _ => panic!("Expected BLOB, but obtained {:?}", deserialized),
            }

            assert_eq!(bytes_read, bytes_written);
        }
    }

    #[test]
    fn test_sqlite_value_string() {
        let test_strings = [
            "",
            "Hello, world!",
            "áéíóúñ", // UTF-8 characters
            "😀🚀🌍", // Emojis (UTF-8 multibyte)
        ];

        for &string in &test_strings {
            let value = SqliteValue::String(string.to_string());
            let mut buffer = Vec::new();

            let bytes_written = value.serialize(&mut buffer).unwrap();
            assert!(bytes_written >= 1);
            assert_eq!(buffer[0], SqliteType::String as u8);

            let mut cursor = Cursor::new(buffer);
            let (deserialized, bytes_read) = SqliteValue::deserialize(&mut cursor).unwrap();

            match deserialized {
                SqliteValue::String(text) => assert_eq!(text, string),
                _ => panic!("Expected STRING, but obtained {:?}", deserialized),
            }

            assert_eq!(bytes_read, bytes_written);
        }
    }

    #[test]
    fn test_serialize_deserialize_values() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(42),
            SqliteValue::Float(std::f64::consts::PI),
            SqliteValue::Blob(vec![1, 2, 3]),
            SqliteValue::String("Hello, SQLite!".to_string()),
        ];

        let mut buffer = Vec::new();
        let bytes_written = serialize_values(&values, &mut buffer).unwrap();

        let mut cursor = Cursor::new(buffer);
        let (deserialized, bytes_read) = deserialize_values(&mut cursor).unwrap();

        assert_eq!(bytes_read, bytes_written);
        assert_eq!(deserialized.len(), values.len());

        // Comparar cada valor
        for (i, value) in values.iter().enumerate() {
            match (value, &deserialized[i]) {
                (SqliteValue::Null, SqliteValue::Null) => {}
                (SqliteValue::Integer(a), SqliteValue::Integer(b)) => assert_eq!(a, b),
                (SqliteValue::Float(a), SqliteValue::Float(b)) => assert_eq!(a, b),
                (SqliteValue::Blob(a), SqliteValue::Blob(b)) => assert_eq!(a, b),
                (SqliteValue::String(a), SqliteValue::String(b)) => assert_eq!(a, b),
                _ => panic!("Type does not match on index: {}", i),
            }
        }
    }
}
