pub mod buffer;
pub mod cell;
pub mod latches;
pub mod ops;
pub mod page;
pub(crate) use ops::PageOps;
//pub mod tuple;
pub mod wal;

#[cfg(miri)]
mod tests;

//use crate::types::DataTypeRef;
//use tuple::TupleRef;
/*
#[macro_export]
macro_rules! impl_display_tuple {
    ($tuple_type:ty, $name:expr) => {
        impl<'a, 'b> std::fmt::Display for $tuple_type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                writeln!(f, "{}: ", $name)?;
                writeln!(f, "Version: {}", self.version())?;
                write!(f, "Keys: [")?;

                for (i, col_def) in self.schema().iter_keys().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "{}: ", col_def.name)?;

                    match self.key(i) {
                        Ok(value) => match value {
                            DataTypeRef::Null => write!(f, "NULL")?,
                            DataTypeRef::SmallInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::HalfInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Int(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::BigInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::SmallUInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::HalfUInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::UInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::BigUInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Float(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Double(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Byte(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Char(v) => write!(f, "'{}'", v.to_owned().to_char())?,
                            DataTypeRef::Boolean(v) => write!(f, "{}", v.to_owned() != 0)?,
                            DataTypeRef::Date(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::DateTime(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Blob(b) => write!(f, "Blob({} bytes)", b.len())?,
                            DataTypeRef::Text(b) => write!(f, "Text: \"{}\"", b.as_str())?,
                        },
                        Err(e) => {
                            write!(f, "<error: {e}>")?;
                        }
                    }
                }

                writeln!(f, "]")?;
                write!(f, "Values: [")?;

                for (i, col_def) in self.schema().iter_values().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "{}: ", col_def.name)?;

                    match self.value(i) {
                        Ok(value) => match value {
                            DataTypeRef::Null => write!(f, "NULL")?,
                            DataTypeRef::SmallInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::HalfInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Int(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::BigInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::SmallUInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::HalfUInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::UInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::BigUInt(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Float(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Double(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Byte(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Char(v) => write!(f, "'{}'", v.to_owned().to_char())?,
                            DataTypeRef::Boolean(v) => write!(f, "{}", v.to_owned() != 0)?,
                            DataTypeRef::Date(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::DateTime(v) => write!(f, "{}", v.to_owned())?,
                            DataTypeRef::Blob(b) => write!(f, "Blob({} bytes)", b.len())?,
                            DataTypeRef::Text(b) => write!(f, "Text: \"{}\"", b.as_str())?,
                        },
                        Err(e) => {
                            write!(f, "<error: {e}>")?;
                        }
                    }
                }

                write!(f, "]")?;

                Ok(())
            }
        }
    };
}

impl_display_tuple!(TupleRef<'a, 'b>, "TupleRef");
*/
