#[macro_export]
macro_rules! def_data_type {
    ($name:ident, Owned) => {
        paste::paste! {
            #[derive(Debug, PartialEq)]
            pub(crate) enum $name {
                Null,
                SmallInt(Int8),
                HalfInt(Int16),
                Int(Int32),
                BigInt(Int64),
                SmallUInt(UInt8),
                HalfUInt(UInt16),
                UInt(UInt32),
                BigUInt(UInt64),
                Float(Float32),
                Double(Float64),
                Byte(UInt8),
                Blob(Blob),
                Text(Blob),
                Date(Date),
                Char(UInt8),
                Boolean(UInt8),
                DateTime(DateTime),
            }

            impl $name {
                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    matches!(
                        self,
                        Self::BigUInt(_)
                            | Self::BigInt(_)
                            | Self::SmallInt(_)
                            | Self::SmallUInt(_)
                            | Self::UInt(_)
                            | Self::Int(_)
                            | Self::HalfInt(_)
                            | Self::HalfUInt(_)
                            | Self::Float(_)
                            | Self::Double(_)
                            | Self::Char(_)
                            | Self::Boolean(_)
                            | Self::Byte(_)
                            | Self::DateTime(_)
                            | Self::Date(_)
                    )
                }

                #[inline]
                pub fn size(&self) -> usize {
                    use std::mem::size_of;
                    match self {
                        Self::Null => 0,
                        Self::SmallInt(_) => size_of::<i8>(),
                        Self::HalfInt(_) => size_of::<i16>(),
                        Self::Int(_) => size_of::<i32>(),
                        Self::BigInt(_) => size_of::<i64>(),
                        Self::SmallUInt(_) => size_of::<u8>(),
                        Self::HalfUInt(_) => size_of::<u16>(),
                        Self::UInt(_) => size_of::<u32>(),
                        Self::BigUInt(_) => size_of::<u64>(),
                        Self::Float(_) => size_of::<f32>(),
                        Self::Double(_) => size_of::<f64>(),
                        Self::Byte(_) | Self::Char(_) | Self::Boolean(_) => size_of::<u8>(),
                        Self::Date(_) => size_of::<u32>(),
                        Self::DateTime(_) => size_of::<u64>(),
                        Self::Blob(b) | Self::Text(b) => b.len(),
                    }
                }




                #[inline]
                pub fn is_null(&self) -> bool {
                    matches!(self, Self::Null)
                }
            }
        }
    };

    ($name:ident, $variant_modifier:ident) => {
        paste::paste! {
            #[derive(Debug, PartialEq)]
            pub(crate) enum $name<'a> {
                Null,
                SmallInt([<Int8 $variant_modifier>]<'a>),
                HalfInt([<Int16 $variant_modifier>]<'a>),
                Int([<Int32 $variant_modifier>]<'a>),
                BigInt([<Int64 $variant_modifier>]<'a>),
                SmallUInt([<UInt8 $variant_modifier>]<'a>),
                HalfUInt([<UInt16 $variant_modifier>]<'a>),
                UInt([<UInt32 $variant_modifier>]<'a>),
                BigUInt([<UInt64 $variant_modifier>]<'a>),
                Float([<Float32 $variant_modifier>]<'a>),
                Double([<Float64 $variant_modifier>]<'a>),
                Byte([<UInt8 $variant_modifier>]<'a>),
                Blob([<Blob $variant_modifier>]<'a>),
                Text([<Blob $variant_modifier>]<'a>),
                Date([<Date $variant_modifier>]<'a>),
                Char([<UInt8 $variant_modifier>]<'a>),
                Boolean([<UInt8 $variant_modifier>]<'a>),
                DateTime([<DateTime $variant_modifier>]<'a>),
            }

            impl<'a> $name<'a> {
                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    matches!(
                        self,
                        Self::BigUInt(_)
                            | Self::BigInt(_)
                            | Self::SmallInt(_)
                            | Self::SmallUInt(_)
                            | Self::UInt(_)
                            | Self::Int(_)
                            | Self::HalfInt(_)
                            | Self::HalfUInt(_)
                            | Self::Float(_)
                            | Self::Double(_)
                            | Self::Char(_)
                            | Self::Boolean(_)
                            | Self::Byte(_)
                            | Self::DateTime(_)
                            | Self::Date(_)
                    )
                }

                #[inline]
                pub fn size(&self) -> usize {
                    use std::mem::size_of;
                    match self {
                        Self::Null => 0,

                        // Signed integers
                        Self::SmallInt(_) => size_of::<i8>(),
                        Self::HalfInt(_) => size_of::<i16>(),
                        Self::Int(_) => size_of::<i32>(),
                        Self::BigInt(_) => size_of::<i64>(),

                        // Unsigned integers
                        Self::SmallUInt(_) => size_of::<u8>(),
                        Self::HalfUInt(_) => size_of::<u16>(),
                        Self::UInt(_) => size_of::<u32>(),
                        Self::BigUInt(_) => size_of::<u64>(),

                        // Floating point
                        Self::Float(_) => size_of::<f32>(),
                        Self::Double(_) => size_of::<f64>(),

                        // 1-byte scalars
                        Self::Byte(_) | Self::Char(_) | Self::Boolean(_) => size_of::<u8>(),

                        // Date/time
                        Self::Date(_) => size_of::<u32>(),
                        Self::DateTime(_) => size_of::<u64>(),

                        // Variable-length
                        Self::Blob(b) | Self::Text(b) => b.as_ref().len(),
                    }
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    matches!(self, Self::Null)
                }



                        #[inline]
                pub fn to_owned(&self) -> DataType {
                 match self {
                Self::Null => DataType::Null,
                Self::SmallInt(p) => DataType::SmallInt(p.to_owned()),
                Self::HalfInt(p) => DataType::HalfInt(p.to_owned()),
                Self::Int(p) => DataType::Int(p.to_owned()),
                Self::BigInt(p) => DataType::BigInt(p.to_owned()),
                Self::SmallUInt(p) => DataType::SmallUInt(p.to_owned()),
                Self::HalfUInt(p) => DataType::HalfUInt(p.to_owned()),
                Self::UInt(p) => DataType::UInt(p.to_owned()),
                Self::BigUInt(p) => DataType::BigUInt(p.to_owned()),
                Self::Float(p) => DataType::Float(p.to_owned()),
                Self::Double(p) => DataType::Double(p.to_owned()),
                Self::Byte(p) => DataType::Byte(p.to_owned()),
                Self::Char(p) => DataType::Char(p.to_owned()),
                Self::Boolean(p) => DataType::Boolean(p.to_owned()),
                Self::Date(p) => DataType::Date(p.to_owned()),
                Self::DateTime(p) => DataType::DateTime(p.to_owned()),
                Self::Blob(b) => DataType::Blob(b.to_blob()),
                Self::Text(b) => DataType::Text(b.to_blob()),
            }
        }



            }
        }
    };
}
