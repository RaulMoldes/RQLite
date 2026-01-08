#[macro_export]
macro_rules! numeric {
    (
        wrapper   $name:ident,
        inner     $inner:ty,
        primitive $primitive:ty,
        marker    $marker:ident $(,)?
    ) => {
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
        pub struct $name(pub $inner);

        impl $crate::types::core::TypeClass for $name {
            const SIZE: Option<usize> = Some(std::mem::size_of::<$inner>());
            const ALIGN: usize = std::mem::align_of::<$inner>();
        }



        /// Will trigger the autogeneration of the ByteMuck ref type.
        unsafe impl bytemuck::Pod for $name {}
        unsafe impl bytemuck::Zeroable for $name {}

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl $name {
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            pub const fn value(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }


        impl $crate::types::core::BytemuckType for $name {}

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                bytemuck::bytes_of(self)
            }
        }


        /// Numeric types always have a fixed size.
        impl $crate::types::core::FixedSizeType for $name {}
        impl $crate::types::core::NumericType for $name {}

        /// Implement [to_primitive] type casting
        impl $crate::types::core::NumericOps for $name {
            type Primitive = $primitive;

            fn to_primitive(self) -> $primitive {
                self.0 as $primitive
            }

            fn from_primitive(value: $primitive) -> Self {
                Self(value as $inner)
            }


            fn abs(&self) -> Self {
                Self($crate::types::core::NumericAbs::numeric_abs(self.0))
            }

            fn sqrt(&self) -> Self {
                Self($crate::types::core::NumericRoundOps::numeric_sqrt(self.0))
            }

            fn floor(&self) -> Self {
                Self($crate::types::core::NumericRoundOps::numeric_floor(self.0))
            }

            fn round(&self) -> Self {
                Self($crate::types::core::NumericRoundOps::numeric_round(self.0))
            }

            fn ceil(&self) -> Self {
                Self($crate::types::core::NumericRoundOps::numeric_ceil(self.0))
            }
        }

        impl $marker for $name {}

        // All numeric types are self-promotable to their target primitive type.
        $crate::promote_pair!($name, $name => $primitive);
    };
}

/// Creates a type promotion table for a pair of numeric types.
#[macro_export]
macro_rules! promote_pair {
    ($lhs:ty, $rhs:ty => $output:ty) => {
        impl $crate::types::core::Promote<$rhs> for $lhs {
            type Output = $output;

            fn promote_lhs(self) -> $output {
                self.0 as $output
            }

            fn promote_rhs(rhs: $rhs) -> $output {
                rhs.0 as $output
            }
        }
    };
}

/// Creates a symmetric promotion relationship between two types.
#[macro_export]
macro_rules! promote_symmetric {
    ($a:ty, $b:ty => $output:ty) => {
        $crate::promote_pair!($a, $b => $output);
        $crate::promote_pair!($b, $a => $output);
    };
}

#[macro_export]
macro_rules! arithmetic_op {
    ($method:ident, $trait_method:ident) => {
        pub fn $method(&self, other: &DataType) -> TypeSystemResult<DataType> {
            use crate::types::core::*;

            match (self, other) {
                // Double combinations
                (DataType::Double(a), DataType::Double(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Double(a), DataType::Float(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Float(a), DataType::Double(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Double(a), DataType::BigInt(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::BigInt(a), DataType::Double(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Double(a), DataType::Int(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Int(a), DataType::Double(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Double(a), DataType::BigUInt(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::BigUInt(a), DataType::Double(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Double(a), DataType::UInt(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::UInt(a), DataType::Double(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }

                // Float combinations
                (DataType::Float(a), DataType::Float(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Float(a), DataType::BigInt(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::BigInt(a), DataType::Float(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Float(a), DataType::Int(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Int(a), DataType::Float(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Float(a), DataType::BigUInt(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::BigUInt(a), DataType::Float(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::Float(a), DataType::UInt(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }
                (DataType::UInt(a), DataType::Float(b)) => {
                    Ok(DataType::Double(Float64(a.$trait_method(*b))))
                }

                // Signed int combinations
                (DataType::BigInt(a), DataType::BigInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::BigInt(a), DataType::Int(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::Int(a), DataType::BigInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::Int(a), DataType::Int(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }

                // Signed + Unsigned
                (DataType::BigInt(a), DataType::UInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::UInt(a), DataType::BigInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::BigInt(a), DataType::BigUInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::BigUInt(a), DataType::BigInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::Int(a), DataType::UInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::UInt(a), DataType::Int(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::Int(a), DataType::BigUInt(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }
                (DataType::BigUInt(a), DataType::Int(b)) => {
                    Ok(DataType::BigInt(Int64(a.$trait_method(*b))))
                }

                // Unsigned combinations
                (DataType::BigUInt(a), DataType::BigUInt(b)) => {
                    Ok(DataType::BigUInt(UInt64(a.$trait_method(*b))))
                }
                (DataType::BigUInt(a), DataType::UInt(b)) => {
                    Ok(DataType::BigUInt(UInt64(a.$trait_method(*b))))
                }
                (DataType::UInt(a), DataType::BigUInt(b)) => {
                    Ok(DataType::BigUInt(UInt64(a.$trait_method(*b))))
                }
                (DataType::UInt(a), DataType::UInt(b)) => {
                    Ok(DataType::BigUInt(UInt64(a.$trait_method(*b))))
                }

                // Non-numeric
                (a, b) => Err(TypeSystemError::UnexpectedDataType(if !a.is_numeric() {
                    a.kind()
                } else {
                    b.kind()
                })),
            }
        }
    };
}
