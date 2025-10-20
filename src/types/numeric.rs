use crate::serialization::Serializable;
use crate::types::{DataType, DataTypeMarker};
use crate::{impl_bitwise_ops, numeric_type};

numeric_type! {
    Int8(i8) => {
        type_marker: DataTypeMarker::SmallInt,
        size: 1,
        display_name: "Int8"
    },
    Int16(i16) => {
        type_marker: DataTypeMarker::HalfInt,
        size: 2,
        display_name: "Int16"
    },
    Int32(i32) => {
        type_marker: DataTypeMarker::Int,
        size: 4,
        display_name: "Int32"
    },
    Int64(i64) => {
        type_marker: DataTypeMarker::Bigint,
        size: 8,
        display_name: "Int64"
    },
    UInt8(u8) => {
        type_marker: DataTypeMarker::SmallUInt,
        size: 1,
        display_name: "UInt8"
    },
    UInt16(u16) => {
        type_marker: DataTypeMarker::HalfUInt,
        size: 2,
        display_name: "UInt16"
    },
    UInt32(u32) => {
        type_marker: DataTypeMarker::UInt,
        size: 4,
        display_name: "UInt32"
    },
    UInt64(u64) => {
        type_marker: DataTypeMarker::BigUInt,
        size: 8,
        display_name: "UInt64"
    },
    Float32(f32) => {
        type_marker: DataTypeMarker::Float,
        size: 4,
        display_name: "Float32"
    },
    Float64(f64) => {
        type_marker: DataTypeMarker::Double,
        size: 8,
        display_name: "Float64"
    }
}

impl_bitwise_ops!(Int8);
impl_bitwise_ops!(Int16);
impl_bitwise_ops!(Int32);
impl_bitwise_ops!(Int64);
impl_bitwise_ops!(UInt8);
impl_bitwise_ops!(UInt16);
impl_bitwise_ops!(UInt32);
impl_bitwise_ops!(UInt64);
