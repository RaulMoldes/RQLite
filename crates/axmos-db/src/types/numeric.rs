use crate::{
    numeric, promote_symmetric,
    types::core::{FloatOps, SignedIntOps, TypeCast, UnsignedIntOps},
};

// Generates the numeric traits implementations
numeric! {
    wrapper   Int64,
    inner     i64,
    primitive i64,
    marker    SignedIntOps,
}

// Generates the numeric traits implementations
numeric! {
    wrapper   Int32,
    inner     i32,
    primitive i64,
    marker    SignedIntOps,
}

// Generates the numeric traits implementations
numeric! {
    wrapper   UInt64,
    inner     u64,
    primitive u64,
    marker    UnsignedIntOps,
}

// Generates the numeric traits implementations
numeric! {
    wrapper   UInt32,
    inner     u32,
    primitive u64,
    marker    UnsignedIntOps,
}

// Generates the numeric traits implementations
numeric! {
    wrapper   Float32,
    inner     f32,
    primitive f64,
    marker    FloatOps,
}

// Generates the numeric traits implementations
numeric! {
    wrapper   Float64,
    inner     f64,
    primitive f64,
    marker    FloatOps,
}

promote_symmetric!(Int64, Int32 => i64);

// Unsigned int combinations
promote_symmetric!(UInt64, UInt32 => u64);

// Signed + Unsigned promote to i64 (to handle potential negative results)
promote_symmetric!(Int32, UInt32 => i64);
promote_symmetric!(Int32, UInt64 => i64);
promote_symmetric!(Int64, UInt32 => i64);
promote_symmetric!(Int64, UInt64 => i64);

// Signed int + Float promote to f64
promote_symmetric!(Int32, Float32 => f64);
promote_symmetric!(Int32, Float64 => f64);
promote_symmetric!(Int64, Float32 => f64);
promote_symmetric!(Int64, Float64 => f64);

// Unsigned int + Float promote to f64
promote_symmetric!(UInt32, Float32 => f64);
promote_symmetric!(UInt32, Float64 => f64);
promote_symmetric!(UInt64, Float32 => f64);
promote_symmetric!(UInt64, Float64 => f64);

// Float combinations
promote_symmetric!(Float64, Float32 => f64);

#[inline]
fn f64_to_signed<T>(value: f64, min: f64, max: f64, convert: fn(i64) -> T) -> Option<T> {
    if value.is_nan() || value.is_infinite() {
        return None;
    }
    let t = value.trunc();
    if t < min || t > max {
        return None;
    }
    Some(convert(t as i64))
}

#[inline]
fn f64_to_unsigned<T>(value: f64, max: f64, convert: fn(u64) -> T) -> Option<T> {
    if value.is_nan() || value.is_infinite() || value < 0.0 {
        return None;
    }
    let t = value.trunc();
    if t > max {
        return None;
    }
    Some(convert(t as u64))
}

impl TypeCast<Int64> for Int32 {
    fn try_cast(&self) -> Option<Int64> {
        Some(Int64(self.0 as i64))
    }
}

impl TypeCast<UInt32> for Int32 {
    fn try_cast(&self) -> Option<UInt32> {
        (self.0 >= 0).then(|| UInt32(self.0 as u32))
    }
}

impl TypeCast<UInt64> for Int32 {
    fn try_cast(&self) -> Option<UInt64> {
        (self.0 >= 0).then(|| UInt64(self.0 as u64))
    }
}

impl TypeCast<Float32> for Int32 {
    fn try_cast(&self) -> Option<Float32> {
        Some(Float32(self.0 as f32))
    }
}

impl TypeCast<Float64> for Int32 {
    fn try_cast(&self) -> Option<Float64> {
        Some(Float64(self.0 as f64))
    }
}

impl TypeCast<Int32> for Int64 {
    fn try_cast(&self) -> Option<Int32> {
        i32::try_from(self.0).ok().map(Int32)
    }
}

impl TypeCast<UInt32> for Int64 {
    fn try_cast(&self) -> Option<UInt32> {
        u32::try_from(self.0).ok().map(UInt32)
    }
}

impl TypeCast<UInt64> for Int64 {
    fn try_cast(&self) -> Option<UInt64> {
        (self.0 >= 0).then(|| UInt64(self.0 as u64))
    }
}

impl TypeCast<Float32> for Int64 {
    fn try_cast(&self) -> Option<Float32> {
        Some(Float32(self.0 as f32))
    }
}

impl TypeCast<Float64> for Int64 {
    fn try_cast(&self) -> Option<Float64> {
        Some(Float64(self.0 as f64))
    }
}

impl TypeCast<Int32> for UInt32 {
    fn try_cast(&self) -> Option<Int32> {
        i32::try_from(self.0).ok().map(Int32)
    }
}

impl TypeCast<Int64> for UInt32 {
    fn try_cast(&self) -> Option<Int64> {
        Some(Int64(self.0 as i64))
    }
}

impl TypeCast<UInt64> for UInt32 {
    fn try_cast(&self) -> Option<UInt64> {
        Some(UInt64(self.0 as u64))
    }
}

impl TypeCast<Float32> for UInt32 {
    fn try_cast(&self) -> Option<Float32> {
        Some(Float32(self.0 as f32))
    }
}

impl TypeCast<Float64> for UInt32 {
    fn try_cast(&self) -> Option<Float64> {
        Some(Float64(self.0 as f64))
    }
}

impl TypeCast<Int32> for UInt64 {
    fn try_cast(&self) -> Option<Int32> {
        i32::try_from(self.0).ok().map(Int32)
    }
}

impl TypeCast<Int64> for UInt64 {
    fn try_cast(&self) -> Option<Int64> {
        i64::try_from(self.0).ok().map(Int64)
    }
}

impl TypeCast<UInt32> for UInt64 {
    fn try_cast(&self) -> Option<UInt32> {
        u32::try_from(self.0).ok().map(UInt32)
    }
}

impl TypeCast<Float32> for UInt64 {
    fn try_cast(&self) -> Option<Float32> {
        Some(Float32(self.0 as f32))
    }
}

impl TypeCast<Float64> for UInt64 {
    fn try_cast(&self) -> Option<Float64> {
        Some(Float64(self.0 as f64))
    }
}

impl TypeCast<Int32> for Float32 {
    fn try_cast(&self) -> Option<Int32> {
        f64_to_signed(self.0 as f64, i32::MIN as f64, i32::MAX as f64, |v| {
            Int32(v as i32)
        })
    }
}

impl TypeCast<Int64> for Float32 {
    fn try_cast(&self) -> Option<Int64> {
        f64_to_signed(self.0 as f64, i64::MIN as f64, i64::MAX as f64, Int64)
    }
}

impl TypeCast<UInt32> for Float32 {
    fn try_cast(&self) -> Option<UInt32> {
        f64_to_unsigned(self.0 as f64, u32::MAX as f64, |v| UInt32(v as u32))
    }
}

impl TypeCast<UInt64> for Float32 {
    fn try_cast(&self) -> Option<UInt64> {
        f64_to_unsigned(self.0 as f64, u64::MAX as f64, UInt64)
    }
}

impl TypeCast<Float64> for Float32 {
    fn try_cast(&self) -> Option<Float64> {
        Some(Float64(self.0 as f64))
    }
}

impl TypeCast<Int32> for Float64 {
    fn try_cast(&self) -> Option<Int32> {
        f64_to_signed(self.0, i32::MIN as f64, i32::MAX as f64, |v| {
            Int32(v as i32)
        })
    }
}

impl TypeCast<Int64> for Float64 {
    fn try_cast(&self) -> Option<Int64> {
        f64_to_signed::<Int64>(self.0, i64::MIN as f64, i64::MAX as f64, Int64)
    }
}

impl TypeCast<UInt32> for Float64 {
    fn try_cast(&self) -> Option<UInt32> {
        f64_to_unsigned(self.0, u32::MAX as f64, |v| UInt32(v as u32))
    }
}

impl TypeCast<UInt64> for Float64 {
    fn try_cast(&self) -> Option<UInt64> {
        f64_to_unsigned(self.0, u64::MAX as f64, UInt64)
    }
}

impl TypeCast<Float32> for Float64 {
    fn try_cast(&self) -> Option<Float32> {
        Some(Float32(self.0 as f32))
    }
}
