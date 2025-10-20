/// Macro to implement bitwise operations for integer types
#[macro_export]
macro_rules! impl_bitwise_ops {
    ($($wrapper:ident),+ $(,)?) => {
        $(
            impl std::ops::BitAnd for $wrapper {
                type Output = Self;
                fn bitand(self, rhs: Self) -> Self::Output {
                    Self(self.0 & rhs.0)
                }
            }

            impl std::ops::BitOr for $wrapper {
                type Output = Self;
                fn bitor(self, rhs: Self) -> Self::Output {
                    Self(self.0 | rhs.0)
                }
            }

            impl std::ops::BitXor for $wrapper {
                type Output = Self;
                fn bitxor(self, rhs: Self) -> Self::Output {
                    Self(self.0 ^ rhs.0)
                }
            }

            impl std::ops::Not for $wrapper {
                type Output = Self;
                fn not(self) -> Self::Output {
                    Self(!self.0)
                }
            }

            impl std::ops::Shl<u32> for $wrapper {
                type Output = Self;
                fn shl(self, rhs: u32) -> Self::Output {
                    Self(self.0 << rhs)
                }
            }

            impl std::ops::Shr<u32> for $wrapper {
                type Output = Self;
                fn shr(self, rhs: u32) -> Self::Output {
                    Self(self.0 >> rhs)
                }
            }

            impl std::ops::BitAndAssign for $wrapper {
                fn bitand_assign(&mut self, rhs: Self) {
                    self.0 &= rhs.0;
                }
            }

            impl std::ops::BitOrAssign for $wrapper {
                fn bitor_assign(&mut self, rhs: Self) {
                    self.0 |= rhs.0;
                }
            }

            impl std::ops::BitXorAssign for $wrapper {
                fn bitxor_assign(&mut self, rhs: Self) {
                    self.0 ^= rhs.0;
                }
            }

            impl std::ops::ShlAssign<u32> for $wrapper {
                fn shl_assign(&mut self, rhs: u32) {
                    self.0 <<= rhs;
                }
            }

            impl std::ops::ShrAssign<u32> for $wrapper {
                fn shr_assign(&mut self, rhs: u32) {
                    self.0 >>= rhs;
                }
            }
        )+
    };
}
