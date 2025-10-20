/// Macro to implement arithmetic operations for numeric types
#[macro_export]
macro_rules! impl_arithmetic_ops {
    ($($wrapper:ident),+ $(,)?) => {
        $(
            $crate::impl_arithmetic_ops!(@single $wrapper);
        )+
    };

    (@single $wrapper:ident) => {
        // Binary operations
        impl std::ops::Add for $wrapper {
            type Output = Self;
            fn add(self, rhs: Self) -> Self::Output {
                Self(self.0 + rhs.0)
            }
        }

        impl std::ops::Sub for $wrapper {
            type Output = Self;
            fn sub(self, rhs: Self) -> Self::Output {
                Self(self.0 - rhs.0)
            }
        }

        impl std::ops::Mul for $wrapper {
            type Output = Self;
            fn mul(self, rhs: Self) -> Self::Output {
                Self(self.0 * rhs.0)
            }
        }

        impl std::ops::Div for $wrapper {
            type Output = Self;
            fn div(self, rhs: Self) -> Self::Output {
                Self(self.0 / rhs.0)
            }
        }

        impl std::ops::Rem for $wrapper {
            type Output = Self;
            fn rem(self, rhs: Self) -> Self::Output {
                Self(self.0 % rhs.0)
            }
        }

        // Assignment operations
        impl std::ops::AddAssign for $wrapper {
            fn add_assign(&mut self, rhs: Self) {
                self.0 += rhs.0;
            }
        }

        impl std::ops::SubAssign for $wrapper {
            fn sub_assign(&mut self, rhs: Self) {
                self.0 -= rhs.0;
            }
        }

        impl std::ops::MulAssign for $wrapper {
            fn mul_assign(&mut self, rhs: Self) {
                self.0 *= rhs.0;
            }
        }

        impl std::ops::DivAssign for $wrapper {
            fn div_assign(&mut self, rhs: Self) {
                self.0 /= rhs.0;
            }
        }

        impl std::ops::RemAssign for $wrapper {
            fn rem_assign(&mut self, rhs: Self) {
                self.0 %= rhs.0;
            }
        }


    };
}
