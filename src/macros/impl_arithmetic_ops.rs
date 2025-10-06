#[macro_export]
macro_rules! impl_arithmetic_ops {
    ($type:ident) => {
        impl std::ops::Add for $type {
            type Output = Self;

            fn add(self, other: Self) -> Self::Output {
                Self(self.0 + other.0)
            }
        }

        impl std::ops::Sub for $type {
            type Output = Self;

            fn sub(self, other: Self) -> Self::Output {
                Self(self.0 - other.0)
            }
        }

        impl std::ops::Mul for $type {
            type Output = Self;

            fn mul(self, other: Self) -> Self::Output {
                Self(self.0 * other.0)
            }
        }

        impl std::ops::Div for $type {
            type Output = Self;

            fn div(self, other: Self) -> Self::Output {
                Self(self.0 / other.0)
            }
        }

        impl std::ops::Add for &$type {
            type Output = $type;

            fn add(self, other: Self) -> Self::Output {
                $type(self.0 + other.0)
            }
        }

        impl std::ops::Sub for &$type {
            type Output = $type;

            fn sub(self, other: Self) -> Self::Output {
                $type(self.0 - other.0)
            }
        }

        impl std::ops::Mul for &$type {
            type Output = $type;

            fn mul(self, other: Self) -> Self::Output {
                $type(self.0 * other.0)
            }
        }

        impl std::ops::Div for &$type {
            type Output = $type;

            fn div(self, other: Self) -> Self::Output {
                $type(self.0 / other.0)
            }
        }

        impl std::ops::AddAssign for $type {
            fn add_assign(&mut self, other: Self) {
                self.0 += other.0;
            }
        }

        impl std::ops::SubAssign for $type {
            fn sub_assign(&mut self, other: Self) {
                self.0 -= other.0;
            }
        }

        impl std::ops::MulAssign for $type {
            fn mul_assign(&mut self, other: Self) {
                self.0 *= other.0;
            }
        }

        impl std::ops::DivAssign for $type {
            fn div_assign(&mut self, other: Self) {
                self.0 /= other.0;
            }
        }
    };
}
