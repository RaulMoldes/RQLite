/// Parameterized tests with single value
#[macro_export]
macro_rules! param_tests {
    ($fn:ident, $param:ident => [$($val:expr),+ $(,)?]) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                #[cfg_attr(miri, ignore)]
                fn [<$fn _ $param _ $val>]()  {
                    $fn($val)
                }
            )+
        }
    };
    // MIRI-safe variant
    ($fn:ident, $param:ident => [$($val:expr),+], miri_safe) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                fn [<$fn _ $param _ $val>]()  {
                    $fn($val)
                }
            )+
        }
    };
}

/// Parameterized tests for types
#[macro_export]
macro_rules! param_type_tests {
    ($fn:ident, $param:ident => [$($ty:ty),+ $(,)?]) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                #[cfg_attr(miri, ignore)]
                fn [<$fn _ $param _ $ty:snake>]() {
                    $fn::<$ty>()
                }
            )+
        }
    };

    // MIRI-safe variant
    ($fn:ident, $param:ident => [$($ty:ty),+$(,)?], miri_safe) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                fn [<$fn _ $param _ $ty:snake>]() {
                    $fn::<$ty>()
                }
            )+
        }
    };
}

#[macro_export]
macro_rules! param2_tests {
    ($fn:ident, $p1:ident, $p2:ident => [$(($v1:expr, $v2:expr)),+ $(,)?]) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                #[cfg_attr(miri, ignore)]
                fn [<$fn _ $p1 _ $v1 _ $p2 _ $v2>]() {
                    $fn($v1, $v2)
                }
            )+
        }
    };

    ($fn:ident, $p1:ident, $p2:ident => [$(($v1:expr, $v2:expr)),+ $(,)?], miri_safe $(,)?) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                fn [<$fn _ $p1 _ $v1 _ $p2 _ $v2>]() {
                    $fn($v1, $v2)
                }
            )+
        }
    };
}

/// Parameterized tests for two type parameters
#[macro_export]
macro_rules! param2_type_tests {

    // Normal variant
    (
        $fn:ident,
        $p1:ident,
        $p2:ident
        => [$(($t1:ty, $t2:ty)),+ $(,)?]
    ) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                #[cfg_attr(miri, ignore)]
                fn [<$fn _ $p1 _ $t1:snake _ $p2 _ $t2:snake>]()  {
                    $fn::<$t1, $t2>()
                }
            )+
        }
    };

    // MIRI-safe variant
    (
        $fn:ident,
        $p1:ident,
        $p2:ident
        => [$(($t1:ty, $t2:ty)),+ $(,)?],
        miri_safe $(,)?
    ) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                fn [<$fn _ $p1 _ $t1:snake _ $p2 _ $t2:snake>]() {
                    $fn::<$t1, $t2>()
                }
            )+
        }
    };
}

#[macro_export]
macro_rules! param3_tests {
    ($fn:ident, $p1:ident, $p2:ident, $p3:ident
        => [$(($v1:expr, $v2:expr, $v3:expr)),+ $(,)?]
    ) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                #[cfg_attr(miri, ignore)]
                fn [<$fn _ $p1 _ $v1 _ $p2 _ $v2 _ $p3 _ $v3>]()  {
                    $fn($v1, $v2, $v3)
                }
            )+
        }
    };

    ($fn:ident, $p1:ident, $p2:ident, $p3:ident
        => [$(($v1:expr, $v2:expr, $v3:expr)),+ $(,)?],
        miri_safe $(,)?
    ) => {
        paste::paste! {
            $(
                #[test]
                #[serial_test::serial]
                fn [<$fn _ $p1 _ $v1 _ $p2 _ $v2 _ $p3 _ $v3>]() {
                    $fn($v1, $v2, $v3)
                }
            )+
        }
    };
}

/// Matrix tests (generates a test for all combinations of two parameter sets)
///
/// ```ignore
/// fn test_matrix(a: usize, b: usize) -> io::Result<()> { Ok(()) }
/// matrix_tests!(test_matrix, a => [1, 2], b => [10, 20]);
/// // Generates: test_matrix_a_1_b_10, test_matrix_a_1_b_20, test_matrix_a_2_b_10, test_matrix_a_2_b_20
/// ```
#[macro_export]
macro_rules! matrix_tests {
    // Normal variant
    ($fn:ident,
     $p1:ident => [$($v1:expr),+ $(,)?],
     $p2:ident => [$($v2:expr),+ $(,)?]
    ) => {
        $crate::matrix_tests!(
            @impl_normal
            $fn, $p1, $p2,
            [$($v1),+],
            [$($v2),+],
            []
        );
    };


    // MIRI-safe variant
    ($fn:ident,
     $p1:ident => [$($v1:expr),+ $(,)?],
     $p2:ident => [$($v2:expr),+ $(,)?],
     miri_safe $(,)?
    ) => {
        $crate::matrix_tests!(
            @impl_miri
            $fn, $p1, $p2,
            [$($v1),+],
            [$($v2),+],
            []
        );
    };

    // Base case (normal)
    (@impl_normal
     $fn:ident, $p1:ident, $p2:ident,
     [],
     [$($v2:expr),+],
     [$($acc:tt)*]
    ) => {
        paste::paste! { $($acc)* }
    };


    // Base case (miri)
    (@impl_miri
     $fn:ident, $p1:ident, $p2:ident,
     [],
     [$($v2:expr),+],
     [$($acc:tt)*]
    ) => {
        paste::paste! { $($acc)* }
    };

    // Recursive (normal)
    (@impl_normal
     $fn:ident, $p1:ident, $p2:ident,
     [$v1_head:expr $(, $v1_tail:expr)*],
     [$($v2:expr),+],
     [$($acc:tt)*]
    ) => {
        $crate::matrix_tests!(
            @impl_normal
            $fn, $p1, $p2,
            [$($v1_tail),*],
            [$($v2),+],
            [
                $($acc)*
                $(
                    #[test]
                    #[serial_test::serial]
                    #[cfg_attr(miri, ignore)]
                    fn [<$fn _ $p1 _ $v1_head _ $p2 _ $v2>]()  {
                        $fn($v1_head, $v2)
                    }
                )+
            ]
        );
    };

    // Recursive (miri)
    (@impl_miri
     $fn:ident, $p1:ident, $p2:ident,
     [$v1_head:expr $(, $v1_tail:expr)*],
     [$($v2:expr),+],
     [$($acc:tt)*]
    ) => {
        $crate::matrix_tests!(
            @impl_miri
            $fn, $p1, $p2,
            [$($v1_tail),*],
            [$($v2),+],
            [
                $($acc)*
                $(
                    #[test]
                    #[serial_test::serial]
                    fn [<$fn _ $p1 _ $v1_head _ $p2 _ $v2>]() {
                        $fn($v1_head, $v2)
                    }
                )+
            ]
        );
    };
}

#[macro_export]
macro_rules! matrix_type_tests {

    (
        $fn:ident,
        $p1:ident => [$($t1:ty),+ $(,)?],
        $p2:ident => [$($t2:ty),+ $(,)?]
    ) => {
        $crate::matrix_type_tests!(
            @impl_normal
            $fn, $p1, $p2,
            [$($t1),+],
            [$($t2),+],
            []
        );
    };


    (
        $fn:ident,
        $p1:ident => [$($t1:ty),+ $(,)?],
        $p2:ident => [$($t2:ty),+ $(,)?],
        miri_safe $(,)?
    ) => {
        $crate::matrix_type_tests!(
            @impl_miri
            $fn, $p1, $p2,
            [$($t1),+],
            [$($t2),+],
            []
        );
    };

    (
        @impl_normal
        $fn:ident, $p1:ident, $p2:ident,
        [],
        [$($t2:ty),+],
        [$($acc:tt)*]
    ) => {
        paste::paste! { $($acc)* }
    };

    (
        @impl_miri
        $fn:ident, $p1:ident, $p2:ident,
        [],
        [$($t2:ty),+],
        [$($acc:tt)*]
    ) => {
        paste::paste! { $($acc)* }
    };

    (
        @impl_normal
        $fn:ident, $p1:ident, $p2:ident,
        [$t1_head:ty $(, $t1_tail:ty)*],
        [$($t2:ty),+],
        [$($acc:tt)*]
    ) => {
        $crate::matrix_type_tests!(
            @impl_normal
            $fn, $p1, $p2,
            [$($t1_tail),*],
            [$($t2),+],
            [
                $($acc)*
                $(
                    #[test]
                    #[cfg_attr(miri, ignore)]
                    fn [<
                        $fn _ $p1 _ $t1_head:snake
                        _ $p2 _ $t2:snake
                    >]()  {
                        $fn::<$t1_head, $t2>()
                    }
                )+
            ]
        );
    };


    (
        @impl_miri
        $fn:ident, $p1:ident, $p2:ident,
        [$t1_head:ty $(, $t1_tail:ty)*],
        [$($t2:ty),+],
        [$($acc:tt)*]
    ) => {
        $crate::matrix_type_tests!(
            @impl_miri
            $fn, $p1, $p2,
            [$($t1_tail),*],
            [$($t2),+],
            [
                $($acc)*
                $(
                    #[test]
                    fn [<
                        $fn _ $p1 _ $t1_head:snake
                        _ $p2 _ $t2:snake
                    >]() {
                        $fn::<$t1_head, $t2>()
                    }
                )+
            ]
        );
    };
}
