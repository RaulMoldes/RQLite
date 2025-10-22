#[macro_export]
macro_rules! sized {
    (
        $vis_type:vis type $name:ident = $inner:ty;
        $vis_const:vis const $size_const:ident
    ) => {
        $vis_type type $name = $inner;
        $vis_const const $size_const: usize = ::std::mem::size_of::<$inner>();
    };

    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($field:ident: $ty:ty),* $(,)?
        };
        $vis_const:vis const $size_const:ident
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $($field: $ty),*
        }

        $vis_const const $size_const: usize = ::std::mem::size_of::<$name>();
    };

    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident ( $($ty:ty),* $(,)? );
        $vis_const:vis const $size_const:ident
    ) => {
        $(#[$meta])*
        $vis struct $name($($ty),*);

        $vis_const const $size_const: usize = ::std::mem::size_of::<$name>();
    };
}
