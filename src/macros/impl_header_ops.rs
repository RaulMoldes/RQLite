#[macro_export]
macro_rules! impl_header_ops {
    ($enum:ident { $($variant:ident),* $(,)? }) => {
        impl HeaderOps for $enum {
            fn cell_count(&self) -> usize {
                match self {
                    $(
                        $enum::$variant(page) => page.cell_count(),
                    )*
                }
            }

            fn max_cell_size(&self) -> usize {
                match self {
                    $(
                        $enum::$variant(page) => page.max_cell_size(),
                    )*
                }
            }

            fn content_start(&self) -> usize {
                match self {
                    $(
                        $enum::$variant(page) => page.content_start(),
                    )*
                }
            }

            fn free_space(&self) -> usize {
                match self {
                    $(
                        $enum::$variant(page) => page.free_space(),
                    )*
                }
            }

            fn id(&self) -> PageId {
                match self {
                    $(
                        $enum::$variant(page) => page.id(),
                    )*
                }
            }

            fn is_overflow(&self) -> bool {
                match self {
                    $(
                        $enum::$variant(page) => page.is_overflow(),
                    )*
                }
            }

            fn page_size(&self) -> usize {
                match self {
                    $(
                        $enum::$variant(page) => page.page_size(),
                    )*
                }
            }

            fn type_of(&self) -> PageType {
                match self {
                    $(
                        $enum::$variant(page) => page.type_of(),
                    )*
                }
            }

            fn get_next_overflow(&self) -> Option<PageId> {
                match self {
                    $(
                        $enum::$variant(page) => page.get_next_overflow(),
                    )*
                }
            }

            fn set_next_overflow(&mut self, overflowpage: PageId) {
                match self {
                    $(
                        $enum::$variant(page) => page.set_next_overflow(overflowpage),
                    )*
                }
            }

            fn set_type(&mut self, page_type: PageType) {
                match self {
                    $(
                        $enum::$variant(page) => page.set_type(page_type),
                    )*
                }
            }

            fn free_space_start(&self) -> usize {
                match self {
                    $(
                        $enum::$variant(page) => page.free_space_start(),
                    )*
                }
            }
        }
    };
}
