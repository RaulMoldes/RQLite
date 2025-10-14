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

#[macro_export]
macro_rules! impl_header_ops_guard {
    ($guard_type:ident<$page:ident>) => {
        impl<$page> HeaderOps for $guard_type<$page>
        where
            $page: HeaderOps,
        {
            fn cell_count(&self) -> usize {
                self.0.cell_count()
            }

            fn content_start(&self) -> usize {
                self.0.content_start()
            }

            fn free_space(&self) -> usize {
                self.0.free_space()
            }

            fn id(&self) -> PageId {
                self.0.id()
            }

            fn is_overflow(&self) -> bool {
                self.0.is_overflow()
            }

            fn page_size(&self) -> usize {
                self.0.page_size()
            }

            fn type_of(&self) -> PageType {
                self.0.type_of()
            }

            fn get_next_overflow(&self) -> Option<PageId> {
                self.0.get_next_overflow()
            }

            fn set_next_overflow(&mut self, overflowpage: PageId) {
                panic!("Cannot modify through ReadGuard");
            }

            fn set_type(&mut self, _page_type: PageType) {
                panic!("Cannot modify through ReadGuard");
            }

            fn free_space_start(&self) -> usize {
                self.0.free_space_start()
            }
        }
    };
}
