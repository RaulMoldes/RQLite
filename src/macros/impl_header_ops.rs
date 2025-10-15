#[macro_export]
macro_rules! impl_header_ops_enum {
    ($enum:ident { $($variant:ident),* $(,)? }) => {
        impl HeaderOps for $enum {
            fn cell_count(&self) -> u16 {
                match self {
                    $(
                        $enum::$variant(page) => page.cell_count(),
                    )*
                }
            }


            fn content_start(&self) -> u16 {
                match self {
                    $(
                        $enum::$variant(page) => page.content_start(),
                    )*
                }
            }

            fn free_space(&self) -> u16 {
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

            fn page_size(&self) -> u16 {
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

            fn free_space_start(&self) -> u16 {
                match self {
                    $(
                        $enum::$variant(page) => page.free_space_start(),
                    )*
                }
            }


             fn set_free_space_ptr(&mut self, ptr: u16) {
                match self {
                    $(
                        $enum::$variant(page) => page.set_free_space_ptr(ptr),
                    )*
                }
            }


             fn set_content_start_ptr(&mut self, ptr: u16) {
                match self {
                    $(
                        $enum::$variant(page) => page.set_content_start_ptr(ptr),
                    )*
                }
            }

             fn set_cell_count(&mut self, count: u16) {
                match self {
                    $(
                        $enum::$variant(page) => page.set_cell_count(count),
                    )*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_header_ops {
    ($page_type:ty) => {
        impl HeaderOps for $page_type {
            fn type_of(&self) -> PageType {
                self.header.type_of()
            }

            fn page_size(&self) -> u16 {
                self.header.page_size()
            }

            fn content_start(&self) -> u16 {
                self.header.content_start()
            }

            fn set_next_overflow(&mut self, overflowpage: PageId) {
                self.header.set_next_overflow(overflowpage)
            }

            fn free_space_start(&self) -> u16 {
                self.header.free_space_start()
            }

            fn cell_count(&self) -> u16 {
                self.header.cell_count()
            }

            fn is_overflow(&self) -> bool {
                self.header.is_overflow()
            }

            fn id(&self) -> PageId {
                self.header.id()
            }

            fn get_next_overflow(&self) -> Option<PageId> {
                self.header.get_next_overflow()
            }

            fn set_type(&mut self, page_type: PageType) {
                self.header.set_type(page_type)
            }

            fn set_cell_count(&mut self, count: u16) {
                self.header.set_cell_count(count)
            }

            fn set_content_start_ptr(&mut self, content_start: u16) {
                self.header.set_content_start_ptr(content_start)
            }

            fn set_free_space_ptr(&mut self, ptr: u16) {
                self.header.set_free_space_ptr(ptr)
            }
        }
    };

    ($page_type:ident<$generic:ident: $trait_bound:path>) => {
        impl<$generic: $trait_bound> HeaderOps for $page_type<$generic> {
            fn type_of(&self) -> PageType {
                self.header.type_of()
            }

            fn page_size(&self) -> u32 {
                self.header.page_size()
            }

            fn content_start(&self) -> u16 {
                self.header.content_start()
            }

            fn set_next_overflow(&mut self, overflowpage: PageId) {
                self.header.set_next_overflow(overflowpage)
            }

            fn free_space_start(&self) -> u16 {
                self.header.free_space_start()
            }

            fn cell_count(&self) -> u16 {
                self.header.cell_count()
            }

            fn is_overflow(&self) -> bool {
                self.header.is_overflow()
            }

            fn id(&self) -> PageId {
                self.header.id()
            }

            fn get_next_overflow(&self) -> Option<PageId> {
                self.header.get_next_overflow()
            }

            fn set_type(&mut self, page_type: PageType) {
                self.header.set_type(page_type)
            }

            fn set_cell_count(&mut self, count: u16) {
                self.header.set_cell_count(count)
            }

            fn set_content_start_ptr(&mut self, content_start: u16) {
                self.header.set_content_start_ptr(content_start)
            }

            fn set_free_space_ptr(&mut self, ptr: u16) {
                self.header.set_free_space_ptr(ptr)
            }
        }
    };
}
