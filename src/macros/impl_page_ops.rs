#[macro_export]
macro_rules! impl_interior_page_ops {
    (
        $enum:ident<$cell:ty> {
            KeyType = $key_type:ty,
            Leaf = $leaf_variant:ident,
            Interior = $interior_variant:ident $(,)?
        }
    ) => {
        impl InteriorPageOps<$cell> for $enum {
            type KeyType = $key_type;

            fn find_child(&self, key: &Self::KeyType) -> PageId {
                match self {
                    $enum::$interior_variant(page) => page.find_child(key),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot find child on leaf page!")
                    }
                }
            }

            fn try_pop_back_interior(&mut self, min_payload_factor: f32) -> Option<$cell> {
                match self {
                    $enum::$interior_variant(page) => {
                        page.try_pop_back_interior(min_payload_factor)
                    }
                    $enum::$leaf_variant(_) => {
                        panic!("Leaf pages don't have rightmost child!")
                    }
                }
            }

            fn try_pop_front_interior(&mut self, min_payload_factor: f32) -> Option<$cell> {
                match self {
                    $enum::$interior_variant(page) => {
                        page.try_pop_front_interior(min_payload_factor)
                    }
                    $enum::$leaf_variant(_) => {
                        panic!("Leaf pages don't have rightmost child!")
                    }
                }
            }

            fn get_rightmost_child(&self) -> Option<PageId> {
                match self {
                    $enum::$interior_variant(page) => page.get_rightmost_child(),
                    $enum::$leaf_variant(_) => {
                        panic!("Leaf pages don't have rightmost child!")
                    }
                }
            }

            fn get_siblings_for(&self, child_id: PageId) -> (Option<PageId>, Option<PageId>) {
                match self {
                    $enum::$interior_variant(page) => page.get_siblings_for(child_id),
                    $enum::$leaf_variant(_) => {
                        panic!("Leaf pages don't have children!")
                    }
                }
            }

            fn set_rightmost_child(&mut self, page_id: PageId) {
                match self {
                    $enum::$interior_variant(page) => page.set_rightmost_child(page_id),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot set rightmost child on leaf page!")
                    }
                }
            }

            fn insert_child(
                &mut self,
                key: Self::KeyType,
                left_child: PageId,
            ) -> std::io::Result<()> {
                match self {
                    $enum::$interior_variant(page) => page.insert_child(key, left_child),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot insert child into leaf page!")
                    }
                }
            }

            fn get_cell_at_interior(&self, id: u16) -> Option<$cell> {
                match self {
                    $enum::$interior_variant(page) => page.get_cell_at_interior(id),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot get cell from  leaf page using interior operations!")
                    }
                }
            }

            fn take_interior_cells(&mut self) -> Vec<$cell> {
                match self {
                    $enum::$interior_variant(page) => page.take_interior_cells(),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot take cells from leaf page using interior operations!")
                    }
                }
            }

            fn remove_child(&mut self, page_id: PageId) {
                match self {
                    $enum::$interior_variant(page) => page.remove_child(page_id),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot remove child from leaf page!")
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_leaf_page_ops {
    (
        $enum:ident<$cell:ty> {
            KeyType = $key_type:ty,
            Leaf = $leaf_variant:ident,
            Interior = $interior_variant:ident $(,)?
        }
    ) => {
        impl LeafPageOps<$cell> for $enum {
            type KeyType = $key_type;

            fn find(&self, key: &Self::KeyType) -> Option<&$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.find(key),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot use leaf search functions on interior pages!")
                    }
                }
            }

            fn get_cell_at_leaf(&self, id: u16) -> Option<$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.get_cell_at_leaf(id),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot get cell from  interior page using leaf operations!")
                    }
                }
            }

            fn try_pop_back_leaf(&mut self, min_payload_factor: f32) -> Option<$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.try_pop_back_leaf(min_payload_factor),
                    $enum::$interior_variant(_) => {
                        panic!(
                            "Invalid use of leaf page function! Use try_pop_back_interior instead"
                        )
                    }
                }
            }

            fn try_pop_front_leaf(&mut self, min_payload_factor: f32) -> Option<$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.try_pop_front_leaf(min_payload_factor),
                    $enum::$interior_variant(_) => {
                        panic!(
                            "Invalid use of leaf page function! Use try_pop_front_interior instead"
                        )
                    }
                }
            }

            fn take_leaf_cells(&mut self) -> Vec<$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.take_leaf_cells(),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot take cells from interior page using leaf operations!")
                    }
                }
            }

            fn insert(&mut self, key: Self::KeyType, cell: $cell) -> std::io::Result<()> {
                match self {
                    $enum::$leaf_variant(page) => page.insert(key, cell),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot insert into interior page using leaf operations!")
                    }
                }
            }

            fn remove(&mut self, key: &Self::KeyType) -> Option<$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.remove(key),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot remove from interior page using leaf operations!")
                    }
                }
            }
        }
    };
}
