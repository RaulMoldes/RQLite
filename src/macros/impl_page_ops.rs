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
            fn find_child(&self, key: &$key_type) -> PageId {
                match self {
                    $enum::$interior_variant(page) => page.find_child(key),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot find child on leaf page!")
                    }
                }
            }

            fn try_pop_children(&mut self, required_size: u16) -> Option<Vec<$cell>> {
                match self {
                    $enum::$interior_variant(page) => page.try_pop_children(required_size),
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

            fn insert_child(&mut self, cell: $cell) -> std::io::Result<()> {
                match self {
                    $enum::$interior_variant(page) => page.insert_child(cell),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot insert child into leaf page!")
                    }
                }
            }

            fn try_insert_children(&mut self, cells: Vec<$cell>) -> std::io::Result<()> {
                match self {
                    $enum::$interior_variant(page) => page.try_insert_children(cells),
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

            fn take_child(&mut self, page_id: PageId) -> Option<$cell> {
                match self {
                    $enum::$interior_variant(page) => page.take_child(page_id),
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
            fn find(&self, key: &$key_type) -> Option<&$cell> {
                match self {
                    $enum::$leaf_variant(page) => page.find(key),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot use leaf search functions on interior pages!")
                    }
                }
            }

            fn scan<'a>(
                &'a self,
                start_key: <$cell as Cell>::Key,
            ) -> impl Iterator<Item = &'a $cell> + 'a
            where
                $cell: 'a,
            {
                match self {
                    $enum::$leaf_variant(page) => page.scan(start_key),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot use leaf search functions on interior pages!")
                    }
                }
            }

            fn get_next(&self) -> Option<PageId> {
                match self {
                    $enum::$leaf_variant(page) => page.get_next(),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot get next from  interior page using leaf operations!")
                    }
                }
            }

            fn set_next(&mut self, page_id: PageId) {
                match self {
                    $enum::$leaf_variant(page) => page.set_next(page_id),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot set next on  interior page using leaf operations!")
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

            fn try_pop(&mut self, required_size: u16) -> Option<Vec<$cell>> {
                match self {
                    $enum::$leaf_variant(page) => page.try_pop(required_size),
                    $enum::$interior_variant(_) => {
                        panic!("Invalid use of leaf page function! Use try_pop_child instead")
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

            fn insert(&mut self, cell: $cell) -> std::io::Result<()> {
                match self {
                    $enum::$leaf_variant(page) => page.insert(cell),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot insert into interior page using leaf operations!")
                    }
                }
            }

            fn try_insert(&mut self, cells: Vec<$cell>) -> std::io::Result<()> {
                match self {
                    $enum::$leaf_variant(page) => page.try_insert(cells),
                    $enum::$interior_variant(_) => {
                        panic!("Cannot insert into interior page using leaf operations!")
                    }
                }
            }

            fn remove(&mut self, key: &$key_type) -> Option<$cell> {
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

#[macro_export]
macro_rules! impl_btree_page_ops {
    (
        $enum:ident {
            Leaf = $leaf_variant:ident,
            Interior = $interior_variant:ident $(,)?
        }
    ) => {
        impl BTreePageOps for $enum {
            fn can_remove(&self, cell_size: u16) -> bool {
                match self {
                    $enum::$leaf_variant(page) => page.can_remove(cell_size),
                    $enum::$interior_variant(page) => page.can_remove(cell_size),
                }
            }

            fn unfuck_cell_offsets(&mut self, removed_offset: u16, removed_size: u16) {
                match self {
                    $enum::$leaf_variant(page) => {
                        page.unfuck_cell_offsets(removed_offset, removed_size)
                    }
                    $enum::$interior_variant(page) => {
                        page.unfuck_cell_offsets(removed_offset, removed_size)
                    }
                }
            }

            fn fits_in(&self, additional_size: u16) -> bool {
                match self {
                    $enum::$leaf_variant(page) => page.fits_in(additional_size),
                    $enum::$interior_variant(page) => page.fits_in(additional_size),
                }
            }

            fn reset_stats(&mut self) {
                match self {
                    $enum::$leaf_variant(page) => page.reset_stats(),
                    $enum::$interior_variant(page) => page.reset_stats(),
                }
            }

            fn get_overflow_size(&self) -> u16 {
                match self {
                    $enum::$leaf_variant(page) => page.get_overflow_size(),
                    $enum::$interior_variant(page) => page.get_overflow_size(),
                }
            }

            fn get_underflow_size(&self) -> u16 {
                match self {
                    $enum::$leaf_variant(page) => page.get_underflow_size(),
                    $enum::$interior_variant(page) => page.get_underflow_size(),
                }
            }
        }
    };
}
