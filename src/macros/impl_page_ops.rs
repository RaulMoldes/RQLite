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

            fn get_rightmost_child(&self) -> Option<PageId> {
                match self {
                    $enum::$interior_variant(page) => page.get_rightmost_child(),
                    $enum::$leaf_variant(_) => {
                        panic!("Leaf pages don't have rightmost child!")
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

            fn remove_child(&mut self, key: &Self::KeyType) -> Option<PageId> {
                match self {
                    $enum::$interior_variant(page) => page.remove_child(key),
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot remove child from leaf page!")
                    }
                }
            }

            fn split_interior(&mut self) -> (Self::KeyType, Self) {
                match self {
                    $enum::$interior_variant(page) => {
                        let (key, new_page) = page.split_interior();
                        (key, $enum::$interior_variant(new_page))
                    }
                    $enum::$leaf_variant(_) => {
                        panic!("Cannot split leaf page as interior!")
                    }
                }
            }

            fn merge_with_next_interior(
                &mut self,
                separator_key: Self::KeyType,
                right_sibling: Self,
            ) {
                match (self, right_sibling) {
                    ($enum::$interior_variant(left), $enum::$interior_variant(right)) => {
                        left.merge_with_next_interior(separator_key, right);
                    }
                    _ => {
                        panic!("Cannot merge non-interior pages with interior merge operation!")
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

            fn split_leaf(&mut self) -> (Self::KeyType, Self) {
                match self {
                    $enum::$leaf_variant(page) => {
                        let (key, new_page) = page.split_leaf();
                        (key, $enum::$leaf_variant(new_page))
                    }
                    $enum::$interior_variant(_) => {
                        panic!("Cannot split interior page as leaf!")
                    }
                }
            }

            fn merge_with_next_leaf(&mut self, right_most: Self) {
                match (self, right_most) {
                    ($enum::$leaf_variant(left), $enum::$leaf_variant(right)) => {
                        left.merge_with_next_leaf(right);
                    }
                    _ => {
                        panic!("Cannot merge non-leaf pages with leaf merge operation!")
                    }
                }
            }
        }
    };
}

