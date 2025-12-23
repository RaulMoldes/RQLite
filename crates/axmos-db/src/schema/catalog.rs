use crate::{ObjectId, UInt64, io::pager::SharedPager, schema::Schema, storage::page::BtreePage, tree::{
    accessor::{BtreeReadAccessor, BtreeWriteAccessor}, bplustree::{Btree, SearchResult, BtreeError}, cell_ops::AsKeyBytes, comparators::DynComparator}, types::PageId};

use super::{base::Relation, meta_table_schema};

pub(crate) struct BtreeBuilder {
    pager: SharedPager,
    min_keys: usize,
    num_siblings_per_side: usize,
}

#[derive(Debug)]
pub enum CatalogError {
    TableNotFound(ObjectId),
    BtreError(BtreeError)
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(id) => write!(f, "Table not found {id}"),
            Self::BtreError(err) => write!(f, "Btree error: {err}")
        }
    }
}

impl From<BtreeError> for CatalogError{
    fn from(value: BtreeError) -> Self {
        Self::BtreError(value)
    }
}

impl std::error::Error for CatalogError {}
type CatalogResult<T> = Result<T, CatalogError>;

impl BtreeBuilder {

    pub(crate) fn build_tree(&self, root: PageId, schema: &Schema) -> Btree<BtreePage, DynComparator, BtreeReadAccessor>{
        let comparator = schema.comparator();
        let pager = self.pager.clone();
        Btree::new(root, pager, self.min_keys, self.num_siblings_per_side, comparator).with_accessor(BtreeReadAccessor::new())
    }


    pub(crate) fn build_tree_mut(&self, root: PageId, schema: &Schema) -> Btree<BtreePage, DynComparator, BtreeWriteAccessor>{
        let comparator = schema.comparator();
        let pager = self.pager.clone();
        Btree::new(root, pager, self.min_keys, self.num_siblings_per_side, comparator).with_accessor(BtreeWriteAccessor::new())
    }
}


#[derive(Debug)]
pub(crate) struct Catalog {
    meta_table: PageId,
  //  meta_index: PageId,
    meta_table_schema: Schema,
 //   meta_index_schema: Schema,
 last_stored_object: ObjectId
}

impl Catalog {
     pub(crate) fn new(meta_table: PageId) -> Self {
        Self {
            meta_table,
            meta_table_schema: meta_table_schema(),
            //meta_index_schema: meta_idx_schema(),
            last_stored_object: 0

        }


    }

    pub(crate) fn with_last_stored_object(mut self, last_stored: ObjectId) -> Self {
        self.last_stored_object = last_stored;
        self
    }



    /// Directly gets a relation from the meta table
    pub(crate) fn get_relation(
        &self,
        id: ObjectId,
        builder: BtreeBuilder
    ) -> CatalogResult<Relation> {
        let mut meta_table = builder.build_tree(self.meta_table, &meta_table_schema());
        let row_id = UInt64(id);
        let result = meta_table.search(row_id.as_key_bytes())?;

        let payload = match result {
            SearchResult::Found(_) => {},
            SearchResult::NotFound(_) => {
                return Err(CatalogError::TableNotFound(id));
            }
        };


        unimplemented!()
    }



}
