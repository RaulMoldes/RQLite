use super::utils::{TestConfig, TestDb, make_relation};
use crate::{
    ObjectId,
    io::pager::{BtreeBuilder, SharedPager},
    multithreading::coordinator::Snapshot,
    schema::catalog::{Catalog, CatalogError},
    storage::page::BtreePage,
};

fn create_test_catalog(pager: SharedPager) -> Catalog {
    let meta_table = pager
        .write()
        .allocate_page::<BtreePage>()
        .expect("Failed to allocate meta_table");
    let meta_index = pager
        .write()
        .allocate_page::<BtreePage>()
        .expect("Failed to allocate meta_index");
    Catalog::new(pager, meta_table, meta_index)
}

pub fn test_get_relation_by_name(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("catalog_get_by_name", &config).expect("Failed to create test db");
    let catalog = create_test_catalog(db.pager.clone());
    let builder = BtreeBuilder::default().with_pager(db.pager.clone());
    let snapshot = Snapshot::default();

    for i in 0..count {
        let root_page = db.pager.write().allocate_page::<BtreePage>().expect("Failed to allocate page");
        let rel = make_relation(i as ObjectId, &format!("named_table_{}", i), root_page);
        catalog
            .store_relation(rel, &builder, snapshot.xid())
            .expect("Store failed");
    }

    for i in 0..count {
        let rel = catalog
            .get_relation_by_name(&format!("named_table_{}", i), &builder, &snapshot)
            .expect("Get by name failed");

        assert!(rel.object_id() == i as ObjectId);
        assert!(rel.name() == format!("named_table_{}", i));
    }

    let missing = catalog.get_relation_by_name("nonexistent_table", &builder, &snapshot);
    assert!(matches!(missing, Err(CatalogError::InvalidObjectName(_))));
}

pub fn test_store_relation(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("catalog_store", &config).expect("Failed to create test db");
    let catalog = create_test_catalog(db.pager.clone());
    let builder = BtreeBuilder::default().with_pager(db.pager.clone());
    let snapshot = Snapshot::default();

    for i in 0..count {
        let root_page = db.pager.write().allocate_page::<BtreePage>().expect("Failed to allocate page");
        let rel = make_relation(i as ObjectId, &format!("table_{}", i), root_page);
        catalog
            .store_relation(rel, &builder, snapshot.xid())
            .expect("Store failed");
    }

    for i in 0..count {
        let rel = catalog
            .get_relation(i as ObjectId, &builder, &snapshot)
            .expect("Get failed");

        assert!(rel.object_id() == i as ObjectId);
        assert!(rel.name() == format!("table_{}", i));
    }
}

pub fn test_get_relation(count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("catalog_get", &config).expect("Failed to create test db");
    let catalog = create_test_catalog(db.pager.clone());
    let builder = BtreeBuilder::default().with_pager(db.pager.clone());
    let snapshot = Snapshot::default();

    for i in 0..count {
        let root_page = db.pager.write().allocate_page::<BtreePage>().expect("Failed to allocate page");
        let rel = make_relation(i as ObjectId, &format!("get_table_{}", i), root_page);
        catalog
            .store_relation(rel, &builder, snapshot.xid())
            .expect("Store failed");
    }

    for i in 0..count {
        let rel = catalog
            .get_relation(i as ObjectId, &builder, &snapshot)
            .expect("Get failed");

        assert!(rel.object_id() == i as ObjectId);
        assert!(rel.name() == format!("get_table_{}", i));
    }

    let missing = catalog.get_relation(count as ObjectId + 999, &builder, &snapshot);
    assert!(matches!(missing, Err(CatalogError::TableNotFound(_))));
}

pub fn test_delete_relation(count: usize, delete_count: usize) {
    let config = TestConfig::default();
    let db = TestDb::new("catalog_delete", &config).expect("Failed to create test db");
    let catalog = create_test_catalog(db.pager.clone());
    let builder = BtreeBuilder::default().with_pager(db.pager.clone());
    let snapshot = Snapshot::default();

    for i in 0..count {

        let root_page = db.pager.write().allocate_page::<BtreePage>().expect("Failed to allocate page");

        let rel = make_relation(i as ObjectId, &format!("deletable_{}", i), root_page);
        catalog
            .store_relation(rel, &builder, snapshot.xid())
            .expect("Store failed");
    }

    for i in 0..delete_count {
        let rel = catalog
            .get_relation(i as ObjectId, &builder, &snapshot)
            .expect("Get failed");
        catalog
            .remove_relation(rel, &builder, &snapshot, false)
            .expect("Delete failed");
    }

    for i in 0..delete_count {
        let result = catalog.get_relation(i as ObjectId, &builder, &snapshot);
        assert!(matches!(result, Err(CatalogError::TableNotFound(_))));
    }

    for i in delete_count..count {
        let result = catalog.get_relation(i as ObjectId, &builder, &snapshot);
        assert!(result.is_ok());
    }
}
