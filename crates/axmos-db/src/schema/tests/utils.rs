use crate::{
    DBConfig, ObjectId,
    io::pager::{Pager, SharedPager},
    schema::base::{Column, Relation},
    types::{PageId, DataTypeKind},
};
use std::path::PathBuf;
use tempfile::TempDir;

pub struct TestConfig {
    pub db_config: DBConfig,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            db_config: DBConfig::default(),
        }
    }
}

pub struct TestDb {
    pub pager: SharedPager,
    pub path: PathBuf,
    _temp_dir: TempDir,
}

impl TestDb {
    pub fn new(name: &str, config: &TestConfig) -> std::io::Result<Self> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join(format!("{}.db", name));

        let pager = SharedPager::from(Pager::from_config(config.db_config, &path)?);

        Ok(Self {
            pager,
            path,
            _temp_dir: temp_dir,
        })
    }
}

pub fn make_relation(id: ObjectId, name: &str, root_page: PageId) -> Relation {
    let columns = vec![
        Column::new_with_defaults(DataTypeKind::BigUInt, "id"),
        Column::new_with_defaults(DataTypeKind::Blob, "name"),
    ];
    Relation::table(id, name, root_page, columns)
}
