mod tests;
mod utils;

use crate::{param_tests, param2_tests};

#[allow(unused_imports)]
use tests::{
    test_delete_relation, test_get_relation, test_get_relation_by_name, test_store_relation,
};

param_tests!(test_store_relation, count => [1, 5, 10, 50, 100]);

param_tests!(test_get_relation, count => [1, 5, 10, 50, 100]);

param2_tests!(test_delete_relation, count, delete_count => [
    (10, 5),
    (50, 25),
    (100, 50),
    (100, 100)
]);

param_tests!(test_get_relation_by_name, count => [1, 5, 10, 50, 100]);
