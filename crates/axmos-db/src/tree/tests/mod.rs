mod tests;
mod utils;

use crate::{param_tests, param2_tests};

#[allow(unused_imports)]
use tests::{
    test_del_i32, test_insert_delete_reinsert_i32, test_interleaved_i32, test_rand_del_i32,
    test_rand_i32, test_reverse_i32, test_seq_i32, test_upsert_i32, test_zigzag_i32,
};

// Sequential insertion tests
param_tests!(test_seq_i32, count => [10, 50, 100, 500, 1000, 5000]);

// Reverse insertion tests
param_tests!(test_reverse_i32, count => [10, 50, 100, 500, 1000]);

// Interleaved insertion tests
param_tests!(test_interleaved_i32, count => [10, 50, 100, 500]);

// Zigzag insertion tests
param_tests!(test_zigzag_i32, count => [10, 50, 100, 500]);

// Upsert tests
param_tests!(test_upsert_i32, count => [10, 50, 100, 500]);

// Insert-delete-reinsert cycle tests
param_tests!(test_insert_delete_reinsert_i32, count => [20, 100, 500]);

// Random insertion tests with different seeds
param2_tests!(test_rand_i32, count, seed => [
    (10, 42),
    (50, 123),
    (100, 456),
    (500, 789),
    (1000, 999),
    (2000, 31415)
]);

// Random deletion tests
param2_tests!(test_rand_del_i32, count, seed => [
    (10, 42),
    (50, 123),
    (100, 456),
    (500, 789)
]);

// Deletion tests with different counts
param2_tests!(test_del_i32, count, delete_count => [
    (20, 10),
    (100, 50),
    (100, 90),
    (500, 250),
    (500, 450)
]);
