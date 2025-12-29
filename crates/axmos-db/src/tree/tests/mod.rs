mod tests;
mod utils;

use crate::{param_tests, param2_tests};

#[allow(unused_imports)]
use tests::{
    test_delete, test_insert_delete_reinsert, test_interleaved_insert, test_random_delete,
    test_random_insert, test_reverse_insert, test_sequential_insert, test_upsert,
    test_zigzag_insert,
};

// Sequential insertion tests
param_tests!(test_sequential_insert, count => [10, 50, 100, 500, 1000, 2000]);

// Reverse insertion tests
param_tests!(test_reverse_insert, count => [10, 50, 100, 500, 1000]); //REVISAR EL ULTIMO TEST

// Interleaved insertion tests
param_tests!(test_interleaved_insert, count => [10, 50, 100, 500]);

// Zigzag insertion tests
param_tests!(test_zigzag_insert, count => [10, 50, 100, 500]);

// Upsert tests
param_tests!(test_upsert, count => [10, 50, 100, 500]);

// Insert-delete-reinsert cycle tests
param_tests!(test_insert_delete_reinsert, count => [20, 100, 500]);

// Random insertion tests with different seeds
param2_tests!(test_random_insert, count, seed => [
    (10, 42),
    (50, 123),
    (100, 456),
    (500, 789),
    (1000, 999),
    (2000, 31415) // REVISAR EL ULTIMO TEST
]);

// Random deletion tests
param2_tests!(test_random_delete, count, seed => [
    (10, 42),
    (50, 123),
    (100, 456),
    (500, 789)
]);

// Deletion tests with different counts
param2_tests!(test_delete, count, delete_count => [
    (20, 10),
    (100, 50),
    (100, 90),
    (500, 250),
    (500, 450)
]);
