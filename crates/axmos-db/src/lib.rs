#![feature(allocator_api)]
#![feature(pointer_is_aligned_to)]
#![feature(set_ptr_value)]
#![feature(debug_closure_helpers)]
#![feature(trait_alias)]
#![allow(dead_code)]
#![allow(unused_assignments)]
#![allow(unused_variables)]
#![feature(slice_ptr_get)]
#![feature(concat_bytes)]
#![feature(str_from_raw_parts)]
#![feature(current_thread_id)]
pub mod common;
//pub mod database;
pub mod io;
mod macros;
//mod sql;
mod storage;
mod structures;
//mod transactions;
pub mod types;

//#[cfg(test)]
//pub mod test_utils;

/// Jemalloc apparently has better alignment guarantees than rust's standard allocator.
/// Rust's global system allocator does not seem to guarantee that allocations are aligned.
/// Therefore we prefer to use [Jemalloc], to ensure allocations are aligned.
///
///  Docs on Jemalloc: https://manpages.debian.org/jessie/libjemalloc-dev/jemalloc.3.en.html.
///
/// More on alignment guarantees: https://github.com/jemalloc/jemalloc/issues/1533
/// We also provide an API with a custom [Direct-IO] allocator (see [io::disk::linux] for details), but Jemalloc has performed better in benchmarks.
#[cfg(not(miri))]
use jemallocator::Jemalloc;
pub(crate) use types::*;

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
pub use common::*;
//pub use database::Database;
