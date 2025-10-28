#![feature(allocator_api)]
#![feature(pointer_is_aligned_to)]
#![feature(set_ptr_value)]
#![feature(debug_closure_helpers)]
#![feature(trait_alias)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![feature(slice_ptr_get)]
#![feature(concat_bytes)]
mod configs;
mod database;
mod io;
mod macros;
mod storage;
mod structures;
mod types;

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

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
pub(crate) use configs::*;
