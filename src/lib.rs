#![feature(allocator_api)]
#![feature(pointer_is_aligned_to)]
#![feature(set_ptr_value)]
#![feature(debug_closure_helpers)]
#![feature(trait_alias)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![feature(slice_ptr_get)]
//mod btree;
mod configs;
mod database;
mod io;
mod macros;
mod serialization;
mod storage;

mod types;

use jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub(crate) use configs::*;


use types::DataType;
