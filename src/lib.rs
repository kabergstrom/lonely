#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs, rust_2018_idioms)]
#![doc(test(attr(deny(rust_2018_idioms, warnings))))]
#![doc(test(attr(allow(unused_extern_crates, unused_variables))))]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
use std as alloc;

mod cache_padded;
mod exec;
mod ring;
pub use exec::{
    load_balance, local_spawn_from_context, local_spawner, Exec, ExecGroup, LocalSpawn,
};

#[cfg(feature = "std")]
pub use exec::DefaultBuildHasher;
pub use ring::{HeapRing, Ring};
