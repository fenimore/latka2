// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
pub mod segment;
pub mod commitlog;
pub mod index;
pub mod message;

pub type Offset = u64;

const TEN_MB: u64 = 1024 * 1024 * 1;
const DEFAULT_SEGMENT_MAX_BYTES: u64 = TEN_MB;
const DEFAULT_INDEX_MAX_BYTES: u64 = TEN_MB;
