//#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

pub mod segment;
pub mod commitlog;
pub mod index;

use std::{io, fs, thread, env};
use std::cmp::{Ord, Ordering, PartialOrd, PartialEq};
use std::fs::{OpenOptions, File};
use std::io::{BufReader, BufWriter, Write, Read, BufRead, SeekFrom, Seek};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::collections::BinaryHeap;

use byteorder::{ByteOrder, BigEndian, WriteBytesExt, ReadBytesExt};
use memmap::{MmapMut, MmapOptions};

pub type Offset = u64;
