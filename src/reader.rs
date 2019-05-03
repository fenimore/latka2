// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
use std::{io};
use std::cmp::{Ord, Ordering, PartialOrd, PartialEq};
use std::fs::{OpenOptions, File};
use std::io::{BufWriter, Write, Read, SeekFrom, Seek};
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};

use crate::{Offset};
use crate::index::{Index, Entry};

#[derive(Debug)]
pub struct Reader {
    offset: Offset,
    max_bytes: u64,

}

impl Reader {
    pub fn new(path: &mut PathBuf, base_offset: Offset, max_bytes: MaxBytes) -> io::Result<Segment> {
        path.push(format!("{:0>20}.log", base_offset));
        let log_writer = OpenOptions::new().create(true).write(true)
            .append(true).open(path.clone())?;
        let log_reader = OpenOptions::new().read(true).open(path.clone())?;
        let size = log_writer.metadata()?.len();

        let mut index_path = path.clone();
        index_path.pop();
        let log_index = Index::new(index_path.clone(), base_offset, max_bytes.1)?;

        Ok(
            Segment {
                base_offset: base_offset,
                next_offset: base_offset,
                position: size, // file size
                writer: log_writer,
                reader: log_reader,
                max_bytes: max_bytes,
                index: log_index,
                path: path.clone(),
                suffix: format!("{:0>20}", base_offset),
            }
        )
    }
}

impl Read for Segment {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.reader.read(buf) }
}

impl Seek for Segment {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> { return self.reader.seek(offset) }
}


#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use super::*;

    #[test]
    fn it_is_full_when_full() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().to_path_buf();
        let mut segment = Segment::new(&mut path.clone(), 0, MaxBytes(16, 64)).unwrap();
        let _ = segment.write("YELLOW ".as_bytes()).unwrap();
        assert!(!segment.is_full(), "not full yet");
        let _ = segment.write("SUBMARINE".as_bytes()).unwrap();
        assert!(segment.is_full(), "is full");
        let _ = segment.write("?".as_bytes()).unwrap();
        assert!(segment.is_full(), "still full");

        assert_eq!(segment.next_offset, 3, "increment 3 offsets");
        assert_eq!(segment.position, 17, "position");
    }
}
