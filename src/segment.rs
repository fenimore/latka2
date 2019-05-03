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


#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct MaxBytes(pub u64, pub u64);

#[derive(Debug)]
pub struct InactiveSegment(PathBuf, Offset, MaxBytes);

impl InactiveSegment {
    pub fn new(path: PathBuf, offset: Offset, max_bytes: MaxBytes) -> InactiveSegment {
        InactiveSegment(path, offset, max_bytes)
    }
    pub fn activate(&self) -> io::Result<Segment> {
        Segment::new(&mut self.0.clone(), self.1, self.2)
    }
}

// Implement ordering for the segment in a commit log's segment list
impl Eq for InactiveSegment { }
impl PartialEq for InactiveSegment {
    fn eq(&self, other: &Self) -> bool { self.1 == other.1 }
}
impl Ord for InactiveSegment {
    fn cmp(&self, other: &Self) -> Ordering { self.1.cmp(&other.1) }  // .reverse()
}
impl PartialOrd for InactiveSegment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

#[derive(Debug)]
pub struct Segment {
    pub base_offset: Offset,
    next_offset: Offset, // TODO: put a mutex :(
    position: Offset,
    max_bytes: MaxBytes,
    path: PathBuf,
    writer: File,
    reader: File,
    index: Index,
    suffix: String,
}

impl Segment {
    pub fn new(path: &mut PathBuf, base_offset: Offset, max_bytes: MaxBytes) -> io::Result<Segment> {
        // TODO: atm new performs double duty as `new` and `load`,
        // for load to work, I'd need to set the base_offset from whatever
        // the index tells me to do. Unless I go the jocko route and
        // rebuild the index on load
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
    pub fn deactivate(&self) -> InactiveSegment {
        InactiveSegment(self.path.clone(), self.base_offset, self.max_bytes)
    }
    pub fn size(&self) -> u64 {
        self.writer.metadata().unwrap().len()
    }
    pub fn current_position(&self) -> u64 { self.position }
    pub fn newest_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn write_index_entry(&mut self, entry: Entry) -> io::Result<()> {
        self.index.write_entry(entry)
    }

    pub fn is_full(&self) -> bool {
        // TODO: add mutex
        return self.position >= self.max_bytes.0
    }

    // TODO: build index if loading the segment and rebuilding index ¯\_(ツ)_/¯
    // build_index
    // pub fn read_at(&mut self, buf: &[u8], offset: Offset) -> io::Result<usize> {Ok(0)}
}

impl Write for Segment {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut buf_writer = BufWriter::new(&self.writer);
        let n = buf_writer.write(buf)?;
        self.next_offset += 1;
        self.position += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> { self.writer.flush() }
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

    #[test]
    fn it_writes() {
        let tmp = tempdir().unwrap();
        let mut path = tmp.path().to_path_buf();
        let mut segment = Segment::new(&mut path.clone(), 0, MaxBytes(64, 64)).unwrap();
        let n = segment.write("YELLOW SUBMARINE".as_bytes()).unwrap();
        let result = {
            let mut buf = [0; 16];
            path.push("00000000000000000000.log");
            let mut log_file = OpenOptions::new().create(false).read(true).open(path).unwrap();
            log_file.read_exact(&mut buf).unwrap();
            buf
        };
        assert_eq!(n, 16, "16 bytes");
        assert_eq!("YELLOW SUBMARINE".as_bytes(), result, "data write");

        assert_eq!(segment.next_offset, 1, "increment next offset");
        assert_eq!(segment.position, 16, "position");
        assert_eq!(segment.base_offset, 0, "base_offset");
        assert_eq!(segment.max_bytes, MaxBytes(64, 64), "max_bytes");
    }

    #[test]
    fn default_new_segment() {
        let tmp = tempdir().unwrap();
        let mut path = tmp.path().to_path_buf().clone();
        let segment = Segment::new(&mut path, 0, MaxBytes(64, 64)).unwrap();

        // TODO test writer and reader file permissions
        assert_eq!(segment.index.len().unwrap(), 64, "file size");
        assert!(segment.path.exists(), "log file exists");
        assert!(segment.index.path_buf().exists(), "index file exists");
        assert_eq!(segment.position, 0, "position");
        assert_eq!(segment.base_offset, 0, "base_offset");
        assert_eq!(segment.max_bytes, MaxBytes(64, 64), "max_bytes");
        assert_eq!(segment.next_offset, 0, "next_offset");
    }
}
