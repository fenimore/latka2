#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
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

use crate::Offset;

fn idx_name(base_offset: Offset) -> PathBuf {
    PathBuf::from(format!("{:0>20}.index", base_offset))
}

// const OFFSET_WIDTH: u32 = 4;
// const OFFSET_OFFSET: u32 = 0;
// const POSITION_WIDTH: u32 = 4;
// const POSITION_OFFSET: u32 = 4;
const ENTRY_WIDTH: u32 = 8;

#[derive(Debug, PartialEq)]
pub struct Entry{
    offset: Offset,
    position: Offset,
}
impl Entry {
    pub fn new(offset: Offset, position: u64) -> Entry {
        Entry{ offset: offset, position: position }
    }
}

#[derive(Debug)]
pub struct RelativeEntry{
    offset: u32,
    position: u32,
}

impl RelativeEntry {
    pub fn new(entry: Entry, base_offset: Offset) -> RelativeEntry {
        RelativeEntry{
            offset: (entry.offset - base_offset) as u32,
            position: entry.position as u32,
        }
    }
    pub fn fill(&mut self, entry: &mut Entry, base_offset: Offset) {
        entry.offset  = base_offset + self.offset as u64;
        entry.position = self.position as u64;
    }
}



// The Index is a memory mapped `.index` file
#[derive(Debug)]
pub struct Index {
    // options
    path: PathBuf,
    bytes: u64,
    base_offset: Offset,
    // attributes
    file: File,
    position: Offset, // position mutex
    // TODO: mmap mutex
    mmap: MmapMut,
    // TODO: readwrite mutex
}

impl Index {
    pub fn new(mut path: PathBuf, base_offset: Offset, bytes: u64) -> io::Result<Index> {
        let opt_bytes = if bytes > 0 {bytes} else { 10 * 1024 * 1024 };
        path.push(idx_name(base_offset));
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        let size = file.metadata()?.len();
        if size == 0 {
            file.set_len(opt_bytes)?;
        }

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Index {
            bytes: opt_bytes,
            path: path,
            base_offset: base_offset,
            file: file,
            position: size,
            mmap: mmap,
        })
    }
    pub fn path_buf(&self) -> PathBuf {
        self.path.clone()
    }
    pub fn len(&self) -> io::Result<u64> {
        let meta = self.file.metadata()?;
        Ok(meta.len())
    }
    pub fn write_at(&mut self, relative_entry: RelativeEntry, offset: Offset) -> io::Result<()> {
        let mut buf = vec![];
        buf.write_u32::<BigEndian>(relative_entry.offset)?;
        buf.write_u32::<BigEndian>(relative_entry.position)?;

        self.mmap[offset as usize..offset as usize + ENTRY_WIDTH as usize].copy_from_slice(&buf);
        Ok(())
    }

    pub fn write_entry(&mut self, entry: Entry) -> io::Result<()> {
        let relative_entry = RelativeEntry::new(entry, self.base_offset);
        // write_at
        self.write_at(relative_entry, self.position)?;
        self.position = self.position + ENTRY_WIDTH as Offset;

        Ok(())
    }

    pub fn read_at(&mut self, buf: &mut [u8], offset: Offset) -> io::Result<usize> {
        if buf.len() != ENTRY_WIDTH as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "EOF"))
        }

        let off = offset as usize;
        let end = off + ENTRY_WIDTH as usize;
        buf.copy_from_slice(&self.mmap[off..end]);
        Ok(ENTRY_WIDTH as usize)
    }

    pub fn read_entry(&mut self, offset: Offset) -> io::Result<Entry> {
        let mut buffer = [0; 8];
        let _ = self.read_at(&mut buffer, offset)?;

        let relative_off = BigEndian::read_u32(&buffer[0..4]);
        let relative_pos = BigEndian::read_u32(&buffer[4..8]);
        let mut relative_entry = RelativeEntry{
            offset: relative_off,
            position: relative_pos,
        };

        let mut result = Entry{offset: 0, position: 0};
        // TODO: wrap (the entry offset?) with mutex
        relative_entry.fill(&mut result, self.base_offset);

        Ok(result)
    }

    pub fn read_entry_at_log_offset(&mut self, offset: Offset) -> io::Result<Entry> {
        self.read_entry(offset * ENTRY_WIDTH as Offset)
    }

}


#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use super::*;


    #[test]
    fn it_defaults_new() {
        let tmp = tempdir().unwrap();

        let index = Index::new(PathBuf::from(tmp.path()), 0, 0).unwrap();

        let mut expected_path = PathBuf::from(tmp.path());
        expected_path.push("00000000000000000000.index");

        assert_eq!(expected_path, index.path, "path matches expectations");
        assert!(index.path.exists(), "file exists");
        assert_eq!(index.file.metadata().unwrap().len(), 10485760, "file size");
        assert_eq!(index.bytes, 10485760, "bytes");
        assert_eq!(index.base_offset, 0, "base_offset");
        assert_eq!(index.position, 0, "position");
    }

    #[test]
    fn it_writes_entry_offset_one() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(PathBuf::from(tmp.path()), 0, 32).unwrap();
        let entry = Entry{offset: 1, position: 16};
        index.write_entry(entry).unwrap();

        let mut buffer = [0; 32];
        index.file.read(&mut buffer).unwrap();
        assert_eq!(buffer, [
            0, 0, 0, 1, 0, 0, 0, 16,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]);
    }

    #[test]
    fn it_writes_entry_at_base_offset() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(tmp.path().to_path_buf(), 2, 32).unwrap();

        index.write_entry(Entry{offset: 2, position: 16}).unwrap();
        index.write_entry(Entry{offset: 3, position: 54}).unwrap();
        index.write_entry(Entry{offset: 4, position: 62}).unwrap();

        let mut buffer = [0; 32];
        index.file.read(&mut buffer).unwrap();
        assert_eq!(buffer, [
            0, 0, 0, 0, 0, 0, 0, 16,
            0, 0, 0, 1, 0, 0, 0, 54,
            0, 0, 0, 2, 0, 0, 0, 62,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]);
    }

    #[test]
    #[should_panic]
    fn it_writes_entry_overflow() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(tmp.path().to_path_buf(), 0, 16).unwrap();
        index.write_entry(Entry{offset: 1, position: 16}).unwrap();
        index.write_entry(Entry{offset: 2, position: 54}).unwrap();
        index.write_entry(Entry{offset: 3, position: 62}).unwrap();
    }

    #[test]
    fn it_reads_entry() {
        let tmp = tempdir().unwrap();
        {
            let mut path = PathBuf::from(tmp.path());
            path.push("00000000000000000000.index");
            let indexes = [
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 1, 0, 0, 0, 42,
                0, 0, 0, 2, 0, 0, 0, 62,
                0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut index_file = OpenOptions::new().create(true).write(true).open(path).unwrap();
            index_file.write(&indexes).unwrap();
        }

        let mut index = Index::new(PathBuf::from(tmp.path()), 0, 32).unwrap();

        let expected = Entry{offset: 0, position: 0};
        let entry = index.read_entry(0).unwrap();
        assert_eq!(entry, expected, "first offset");


        let expected = Entry{offset: 1, position: 42};
        let entry = index.read_entry(ENTRY_WIDTH as u64).unwrap();
        assert_eq!(entry, expected, "second offset");
    }
}