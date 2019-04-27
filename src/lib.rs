#![allow(dead_code)]
#![allow(unused_imports)]
//#![allow(non_snake_case)]
#![allow(unused_variables)]
//#![feature(bufreader_buffer)]
use std::{io, fs, thread, env};
use std::fs::{OpenOptions, File};
use std::io::{BufReader, BufWriter, Write, Read, BufRead, SeekFrom, Seek};
use std::io::prelude::*;
use std::path::PathBuf;

use byteorder::{ByteOrder, BigEndian, WriteBytesExt, ReadBytesExt};
use memmap::{MmapMut, MmapOptions};


pub type Offset = u64;

fn idx_name(base_offset: Offset) -> PathBuf {
    PathBuf::from(format!("{:0>20}.index", base_offset))
}

fn log_name(base_offset: Offset) -> PathBuf {
    PathBuf::from(format!("{:0>20}.log", base_offset))
}

const OFFSET_WIDTH: u32 = 4;
const OFFSET_OFFSET: u32 = 0;
const POSITION_WIDTH: u32 = 4;
const POSITION_OFFSET: u32 = 4;
const ENTRY_WIDTH: u32 = 8;

#[derive(Debug, PartialEq)]
pub struct Entry{
    offset: Offset,
    position: Offset,
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
    offset: Offset,
    // TODO: mmap mutex
    mmap: MmapMut,
    // TODO: readwrite mutex
}



impl Index {
    pub fn new(mut path: PathBuf, bytes: u64, base_offset: Offset) -> io::Result<Index> {
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
            offset: size,
            mmap: mmap,
        })
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
        self.write_at(relative_entry, self.offset)?;
        // TODO: mutex update
        self.offset = self.offset + ENTRY_WIDTH as Offset;

        Ok(())
    }

    pub fn read_entry(&mut self, offset: Offset) -> io::Result<Entry> {
        let offset_start = offset as usize;
        let offset_end = offset_start + OFFSET_WIDTH as usize;
        let position_start = offset_end;
        let position_end = position_start + POSITION_WIDTH as usize;

        let relative_off = BigEndian::read_u32(&self.mmap[offset_start ..offset_end]);
        let relative_pos = BigEndian::read_u32(&self.mmap[position_start..position_end]);
        let mut relative_entry = RelativeEntry{
            offset: relative_off,
            position: relative_pos,
        };

        let mut result = Entry{offset: 0, position: 0};
        // TODO: wrap (the entry offset?) with mutex
        relative_entry.fill(&mut result, self.base_offset);

        Ok(result)
    }

}


pub struct Segment {
    base_offset: Offset,
    next_offset: Offset,
    position: Offset,
    max_bytes: u64,
    writer: Option<File>,
    reader: Option<File>,
    // log: File, underlying FD
    // index: Index,
    // TODO: add mutex
}

impl Segment {
    pub fn new(partition_path: String, base_offset: Offset, max_bytes: u64) -> io::Result<Segment> {
        let mut log_path = PathBuf::new();

        // create index
        let log_index = Index::new(PathBuf::from(&partition_path), 0, base_offset)?;

        // Create log
        log_path.push(partition_path);
        log_path.push(log_name(base_offset));
        let log_writer = OpenOptions::new().create(true).write(true)
            .append(true).open(log_path.clone())?;
        let log_reader = OpenOptions::new().read(true).open(log_path)?;
        let size = log_writer.metadata()?.len();

        Ok(Segment {
            base_offset: base_offset,
            next_offset: base_offset,
            position: size, // file size
            writer: Some(log_writer),
            reader: Some(log_reader),
            max_bytes: max_bytes,
        })
    }
}



#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    //use std::fs;
    use super::*;

    #[test]
    fn new_index() {
        let tmp = tempdir().unwrap();

        let index = Index::new(PathBuf::from(tmp.path()), 0, 0).unwrap();

        let mut expected_path = PathBuf::from(tmp.path());
        expected_path.push("00000000000000000000.index");

        assert_eq!(expected_path, index.path, "path matches expectations");
        assert!(index.path.exists(), "file exists");
        assert_eq!(index.file.metadata().unwrap().len(), 10485760, "file size");
        assert_eq!(index.bytes, 10485760, "bytes");
        assert_eq!(index.base_offset, 0, "base_offset");
        assert_eq!(index.offset, 0, "offset");
    }

    #[test]
    fn index_write_entry_offset_one() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(PathBuf::from(tmp.path()), 32, 0).unwrap();
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
    fn index_write_entry_at_base_offset() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(PathBuf::from(tmp.path()), 32, 2).unwrap();

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
    fn index_write_entry_overflow() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(PathBuf::from(tmp.path()), 16, 0).unwrap();
        index.write_entry(Entry{offset: 1, position: 16}).unwrap();
        index.write_entry(Entry{offset: 2, position: 54}).unwrap();
        index.write_entry(Entry{offset: 3, position: 62}).is_err();
    }

    #[test]
    fn index_read_entry() {
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

        let mut index = Index::new(PathBuf::from(tmp.path()), 32, 0).unwrap();

        let expected = Entry{offset: 0, position: 0};
        let entry = index.read_entry(0).unwrap();
        assert_eq!(entry, expected, "first offset");

        let expected = Entry{offset: 1, position: 42};
        let entry = index.read_entry(ENTRY_WIDTH as u64).unwrap();
        assert_eq!(entry, expected, "second offset");
    }
}
