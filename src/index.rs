use std::{io};
use std::fs::{OpenOptions, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use byteorder::{ByteOrder, BigEndian, WriteBytesExt};
use memmap::{MmapMut, MmapOptions};

use crate::{Offset};
use crate::entry::{Entry, RelativeEntry, ENTRY_WIDTH};

fn idx_name(base_offset: Offset) -> PathBuf {
    PathBuf::from(format!("{:0>20}.index", base_offset))
}


// The Index is a memory mapped `.index` file
#[derive(Debug)]
pub struct Index {
    // options
    path: PathBuf,
    max_bytes: u64,
    base_offset: Offset,
    // attributes
    file: File,
    position: Offset, // position mutex
    // TODO: mmap mutex
    mmap: MmapMut,
    // TODO: readwrite mutex
}

impl Index {
    pub fn open(path: PathBuf, base_offset: Offset, max_bytes: u64) -> io::Result<Index> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        let size = file.metadata()?.len();
        if size == 0 {
            file.set_len(max_bytes)?;
        }

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let mut index = Index {
            max_bytes: max_bytes,
            path: path,
            base_offset: base_offset,
            file: file,
            position: 0,
            mmap: mmap,
        };
        let entry = index.find_latest_entry()?;
        index.position = entry.position;
        Ok(index)
    }

    pub fn new(mut path: PathBuf, base_offset: Offset, max_bytes: u64) -> io::Result<Index> {
        if max_bytes % ENTRY_WIDTH as u64 != 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "max_bytes must be divisible by 8"))
        } else if max_bytes < 16 {
            return Err(io::Error::new(io::ErrorKind::Other, "max_bytes must 16 or greater"))
        }


        path.push(idx_name(base_offset));
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        let size = file.metadata()?.len();
        if size == 0 {
            file.set_len(max_bytes)?;
        }

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Index {
            max_bytes: max_bytes,
            path: path,
            base_offset: base_offset,
            file: file,
            position: 0,
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
    pub fn is_empty(&self) -> bool {
        // if the first two entries are zeroes, then the index is "empty"
        self.mmap[0..16] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
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
        self.write_at(relative_entry, relative_entry.offset as Offset * ENTRY_WIDTH as Offset)?;
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

    pub fn find_latest_entry(&mut self) -> io::Result<Entry> {
        // super naive and dumb "search" for latest entry
        let end = self.len()?;
        let index_count = end / 8;

        let mut latest_entry = Entry{offset: 0, position: 0};
        for x in 0..index_count {
            let entry = self.read_log_entry(x)?;
            if entry.offset >= latest_entry.offset {
                latest_entry.offset = entry.offset;
                latest_entry.position = entry.position;
            }
        };
        Ok(latest_entry)
    }

    pub fn read_log_entry(&mut self, offset: Offset) -> io::Result<Entry> {
        self.read_entry(offset * ENTRY_WIDTH as u64)
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

        let index = Index::new(PathBuf::from(tmp.path()), 0, 64).unwrap();

        let mut expected_path = PathBuf::from(tmp.path());
        expected_path.push("00000000000000000000.index");

        assert_eq!(expected_path, index.path, "path matches expectations");
        assert!(index.path.exists(), "file exists");
        assert_eq!(index.file.metadata().unwrap().len(), 64, "file size");
        assert_eq!(index.max_bytes, 64, "bytes");
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
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 16,
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
    fn it_finds_last_entry() {
        let tmp = tempdir().unwrap();
        let mut index = Index::new(tmp.path().to_path_buf(), 2, 64).unwrap();

        {
            index.write_entry(Entry{offset: 2, position: 16}).unwrap();
            index.write_entry(Entry{offset: 3, position: 54}).unwrap();
            index.write_entry(Entry{offset: 4, position: 62}).unwrap();
            index.write_entry(Entry{offset: 5, position: 88}).unwrap();
        }

        let entry = index.find_latest_entry().unwrap();
        assert_eq!(entry, Entry{offset: 5, position: 88});
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
