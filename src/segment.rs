use std::{io};
use std::cmp::{Ord, Ordering, PartialOrd, PartialEq};
use std::fs::{OpenOptions, File};
use std::io::{BufWriter, Write, Read, SeekFrom, Seek};
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};

use crate::{Offset};
use crate::index::{Index};
use crate::entry::{Entry};

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct MaxBytes(pub u64, pub u64);

#[derive(Debug)]
pub struct OpenSegment {
    log_reader: File,
    log_writer: File,
    log_index: Index,
}


impl Write for OpenSegment {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut buf_writer = BufWriter::new(&self.log_writer);
        let n = buf_writer.write(buf)?;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> { self.log_writer.flush() }
}

impl Read for OpenSegment {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.log_reader.read(buf) }
}

impl Seek for OpenSegment {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> { return self.log_reader.seek(offset) }
}



#[derive(Debug, Clone)]
pub struct SegmentMeta {
    segment_path: PathBuf,
    index_path: PathBuf,
    pub base_offset: Offset,
    next_offset: Offset,
    position: Offset,
    max_bytes: MaxBytes,
}


impl SegmentMeta {
    pub fn load(path: PathBuf, max_bytes: MaxBytes) -> Option<SegmentMeta> {
        if path.is_dir() { return None }
        let ext = match path.extension() {
            Some(ext) => {
                ext.to_string_lossy()
            },
            None => { return None }
        };
        if !ext.contains("log") { return None }
        let stem = match path.file_stem() {
            Some(stem) => { stem.to_string_lossy() },
            None => { return None }
        };
        let offset = match stem.parse::<Offset>() {
            Ok(off) => off,
            _ => { return None },
        };

        let mut base_path = path.clone();
        base_path.pop();
        let mut meta = SegmentMeta::new(base_path, offset, max_bytes);
        let mut open_segment = meta.open().ok()?;
        meta.position = meta.size();
        let entry = open_segment.log_index.find_latest_entry().ok()?;

        meta.next_offset = if open_segment.log_index.is_empty() {
            entry.offset
        } else {
            entry.offset + 1
        };
        Some(meta)
    }

    pub fn new(base_path: PathBuf, base_offset: Offset, max_bytes: MaxBytes) -> SegmentMeta {
        let mut log_path = base_path.clone();
        let mut index_path = base_path.clone();
        log_path.push(format!("{:0>20}.log", base_offset));
        index_path.push(format!("{:0>20}.index", base_offset));
        SegmentMeta{
            segment_path: log_path,
            index_path: index_path,
            base_offset: base_offset,
            next_offset: base_offset,
            position: 0,
            max_bytes: max_bytes,
        }
    }

    pub fn open(&self) -> io::Result<(OpenSegment)> {
        let log_writer = OpenOptions::new().create(true).write(true)
            .append(true).open(self.segment_path.clone())?;
        let log_reader = OpenOptions::new().read(true).open(self.segment_path.clone())?;
        let log_index = Index::open(self.index_path.clone(), self.base_offset, self.max_bytes.1)?;

        Ok(OpenSegment{log_reader: log_reader, log_writer: log_writer, log_index: log_index})
    }

    pub fn size(&self) -> u64 {
        match self.open().ok() {
            Some(seg) => seg.log_writer.metadata().unwrap().len(),
            None => 0,
        }
    }

    pub fn write_index_entry(&mut self, entry: Entry) -> io::Result<()> {
        self.open()?.log_index.write_entry(entry)
    }

    pub fn is_full(&self) -> bool {
        return self.position >= self.max_bytes.0
    }
    pub fn newest_offset(&self) -> u64 {self.next_offset}
    pub fn current_position(&self) -> u64 { self.position }
}


impl Write for SegmentMeta {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let open_segment = self.open()?;
        let mut buf_writer = BufWriter::new(&open_segment.log_writer);
        let n = buf_writer.write(buf)?;
        self.next_offset += 1;
        self.position += n as u64;

        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> { self.open()?.log_writer.flush() }
}

impl Read for SegmentMeta {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.open()?.log_reader.read(buf) }
}

impl Seek for SegmentMeta {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> { return self.open()?.log_reader.seek(offset) }
}

// Implement ordering for the segment in a commit log's segment list
impl Eq for SegmentMeta { }
impl PartialEq for SegmentMeta {
    fn eq(&self, other: &Self) -> bool { self.base_offset == other.base_offset }
}
impl Ord for SegmentMeta {
    fn cmp(&self, other: &Self) -> Ordering { self.base_offset.cmp(&other.base_offset) }  // .reverse()
}
impl PartialOrd for SegmentMeta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}


#[cfg(test)]
mod tests {
    use std::fs;
    use tempfile::tempdir;
    use super::*;

    #[test]
    fn it_creates_segment_meta() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().to_path_buf().clone();
        let segment = SegmentMeta::new(path, 0, MaxBytes(64, 64));
        assert!(!segment.index_path.exists(), "index file doens't exists");
        assert!(!segment.segment_path.exists(), "log file doesn't exists");
        assert_eq!(segment.position, 0, "position");
        assert_eq!(segment.base_offset, 0, "base_offset");
        assert_eq!(segment.max_bytes, MaxBytes(64, 64), "max_bytes");
        assert_eq!(segment.next_offset, 0, "next_offset");
    }


    #[test]
    fn it_can_write_to_log() {
        let tmp = tempdir().unwrap();
        let mut path = tmp.path().to_path_buf();
        let mut segment = SegmentMeta::new(path.clone(), 0, MaxBytes(64, 64));
        let n = segment.write("YELLOW SUBMARINE".as_bytes()).unwrap();
        let result = {
            let mut buf = [0; 16];
            path.push("00000000000000000000.log");
            let mut log_file = OpenOptions::new().create(false).read(true).open(path).unwrap();
            log_file.read_exact(&mut buf).unwrap();
            buf
        };
        assert_eq!(n, 16, "write returns 16");
        assert_eq!("YELLOW SUBMARINE".as_bytes(), result, "data writes");
    }

    #[test]
    fn it_returns_none_loading_wrong_path() {
        let tmp = tempdir().unwrap().path().to_path_buf().clone();
        let root_dir = SegmentMeta::load(tmp, MaxBytes(64, 64));

        assert!(root_dir.is_none(), "directory isn't a segment");
    }

    #[test]
    fn it_loads_empty_existing_segment_meta() {
        let mut tmp = tempdir().unwrap().path().to_path_buf().clone();
        fs::create_dir_all(&tmp).unwrap();
        tmp.push("00000000000000000000.log");

        let segment = SegmentMeta::load(tmp, MaxBytes(32, 16)).unwrap();
        assert_eq!(segment.position, 0, "position");
        assert_eq!(segment.base_offset, 0, "base_offset");
        assert_eq!(segment.max_bytes, MaxBytes(32, 16), "max_bytes");
        assert_eq!(segment.next_offset, 0, "next_offset is zero (uninitiated index)");
    }

    #[test]
    fn it_loads_existing_segment_meta() {
        let mut tmp = tempdir().unwrap().path().to_path_buf().clone();
        {
            let mut path = tmp.clone();
            fs::create_dir_all(&path).unwrap();
            path.push("00000000000000000000.index");
            let mut index = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000000.log");
            let mut log = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            let _ = log.write(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 88, 88,
                                0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 14, 88, 88]).unwrap();
            let _ = index.write(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 14]).unwrap();
        }


        tmp.push("00000000000000000000.log");
        let segment = SegmentMeta::load(tmp, MaxBytes(32, 16)).unwrap();
        assert_eq!(segment.position, 28, "position");
        assert_eq!(segment.base_offset, 0, "base_offset");
        assert_eq!(segment.max_bytes, MaxBytes(32, 16), "max_bytes");
        assert_eq!(segment.next_offset, 2, "next_offset");
    }
}
