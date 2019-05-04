#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_labels)]
#![allow(unused_variables)]
use std::{io, fs};
use std::fs::{OpenOptions, File};
use std::io::prelude::*;
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};
use std::iter::FromIterator;

use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use crate::Offset;
use crate::message::{Message};
use crate::entry::{Entry};
use crate::segment::{OpenSegment, SegmentMeta, MaxBytes};


pub struct Partition {
    // options
    path: PathBuf,
    max_bytes: MaxBytes,
    // attributes
    name: String,
    segments: Vec<SegmentMeta>,
    active_segment: SegmentMeta, // TODO: use arc to hold segments and mutexes
}



impl Partition {
    pub fn create(name: String, path: &mut PathBuf, max_bytes: MaxBytes) -> io::Result<Partition> {
        path.push(name.clone());
        fs::create_dir_all(path.clone())?;
        let active = SegmentMeta::new(path.clone(), 0, max_bytes);
        let segments: Vec<SegmentMeta> = Vec::new();
        Ok(
            Partition {
                name: name,
                path: path.to_path_buf(),
                max_bytes: max_bytes,
                segments: segments,
                active_segment: active,
            }
        )
    }

    pub fn load(path: &mut PathBuf, max_bytes: MaxBytes) -> io::Result<Partition> {
        let mut segments = Partition::scan(path.clone(), max_bytes)?;
        let latest_segment = match segments.pop() {
            Some(seg) => seg,
            None => SegmentMeta::new(path.clone(), 0, max_bytes),
        };

        let name = path.file_stem().unwrap();
        Ok(
            Partition {
                path: path.to_path_buf(),
                max_bytes: max_bytes,
                name: String::from(name.to_string_lossy()),
                segments: segments,
                active_segment: latest_segment,
            }
        )
    }

    pub fn scan(path: PathBuf, max_bytes: MaxBytes) -> io::Result<Vec<SegmentMeta>> {
        let mut segments: Vec<SegmentMeta> = Vec::new();
        for entry in fs::read_dir(path.clone())? {
            let log_path = entry?.path();
            let segment_meta = match SegmentMeta::load(log_path, max_bytes) {
                Some(meta) => meta,
                None => continue,
            };
            segments.push(segment_meta);
        }
        segments.sort_unstable();
        Ok(segments)
    }

    fn check_split(&mut self) -> bool {
        self.active_segment.is_full()
    }

    fn split(&mut self) -> io::Result<()> {
        let next_offset = self.active_segment.newest_offset();

        self.segments.push(self.active_segment.clone());
        self.active_segment = SegmentMeta::new(
            self.path.clone(),
            self.active_segment.newest_offset(),
            self.max_bytes,
        );
        Ok(())
    }

    pub fn append(&mut self, message: &[u8])-> io::Result<Offset> {
        if self.check_split() {
            self.split()?
        }

        let next_offset = self.active_segment.newest_offset();
        let position = self.active_segment.current_position();
        let message = Message::new(next_offset, position as u32, message);
        let payload = message.to_vec()?;
        let n = self.active_segment.write(&payload)?;

        let entry = Entry::new(next_offset, position);
        let _ = self.active_segment.write_index_entry(entry)?;

        Ok(self.active_segment.newest_offset())
    }
}


#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use super::*;

    #[test]
    fn it_creates_new_partition() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        let partition = Partition::create(String::from("topic"), &mut tmp, MaxBytes(64, 64)).unwrap();

        // TODO: add asserts
        assert_eq!(partition.active_segment.newest_offset(), 0, "Next offset");
        assert_eq!(partition.name, "topic", "The Partition name");
        assert_eq!(partition.segments.len(), 0, "no inactive segments");
        assert_eq!(partition.max_bytes, MaxBytes(64, 64), "no default segment bytes");
    }

    #[test]
    fn it_scans_partition_dir() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        tmp.push("topic");
        {
            let mut path = tmp.clone();
            fs::create_dir_all(&path).unwrap();
            path.push("00000000000000000000.index");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000000.log");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000088.index");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000088.log");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
        }
        let mut segments = Partition::scan(tmp, MaxBytes(64, 64)).unwrap();

        let top_seg = segments.pop();
        assert!(top_seg.is_some(), "is some");
        let segment = top_seg.unwrap();
        assert_eq!(segment.base_offset, 88, "base_offset of latest segment is 88");
        assert_eq!(segment.newest_offset(), 88, "next offset is 88 too (empty index/segment)");
    }

    #[test]
    fn it_loads_existing_empty_segments() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        tmp.push("topic/");
        {
            let mut path = tmp.clone();
            fs::create_dir_all(&path).unwrap();
            path.push("00000000000000000000.index");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000000.log");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000088.index");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000088.log");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
        }
        let partition = Partition::load(&mut tmp, MaxBytes(64, 64)).unwrap();

        assert_eq!(partition.active_segment.newest_offset(), 88, "next offset is 88");
        assert_eq!(partition.segments.len(), 1, "One 'docketed' existing segment meta");
    }

    #[test]
    fn it_loads_existing_segments() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        tmp.push("topic/");
        {
            let mut path = tmp.clone();
            fs::create_dir_all(&path).unwrap();
            path.push("00000000000000000000.index");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000000.log");
            let _ = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            path.pop();
            path.push("00000000000000000088.log");
            let mut seg = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            let _ = seg.write(&[0, 0, 0, 0, 0, 0, 0, 88, 0, 0, 0, 0, 88, 88,
                                0, 0, 0, 0, 0, 0, 0, 89, 0, 0, 0, 14, 88, 88]).unwrap();
            path.pop();
            path.push("00000000000000000088.index");
            let mut idx = OpenOptions::new().create(true).write(true).open(&path).unwrap();
            let _ = idx.write(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 14]).unwrap();
        }
        let partition = Partition::load(&mut tmp, MaxBytes(64, 64)).unwrap();

        assert_eq!(partition.active_segment.newest_offset(), 90, "next offset is 90!");
        assert_eq!(partition.segments.len(), 1, "One 'docketed' existing segment meta");
    }

    #[test]
    fn it_appends_to_partition() {
        let tmp = tempdir().unwrap().path().to_path_buf();
        let mut partition = Partition::create(String::from("topic"), &mut tmp.clone(), MaxBytes(64, 32)).unwrap();
        let first_offset = partition.append("YELLOW SUBMARINE".as_bytes()).unwrap();
        let second_offset = partition.append("NIGHTMARE STEAM".as_bytes()).unwrap();
        let segment = {
            let mut path = partition.path.clone();
            path.push("00000000000000000000.log");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let index = {
            let mut path = partition.path.clone();
            path.push("00000000000000000000.index");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let expected_segment = "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}YELLOW SUBMARINE\
                                \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{1}\u{0}\u{0}\u{0}\u{1c}NIGHTMARE STEAM";
        let expected_index = "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{1}\u{0}\u{0}\u{0}\u{1c}\
                              \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}";
        assert_eq!(first_offset, 1, "next offset is 1!");
        assert_eq!(second_offset, 2, "second offset is 2!");
        assert_eq!(segment.as_bytes(), expected_segment.as_bytes(), "segment write");
        assert_eq!(index.as_bytes(), expected_index.as_bytes(), "index write");
    }

    #[test]
    fn it_splits_when_full() {
        let tmp = tempdir().unwrap().path().to_path_buf();
        let mut partition = Partition::create(String::from("topic"), &mut tmp.clone(), MaxBytes(28, 16)).unwrap();
        assert_eq!(partition.active_segment.size(), 0);
        let first_offset = partition.append("YELLOW SUBMARINE".as_bytes()).unwrap();
        assert_eq!(partition.active_segment.size(), 28);
        let second_offset = partition.append("XX".as_bytes()).unwrap();
        assert_eq!(partition.active_segment.size(), 14);
        let third_offset = partition.append("XX".as_bytes()).unwrap();
        assert_eq!(partition.active_segment.size(), 28);
        let first_segment = {
            let mut path = partition.path.clone();
            path.push("00000000000000000000.log");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let first_index = {
            let mut path = partition.path.clone();
            path.push("00000000000000000000.index");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let expected_first_segment = "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}YELLOW SUBMARINE";
        let expected_first_index = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let second_segment = {
            let mut path = partition.path.clone();
            path.push("00000000000000000001.log");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let second_index = {
            let mut path = partition.path.clone();
            path.push("00000000000000000001.index");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let expected_second_segment = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 88, 88,
                                       0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 14, 88, 88];
        let expected_second_index = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 14];
        assert_eq!(first_offset, 1, "next offset is 1!");
        assert_eq!(second_offset, 2, "second (next) offset is 2!");
        assert_eq!(third_offset, 3, "third (next) offset is 3!");
        assert_eq!(partition.active_segment.newest_offset(), 3);
        assert_eq!(partition.active_segment.size(), 28);
        assert_eq!(first_segment.as_bytes(), expected_first_segment.as_bytes(), "first segment write");
        assert_eq!(first_index.as_bytes(), expected_first_index, "first index write");
        assert_eq!(second_segment.as_bytes(), expected_second_segment, "second segment write");
        assert_eq!(second_index.as_bytes(), expected_second_index, "second index write");
    }
}
