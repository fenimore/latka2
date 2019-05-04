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
use std::collections::BinaryHeap;

use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use crate::Offset;
use crate::index::{Entry};
use crate::segment::{OpenSegment, SegmentMeta, MaxBytes};

const SIZE: u64 = 8;
const MSG_HEADER_LEN: u64 = 12;

pub struct Message {
    offset: Offset,
    position: u32,
    payload: Vec<u8>,
}

impl Message {
    pub fn new(offset: Offset, position: u32, payload: &[u8]) -> Message {
        Message{
            payload: payload.to_vec(),
            offset: offset,
            position: position,
        }
    }

    pub fn size(&self) -> usize {
        self.payload.len() + self.offset as usize + self.position as usize
    }

    pub fn from_vec(raw: &mut Vec<u8>) -> Message {
        let off = BigEndian::read_u64(&raw[0..8]);
        let pos = BigEndian::read_u32(&raw[8..12]);
        raw.drain(0..12);
        Message {
            offset: off,
            position: pos,
            payload: raw.to_vec(),
        }
    }

    pub fn to_vec(&self) -> io::Result<Vec<u8>> {
        let mut buf = vec![];

        buf.write_u64::<BigEndian>(self.offset)?;
        buf.write_u32::<BigEndian>(self.position)?;
        if buf.len() != 12 {
            return Err(io::Error::new(io::ErrorKind::Other, "Header wrong size"))
        }
        buf.append(&mut self.payload.to_vec());

        Ok(buf)
    }
}

pub struct Partition {
    // options
    path: PathBuf,
    max_bytes: MaxBytes,
    // attributes
    name: String,
    segments: BinaryHeap<SegmentMeta>,
    active_segment: SegmentMeta, // TODO: use arc to hold segments and mutexes
}



impl Partition {
    pub fn create(name: String, path: &mut PathBuf, max_bytes: MaxBytes) -> io::Result<Partition> {
        path.push(name.clone());
        fs::create_dir_all(path.clone())?;
        let active = SegmentMeta::new(path.clone(), 0, max_bytes);
        let segments: BinaryHeap<SegmentMeta> = BinaryHeap::new();
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

    pub fn scan(path: PathBuf, max_bytes: MaxBytes) -> io::Result<BinaryHeap<SegmentMeta>> {
        let mut segments: BinaryHeap<SegmentMeta> = BinaryHeap::new();
        for entry in fs::read_dir(path.clone())? {
            let log_path = entry?.path();
            let segment_meta = match SegmentMeta::load(log_path, max_bytes) {
                Some(meta) => meta,
                None => continue,
            };
            segments.push(segment_meta);
        }
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
    fn new_message() {
        let message = Message::new(1, 3, &[0, 1, 2, 3]);
        assert_eq!(message.offset, 1);
        assert_eq!(message.position, 3);
        assert_eq!(message.payload, vec![0, 1, 2, 3]);
    }

    #[test]
    fn message_from_vec() {
        let mut raw = vec![0u8, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 1, 2, 3];
        let message = Message::from_vec(&mut raw);
        assert_eq!(message.offset, 1);
        assert_eq!(message.position, 3);
        assert_eq!(message.payload, vec![0, 1, 2, 3]);
    }

    #[test]
    fn message_to_vec() {
        let message = Message::new(1, 3, &[0, 1, 2, 3]);
        let res = message.to_vec().unwrap();

        assert_eq!(res, vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn it_creates_new_commit_log() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        let commitlog = Partition::create(String::from("topic"), &mut tmp, MaxBytes(64, 64)).unwrap();

        // TODO: add asserts
        assert_eq!(commitlog.active_segment.newest_offset(), 0, "Next offset");
        assert_eq!(commitlog.name, "topic", "The Partition name");
        assert_eq!(commitlog.segments.len(), 0, "no inactive segments");
        assert_eq!(commitlog.max_bytes, MaxBytes(64, 64), "no default segment bytes");
    }

    #[test]
    fn it_loads_existing_segments() {
        // TODO: at the moment it load using a hcaky hacky. And it's no good. Sooo
        // I should create a load function
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        {
            let mut path = tmp.clone();
            path.push("topic/");
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
        let commitlog = Partition::create(String::from("topic"), &mut tmp, MaxBytes(64, 64)).unwrap();

        assert_eq!(commitlog.active_segment.newest_offset(), 88, "Active Segment is 0");
        assert_eq!(commitlog.segments.len(), 1, "One 'docketed' existing Segment");
    }

    #[test]
    fn it_appends_to_commit_log() {
        let tmp = tempdir().unwrap().path().to_path_buf();
        let mut commitlog = Partition::create(String::from("topic"), &mut tmp.clone(), MaxBytes(64, 32)).unwrap();
        let first_offset = commitlog.append("YELLOW SUBMARINE".as_bytes()).unwrap();
        let second_offset = commitlog.append("NIGHTMARE STEAM".as_bytes()).unwrap();
        let segment = {
            let mut path = commitlog.path.clone();
            path.push("00000000000000000000.log");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let index = {
            let mut path = commitlog.path.clone();
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
}
