#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_labels)]
#![allow(unused_variables)]
use std::{io, fs, thread, env};
use std::fs::{OpenOptions, File};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::iter::FromIterator;
use std::collections::BinaryHeap;

use byteorder::{ByteOrder, BigEndian, WriteBytesExt, ReadBytesExt};

use crate::Offset;
use crate::index::{Entry};
use crate::segment::{Segment, InactiveSegment};

const OFFSET: u64 = 0;
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


pub struct CommitLog {
    // options
    path: PathBuf,
    segment_bytes: u64,
    // attributes
    name: String,
    segments: BinaryHeap<InactiveSegment>,
    active_segment: Segment, // TODO: use arc to hold segments and mutexes
}



impl CommitLog {
    pub fn new(name: String, path: &mut PathBuf, segment_bytes: u64) -> io::Result<CommitLog> {
        // TODO: refactor and split up into load and new
        let mut segments: BinaryHeap<InactiveSegment> = BinaryHeap::new();
        path.push(name.clone());
        fs::create_dir_all(path.clone())?;
        for entry in fs::read_dir(path.clone())? {
            let path_buf = entry?.path();
            let file_path = path_buf.as_path();
            let ext = file_path.extension().unwrap().to_string_lossy();
            if !ext.contains("log") { continue }
            let stem = file_path.file_stem().unwrap().to_string_lossy();
            let offset = match stem.parse::<Offset>() {
                Ok(off) => off,
                _ => { continue },  // TODO: log errors
            };

            let mut partition_path = file_path.to_path_buf();
            partition_path.pop();
            let seg = InactiveSegment::new(partition_path, offset, 0);
            segments.push(seg);

        }
        let latest_segment = segments.pop();

        let active = match latest_segment {
            Some(inactive_segment) => { inactive_segment.activate()? },
            None => {
                let mut partition_path = path.to_path_buf().clone();
                Segment::new(&mut partition_path, 0, 0)?
            },
        };

        Ok(
            CommitLog {
                path: path.to_path_buf(),
                segment_bytes: segment_bytes,
                name: name,
                segments: segments,
                active_segment: active,
            }
        )
    }

    fn check_split(&mut self) -> bool {
        self.active_segment.is_full()
    }

    fn split(&mut self) -> io::Result<()> {
        self.segments.push(self.active_segment.deactivate());
        self.active_segment = Segment::new(
            &mut self.path,
            self.active_segment.newest_offset(),
            self.segment_bytes,
        )?;
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
        let tmp = tempdir().unwrap();
        let commitlog = CommitLog::new(String::from("topic"), &mut tmp.path().to_path_buf().clone(), 0).unwrap();

        // TODO: add asserts
        assert_eq!(commitlog.active_segment.newest_offset(), 0, "Next offset");
        assert_eq!(commitlog.name, "topic", "The CommitLog name");
        assert_eq!(commitlog.segments.len(), 0, "no inactive segments");
        assert_eq!(commitlog.segment_bytes, 0, "no default segment bytes");
    }

    #[test]
    fn it_loads_existing_segments() {
        let tmp = tempdir().unwrap();
        {
            let mut path = tmp.path().to_path_buf().clone();
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

        let commitlog = CommitLog::new(String::from("topic/"), &mut tmp.path().to_path_buf().clone(), 0).unwrap();
        assert_eq!(commitlog.active_segment.newest_offset(), 88, "Active Segment is 0");
        assert_eq!(commitlog.segments.len(), 1, "One 'docketed' existing Segment");
    }

    #[test]
    fn it_write_to_commit_log() {
        let tmp = tempdir().unwrap().path().to_path_buf();
        let mut commitlog = CommitLog::new(String::from("topic/"), &mut tmp.clone(), 0).unwrap();
        let first_offset = commitlog.append("YELLOW SUBMARINE".as_bytes()).unwrap();
        let second_offset = commitlog.append("NIGHTMARE STEAM".as_bytes()).unwrap();
        let written = {
            let mut path = commitlog.path.clone();
            path.push("00000000000000000000.log");
            let mut file = OpenOptions::new().create(false).read(true).open(&path).unwrap();
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            buf
        };
        let expected = "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}YELLOW SUBMARINE\
                        \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{1}\u{0}\u{0}\u{0}\u{1c}NIGHTMARE STEAM";
        assert_eq!(first_offset, 1, "next offset is 1!");
        assert_eq!(second_offset, 2, "second offset is 1!");
        assert_eq!(written.as_bytes(), expected.as_bytes(), "match data and header");
    }

}
