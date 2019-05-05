// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
use std::{io};
use std::io::{Read, SeekFrom, Seek};
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};

use crate::{Offset};
use crate::partition::Partition;
use crate::segment::{SegmentMeta, MaxBytes};


pub struct Reader {
    segments: Vec<SegmentMeta>, // sorted largest to smallest
    active_segment: SegmentMeta,
    max_bytes: MaxBytes,
    offset: Offset,
    relative_position: u64,
}

impl Reader {
    pub fn new(offset: Offset, path: PathBuf, max_bytes: MaxBytes) -> Option<Reader> {
        let mut segments = Partition::scan(path, max_bytes).ok()?;
        segments.reverse();  // largest -> smallest
        let mut cursor: Option<SegmentMeta> = None;
        loop {
            let segment = match segments.pop() {
                Some(seg) => seg,
                None => break,
            };
            if offset < segment.base_offset { break };
            cursor = Some(segment);
        }
        if let Some(mut active) = cursor {
            let entry = active.read_index_entry(offset).ok()?;
            let _ = active.seek(SeekFrom::Start(entry.position));
            return Some(
                Reader{
                    segments: segments,
                    active_segment: active,
                    max_bytes: max_bytes,
                    offset: offset,
                    relative_position: entry.position,
                }
            );
        }
        None
    }
}


impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_size: usize = 0;
        loop {
            match self.active_segment.read(buf) {
                Ok(n) => {
                    //println!("{:?} n read", n);
                    read_size += n;
                    self.relative_position += read_size as u64;
                },
                Err(_) => {
                    if let Some(seg) = self.segments.pop() {
                        self.active_segment = seg;
                        self.relative_position = 0;
                        continue;
                    }
                    break; // check for EOF
                },
            };
            break;
        }
        return Ok(read_size);
    }
}

impl Seek for Reader {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        return self.active_segment.seek(offset)
    }
}


#[cfg(test)]
mod tests {
    use std::fs;
    use tempfile::tempdir;
    use super::*;
    use crate::partition::Partition;

    fn write_partition(tmp: PathBuf, max_bytes: MaxBytes) -> bool {
        // BAH: refactor this :X
        let mut partition = Partition::create(String::from("topic"), &mut tmp.clone(), max_bytes).unwrap();
        let _ = partition.append("YELLOW SUBMARINE".as_bytes()).unwrap();
        let _ = partition.append("PURPLE PRESIDENT".as_bytes()).unwrap();
        let _ = partition.append("PRECIOUS PENNIES".as_bytes()).unwrap();
        true
    }

    #[test]
    fn no_new_reader_for_empty_partition() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        tmp.push("topic/");
        fs::create_dir_all(&tmp).unwrap();
        let reader = Reader::new(0, tmp, MaxBytes(64, 32));

        assert!(reader.is_none(), "there should be no reader");
    }


    #[test]
    fn new_reader() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        write_partition(tmp.clone(), MaxBytes(128, 64));
        tmp.push("topic/");

        let reader = Reader::new(0, tmp, MaxBytes(128, 64));
        assert!(reader.is_some(), "reader is some");
        let actual = reader.unwrap();
        assert_eq!(actual.relative_position, 0);
        assert_eq!(actual.offset, 0);
        assert_eq!(actual.max_bytes, MaxBytes(128, 64));
    }

    #[test]
    fn it_can_read_from_one_segment() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        write_partition(tmp.clone(), MaxBytes(128, 64));
        tmp.push("topic/");

        let mut reader = Reader::new(0, tmp, MaxBytes(128, 64)).unwrap();

        let mut buf = [0_u8; 28];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(buf, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         89, 69, 76, 76, 79, 87, 32, 83, 85, 66, 77, 65, 82, 73, 78, 69]);
        assert_eq!(n, 28)

    }

    #[test]
    fn it_can_read_from_a_second_segment_in_same_buffer() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        write_partition(tmp.clone(), MaxBytes(24, 32));
        tmp.push("topic/");
        let mut reader = Reader::new(0, tmp, MaxBytes(128, 64)).unwrap();

        let mut buf = [0_u8; 28];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(buf, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         89, 69, 76, 76, 79, 87, 32, 83, 85, 66, 77, 65, 82, 73, 78, 69]);
        assert_eq!(n, 28);
    }

    #[test]
    fn it_can_read_into_giant_buffer() {
        let mut tmp = tempdir().unwrap().path().to_path_buf();
        write_partition(tmp.clone(), MaxBytes(64, 32));
        tmp.push("topic/");
        let mut reader = Reader::new(0, tmp, MaxBytes(128, 64)).unwrap();

        let mut buf = [0_u8; 1024];
        let n = reader.read(&mut buf).unwrap();

        assert_eq!(buf[27], 69, "E");
        assert_eq!(buf.len(), 1024, "mostly empty buffer");
        assert_eq!(n, 84, "84 bytes read ((16  + HEADER(12)) * 3)");
    }
}
