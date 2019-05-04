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
use crate::segment::MaxBytes;

const SIZE: u64 = 8;
const MSG_HEADER_LEN: u64 = 12;

pub struct Message {
    pub offset: Offset,
    pub position: u32,
    pub payload: Vec<u8>,
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
}
