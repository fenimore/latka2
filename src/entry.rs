use crate::{Offset};

pub const ENTRY_WIDTH: u32 = 8;


#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Entry{
    pub offset: Offset,
    pub position: Offset,
}
impl Entry {
    pub fn new(offset: Offset, position: u64) -> Entry {
        Entry{ offset: offset, position: position }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RelativeEntry{
    pub offset: u32,
    pub position: u32,
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


// TODO: write tests
