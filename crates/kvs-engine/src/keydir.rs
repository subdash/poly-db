use crate::record::HEADER_LEN;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Entry {
    pub(crate) file_id: u32,
    pub(crate) pos: u64,
    pub(crate) len: u32,
}

impl Entry {
    // With self.len being the payload length, by adding HEADER_LEN we get the
    // framed length of the record the Entry points at.
    pub(crate) fn framed_len(&self) -> u64 {
        HEADER_LEN as u64 + u64::from(self.len)
    }
}

pub(crate) type KeyDir = std::collections::HashMap<String, Entry>;
