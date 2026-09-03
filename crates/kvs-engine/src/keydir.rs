#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Entry {
    pub(crate) file_id: u32,
    pub(crate) pos: u64,
    pub(crate) len: u32,
    pub(crate) timestamp: u64,
}

pub(crate) type KeyDir = std::collections::HashMap<String, Entry>;
