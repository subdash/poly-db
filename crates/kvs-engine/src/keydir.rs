#[derive(Debug, Clone, Copy)]
pub(crate) struct Entry {
    #[allow(dead_code)]
    pub(crate) file_id: u32,
    pub(crate) pos: u64,
    pub(crate) len: u32,
}

pub(crate) type KeyDir = std::collections::HashMap<String, Entry>;
