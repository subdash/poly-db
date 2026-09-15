#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    // A policy that dictates whether or not a sync should be performed after
    // each write.
    #[default]
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EngineConfig {
    pub fsync: FsyncPolicy,
    /// Roll the active file once it passes this many bytes
    pub max_file_bytes: u64,
    /// Merge when dead bytes reach this fraction of total bytes
    pub dead_ratio: f64,
    /// Never merge a log smaller than this
    pub min_merge_bytes: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            fsync: FsyncPolicy::Always,
            max_file_bytes: 64 * 1024 * 1024, // 64 MiB
            dead_ratio: 0.5,
            min_merge_bytes: 1024 * 1024, // 1 MiB
        }
    }
}
