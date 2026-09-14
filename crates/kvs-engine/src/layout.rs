use crate::Result;
use std::path::{Path, PathBuf};

pub(crate) const LOG_EXT: &str = "log";

pub(crate) fn log_path(dir: &Path, id: u32) -> PathBuf {
    dir.to_path_buf().join(format!("{}.{}", id, LOG_EXT))
}

pub(crate) fn log_ids(dir: &Path) -> Result<Vec<u32>> {
    let mut ids: Vec<u32> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            path.is_file() && path.extension().is_some_and(|ext| ext == LOG_EXT)
        })
        .filter_map(|entry| entry.path().file_stem()?.to_str()?.parse::<u32>().ok())
        .collect();

    ids.sort_unstable();

    Ok(ids)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::{Command, record};
    use std::io::Write;

    /// Write `cmds` to `<dir>/<id>.log` as framed records. Returns the file length.
    pub(crate) fn write_log(dir: &Path, id: u32, cmds: &[Command]) -> u64 {
        let path = log_path(dir, id);
        let mut file = std::fs::File::create(&path).expect("create log");
        for cmd in cmds {
            let bytes = record::encode(cmd).expect("encode");
            file.write_all(&bytes).expect("write record");
        }
        file.flush().expect("flush");
        file.metadata().expect("metadata").len()
    }

    /// The offset and payload length of record `index` in a log built from `cmds`.
    pub(crate) fn locate(cmds: &[Command], index: usize) -> (u64, u32) {
        let mut offset = 0u64;
        for cmd in &cmds[..index] {
            offset += record::encode(cmd).expect("encode").len() as u64;
        }
        let len = record::encode(&cmds[index]).expect("encode").len() - record::HEADER_LEN;
        (offset, len as u32)
    }

    pub(crate) fn set(key: &str, value: &str) -> Command {
        Command::Set {
            key: key.into(),
            value: value.into(),
        }
    }

    pub(crate) fn remove(key: &str) -> Command {
        Command::Remove { key: key.into() }
    }
}

#[cfg(test)]
mod tests {

    const HINT_EXT: &str = "hint";
    fn hint_path(dir: &Path, id: u32) -> PathBuf {
        dir.to_path_buf().join(format!("{}.{}", id, HINT_EXT))
    }

    fn tmp_log_path(dir: &Path, id: u32) -> PathBuf {
        dir.to_path_buf().join(format!("{}.{}.tmp", id, LOG_EXT))
    }

    fn tmp_hint_path(dir: &Path, id: u32) -> PathBuf {
        dir.to_path_buf().join(format!("{}.{}.tmp", id, HINT_EXT))
    }

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn paths_are_named_by_id_and_extension() {
        let dir = Path::new("/data");
        assert_eq!(log_path(dir, 0), Path::new("/data/0.log"));
        assert_eq!(log_path(dir, 42), Path::new("/data/42.log"));
        assert_eq!(hint_path(dir, 42), Path::new("/data/42.hint"));
        assert_eq!(tmp_log_path(dir, 42), Path::new("/data/42.log.tmp"));
        assert_eq!(tmp_hint_path(dir, 42), Path::new("/data/42.hint.tmp"));
    }

    #[test]
    fn log_ids_of_an_empty_directory_is_empty() {
        let dir = TempDir::new().expect("tempdir");
        assert!(log_ids(dir.path()).expect("log_ids").is_empty());
    }

    #[test]
    fn log_ids_are_returned_in_ascending_numeric_order() {
        let dir = TempDir::new().expect("tempdir");
        for id in [10u32, 2, 0, 9] {
            std::fs::write(log_path(dir.path(), id), b"").expect("write");
        }
        assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0, 2, 9, 10]);
    }

    #[test]
    fn log_ids_ignores_everything_that_is_not_a_numbered_log() {
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(log_path(dir.path(), 3), b"").expect("write");
        for name in [
            "notes.txt",
            "foo.log",
            "3.hint",
            "4.log.tmp",
            "4.
  hint.tmp",
            "-1.log",
        ] {
            std::fs::write(dir.path().join(name), b"").expect("write");
        }
        assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![3]);
    }
}
