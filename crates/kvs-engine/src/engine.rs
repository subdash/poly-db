use crate::{
    Command, EngineError, Result,
    keydir::{Entry, KeyDir},
    record::{self, HEADER_LEN},
};
use std::os::unix::fs::FileExt;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Engine {
    appender: BufWriter<File>,
    write_offset: u64,
    read_handle: Arc<File>,
    key_dir: KeyDir,
    #[allow(dead_code)]
    data_directory_path: PathBuf,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> Result<Engine> {
        // Create dir if it does not exist
        let dir = path.as_ref();
        std::fs::create_dir_all(dir)?;

        // Create file if it doesn't exist, grab append/read file descriptors,
        // offset and initialize engine.
        let log_path = dir.join("0.log");
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)?;
        let read_handle = Arc::new(File::open(&log_path)?);
        let offset = read_handle.metadata()?.len();
        let writer = BufWriter::new(file);

        Ok(Engine {
            data_directory_path: dir.to_path_buf(),
            appender: writer,
            write_offset: offset,
            read_handle,
            key_dir: HashMap::new(),
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Encode
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let record = record::encode(&cmd)?;
        let record_len = record.len();

        // Calculate current write offset and payload length
        let pos = self.write_offset;
        let len = (record_len - HEADER_LEN) as u32;

        // Append the bytes, flush (buffer -> OS) and fsync (OS -> disk), update offset
        self.appender.write_all(&record)?;
        self.appender.flush()?;
        self.appender.get_ref().sync_data()?;
        self.write_offset += record_len as u64;

        // Write to key dir
        let entry = Entry {
            pos,
            file_id: 0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_default(),
            len,
        };
        self.key_dir.insert(key, entry);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<String> {
        // Look up entry in key dir
        let entry = self.key_dir.get(key).ok_or(EngineError::KeyNotFound)?;
        // Read contents into buffer
        let mut record = vec![0u8; HEADER_LEN + entry.len as usize];
        self.read_handle.read_exact_at(&mut record, entry.pos)?;

        // Decode and return
        match record::decode(&record, entry.pos)? {
            Command::Set { value, .. } => Ok(value),
            Command::Remove { .. } => Err(EngineError::Corrupt { offset: entry.pos }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineError;
    use tempfile::TempDir;

    fn open_temp() -> (TempDir, Engine) {
        let dir = TempDir::new().expect("tempdir");
        let engine = Engine::open(dir.path()).expect("open");
        (dir, engine)
    }

    #[test]
    fn get_returns_a_value_that_was_set() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("set");
        assert_eq!(engine.get("alpha").expect("get"), "one");
    }

    #[test]
    fn set_overwrites_an_existing_key() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("first set");
        engine
            .set("alpha".into(), "two".into())
            .expect("second set");
        assert_eq!(engine.get("alpha").expect("get"), "two");
    }

    #[test]
    fn keys_are_independent() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("set alpha");
        engine.set("beta".into(), "two".into()).expect("set beta");
        assert_eq!(engine.get("alpha").expect("get"), "one");
        assert_eq!(engine.get("beta").expect("get"), "two");
    }

    #[test]
    fn get_on_an_unknown_key_reports_key_not_found() {
        let (_dir, engine) = open_temp();
        let err = engine
            .get("ghost")
            .expect_err("an unset key must not resolve");
        assert!(matches!(err, EngineError::KeyNotFound));
    }

    #[test]
    fn empty_values_round_trip() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), String::new()).expect("set");
        assert_eq!(engine.get("alpha").expect("get"), "");
    }

    #[test]
    fn open_creates_a_missing_data_directory() {
        let dir = TempDir::new().expect("tempdir");
        let nested = dir.path().join("not-yet");
        let _engine = Engine::open(&nested).expect("open must create the directory");
        assert!(
            nested.join("0.log").exists(),
            "the log file must exist after open"
        );
    }

    #[test]
    fn every_write_appends_to_the_log() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let log = dir.path().join("0.log");

        engine.set("alpha".into(), "one".into()).expect("set");
        let after_first = std::fs::metadata(&log).expect("metadata").len();
        assert!(after_first > 0, "the first write must reach the file");

        engine.set("alpha".into(), "two".into()).expect("overwrite");
        let after_second = std::fs::metadata(&log).expect("metadata").len();
        assert!(
            after_second > after_first,
            "an overwrite appends a new record rather than editing in place"
        );
    }
}
