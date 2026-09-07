use crate::{
    Command, EngineError, Reader, Result,
    keydir::{Entry, KeyDir},
    record::{self, HEADER_LEN, MAX_KEY_BYTES, MAX_PAYLOAD_BYTES, MAX_VALUE_BYTES},
};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

pub struct Engine {
    write_offset: u64,
    writer: BufWriter<File>,
    reader: Reader,
    policy: FsyncPolicy,
    #[allow(dead_code)]
    data_directory_path: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    #[default]
    Always,
    Never,
}

impl Engine {
    pub fn open_with(path: impl AsRef<Path>, policy: FsyncPolicy) -> Result<Engine> {
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

        // Replay takes an `impl Read`, which Arc<File> does not implement, so we must dereference
        // the pointer and pass a reference to the file handle it points to.
        let (key_dir, write_offset) = Engine::replay(&*read_handle)?;

        // In the case of a corrupted log, the replay loop will exit early and the log will be
        // truncated to the end of the last uncorrupted entry.
        let unprocessed_bytes = file.metadata()?.len() - write_offset;
        if unprocessed_bytes > 0 {
            file.set_len(write_offset)?;
            eprintln!(
                "{unprocessed_bytes} bytes after byte {write_offset} were corrupted and truncated from the log."
            );
        }

        let reader = Reader {
            key_dir,
            read_handle,
        };
        let writer = BufWriter::new(file);

        let engine = Engine {
            data_directory_path: dir.to_path_buf(),
            writer,
            write_offset,
            policy,
            reader,
        };

        Ok(engine)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Engine> {
        Engine::open_with(path, FsyncPolicy::default())
    }

    pub fn reader(&self) -> Reader {
        self.reader.clone()
    }

    fn replay(reader: impl Read) -> Result<(Arc<RwLock<KeyDir>>, u64)> {
        let mut key_dir = HashMap::new();
        let mut reader = BufReader::new(reader);
        let mut offset = 0;

        loop {
            // Read header
            let mut header = [0u8; HEADER_LEN];

            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Read payload
            let payload_len = record::payload_len(&header);

            if payload_len as usize > MAX_PAYLOAD_BYTES {
                // Don't allocate more than the max allowed
                break;
            }

            let mut full_record = vec![0u8; HEADER_LEN + payload_len as usize];
            full_record[0..HEADER_LEN].copy_from_slice(&header);

            match reader.read_exact(&mut full_record[HEADER_LEN..]) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Decode
            let Ok(cmd) = record::decode(&full_record, offset) else {
                break;
            };

            match cmd {
                Command::Set { key, .. } => {
                    let entry = Entry {
                        pos: offset,
                        file_id: 0,
                        len: payload_len,
                    };
                    key_dir.insert(key, entry);
                }

                Command::Remove { key } => {
                    key_dir.remove(&key);
                }
            };

            offset += HEADER_LEN as u64 + payload_len as u64;
        }

        let key_dir = Arc::new(RwLock::new(key_dir));
        Ok((key_dir, offset))
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if key.len() > MAX_KEY_BYTES {
            return Err(EngineError::KeyTooLarge { len: key.len() });
        }

        if value.len() > MAX_VALUE_BYTES {
            return Err(EngineError::ValueTooLarge { len: value.len() });
        }

        // Encode
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };

        // Append to log
        let (pos, len) = self.append(&cmd)?;

        // Write to key dir
        let entry = Entry {
            pos,
            file_id: 0,
            len,
        };

        self.reader
            .key_dir
            .write()
            .expect("keydir lock poisoned")
            .insert(key, entry);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<String> {
        self.reader.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        if key.len() > MAX_KEY_BYTES {
            return Err(EngineError::KeyTooLarge { len: key.len() });
        }

        let key_found = self
            .reader
            .key_dir
            .read() // Obtain read-lock which dies after let binding
            .expect("keydir lock poisoned")
            .contains_key(key);

        // Check for membership in key dir
        if !key_found {
            return Err(EngineError::KeyNotFound);
        }

        let cmd = Command::Remove {
            key: String::from(key),
        };

        // Append command to log
        self.append(&cmd)?;
        self.reader
            .key_dir
            .write() // Obtain write lock to remove key from memory
            .expect("keydir lock poisoned")
            .remove(key);

        Ok(())
    }

    fn append(&mut self, cmd: &Command) -> Result<(u64, u32)> {
        // Capture log position prior to appending
        let pos = self.write_offset;

        let record = record::encode(cmd)?;
        let record_len = record.len();
        let payload_len = (record_len - HEADER_LEN) as u32;

        // Append the bytes, flush (buffer -> OS) and fsync (OS -> disk)
        // when policy instructs us to, update offset
        self.writer.write_all(&record)?;
        self.writer.flush()?;
        match self.policy {
            FsyncPolicy::Always => {
                self.writer.get_ref().sync_data()?;
            }
            FsyncPolicy::Never => {}
        }
        self.write_offset += record_len as u64;

        Ok((pos, payload_len))
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

    //
    // get/set
    //
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

    //
    // remove
    //
    #[test]
    fn remove_makes_a_key_unreadable() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.remove("alpha").expect("remove");
        let err = engine
            .get("alpha")
            .expect_err("a removed key must not resolve");
        assert!(matches!(err, EngineError::KeyNotFound));
    }

    #[test]
    fn remove_on_an_unknown_key_reports_key_not_found() {
        let (_dir, mut engine) = open_temp();
        let err = engine
            .remove("ghost")
            .expect_err("removing nothing must fail");
        assert!(matches!(err, EngineError::KeyNotFound));
    }

    #[test]
    fn a_rejected_remove_writes_nothing_to_the_log() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let log = dir.path().join("0.log");

        engine.set("alpha".into(), "one".into()).expect("set");
        let before = std::fs::metadata(&log).expect("metadata").len();
        engine
            .remove("ghost")
            .expect_err("removing nothing must fail");
        let after = std::fs::metadata(&log).expect("metadata").len();

        assert_eq!(
            before, after,
            "a rejected remove must not append a tombstone"
        );
    }

    #[test]
    fn a_key_can_be_set_again_after_removal() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.remove("alpha").expect("remove");
        engine.set("alpha".into(), "two".into()).expect("set again");
        assert_eq!(engine.get("alpha").expect("get"), "two");
    }

    #[test]
    fn remove_leaves_other_keys_alone() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("set alpha");
        engine.set("beta".into(), "two".into()).expect("set beta");
        engine.remove("alpha").expect("remove alpha");
        assert_eq!(engine.get("beta").expect("get"), "two");
    }

    #[test]
    fn an_accepted_remove_appends_a_tombstone() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let log = dir.path().join("0.log");

        engine.set("alpha".into(), "one".into()).expect("set");

        let before = std::fs::metadata(&log).expect("metadata").len();
        engine.remove("alpha").expect("remove");
        let after = std::fs::metadata(&log).expect("metadata").len();
        assert!(
            after > before,
            "an accepted remove must append a tombstone to the log"
        );
    }

    #[test]
    fn remove_rejects_an_oversized_key() {
        let (_dir, mut engine) = open_temp();
        let key = "k".repeat(MAX_KEY_BYTES + 1);
        let err = engine
            .remove(&key)
            .expect_err("an oversized key must be rejected");
        assert!(matches!(err, EngineError::KeyTooLarge { .. }));
    }

    #[test]
    fn set_rejects_an_oversized_key() {
        let (_dir, mut engine) = open_temp();
        let key = "k".repeat(MAX_KEY_BYTES + 1);
        let err = engine
            .set(key, "one".into())
            .expect_err("an oversized key must be rejected");
        assert!(matches!(err, EngineError::KeyTooLarge { .. }));
    }

    #[test]
    fn set_rejects_an_oversized_value() {
        let (_dir, mut engine) = open_temp();
        let value = "v".repeat(MAX_VALUE_BYTES + 1);
        let err = engine
            .set("alpha".into(), value)
            .expect_err("an oversized value must be rejected");
        assert!(matches!(err, EngineError::ValueTooLarge { .. }));
    }

    #[test]
    fn a_key_exactly_at_the_limit_is_accepted() {
        let (_dir, mut engine) = open_temp();
        let key = "k".repeat(MAX_KEY_BYTES);
        engine
            .set(key.clone(), "one".into())
            .expect("a key at the limit is legal");
        assert_eq!(engine.get(&key).expect("get"), "one");
    }

    #[test]
    fn limits_are_measured_in_bytes_not_characters() {
        let (_dir, mut engine) = open_temp();
        // 'é' is two bytes in UTF-8, so this is over the limit despite being
        // MAX_KEY_BYTES characters long.
        let key = "é".repeat(MAX_KEY_BYTES);
        let err = engine
            .set(key, "one".into())
            .expect_err("byte length is what counts");
        assert!(matches!(err, EngineError::KeyTooLarge { .. }));
    }

    #[test]
    fn a_rejected_write_leaves_the_log_untouched() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let log = dir.path().join("0.log");

        engine.set("alpha".into(), "one".into()).expect("set");
        let before = std::fs::metadata(&log).expect("metadata").len();

        engine
            .set("k".repeat(MAX_KEY_BYTES + 1), "one".into())
            .expect_err("must be rejected");
        let after = std::fs::metadata(&log).expect("metadata").len();

        assert_eq!(before, after, "validation must happen before the append");
    }

    #[test]
    fn the_never_policy_still_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        {
            let mut engine = Engine::open_with(dir.path(), FsyncPolicy::Never).expect("open");
            engine.set("alpha".into(), "one".into()).expect("set");
        }
        let engine = Engine::open(dir.path()).expect("reopen");
        assert_eq!(engine.get("alpha").expect("get"), "one");
    }

    #[test]
    fn the_default_policy_is_always() {
        assert_eq!(FsyncPolicy::default(), FsyncPolicy::Always);
    }
}
