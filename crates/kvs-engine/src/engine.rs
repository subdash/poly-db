use crate::{
    Command, EngineConfig, EngineError, Reader, Result,
    config::FsyncPolicy,
    keydir::Entry,
    layout::log_path,
    record::{self, HEADER_LEN, MAX_KEY_BYTES, MAX_VALUE_BYTES},
    replay,
    stats::Stats,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

pub struct Engine {
    write_offset: u64,
    writer: BufWriter<File>,
    reader: Reader,
    config: EngineConfig,
    data_directory_path: PathBuf,
    active_id: u32,
    stats: Arc<Stats>,
}

impl Engine {
    pub fn open_with(path: impl AsRef<Path>, config: EngineConfig) -> Result<Engine> {
        // Create dir if it does not exist
        let dir = path.as_ref();
        std::fs::create_dir_all(dir)?;

        let mut replayed = replay::build(dir)?;

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(log_path(dir, replayed.active_id))?;

        // In the case of a corrupted log, the replay loop will exit early and the log will be
        // truncated to the end of the last uncorrupted entry.
        let unprocessed_bytes = file.metadata()?.len().saturating_sub(replayed.write_offset);
        if unprocessed_bytes > 0 {
            file.set_len(replayed.write_offset)?;
            tracing::warn!(
                bytes = unprocessed_bytes,
                offset = replayed.write_offset,
                "some bytes were corrupted and truncated from the log"
            );
        }

        let active_file_read_handle = Arc::new(File::open(log_path(dir, replayed.active_id))?);
        replayed
            .store
            .files
            .insert(replayed.active_id, active_file_read_handle);

        let reader = Reader {
            store: Arc::new(RwLock::new(replayed.store)),
        };
        let writer = BufWriter::new(file);
        let engine = Engine {
            data_directory_path: dir.to_path_buf(),
            writer,
            write_offset: replayed.write_offset,
            config,
            reader,
            active_id: replayed.active_id,
            stats: Arc::new(Stats::new(replayed.total_bytes)),
        };

        Ok(engine)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Engine> {
        Engine::open_with(path, EngineConfig::default())
    }

    pub fn reader(&self) -> Reader {
        self.reader.clone()
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
        let entry = self.append(&cmd)?;

        // Write to key dir
        let displaced = self
            .reader
            .store
            .write()
            .expect("store lock poisoned")
            .key_dir
            .insert(key, entry);

        // `insert` above will return the old displaced entry if the new entry overwrites it.
        // In that case we have dead bytes in the log (of the old entry), so we should record that.
        if let Some(old) = displaced {
            self.stats.record_dead(old.framed_len());
        }

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
            .store
            .read() // Obtain read-lock which dies after let binding
            .expect("store lock poisoned")
            .key_dir
            .contains_key(key);

        // Check for membership in key dir
        if !key_found {
            return Err(EngineError::KeyNotFound);
        }

        let cmd = Command::Remove {
            key: String::from(key),
        };

        // Append command to log
        let tombstone = self.append(&cmd)?;
        let removed = self
            .reader
            .store
            .write() // Obtain write lock to remove key from memory
            .expect("store lock poisoned")
            .key_dir
            .remove(key)
            // Unreachable right now because only one thread holds &mut Engine. It only ever replaces
            // entries, never removes them.
            .expect("attempted to remove non-existent key");

        self.stats
            .record_dead(tombstone.framed_len() + removed.framed_len());

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    fn append(&mut self, cmd: &Command) -> Result<Entry> {
        self.maybe_roll()?;
        // Capture log position prior to appending
        let pos = self.write_offset;

        let record = record::encode(cmd)?;
        let record_len = record.len();
        let payload_len = u32::try_from(record_len - HEADER_LEN)
            .expect("record_len - HEADER_LEN should not exceed u32");

        // Append the bytes, flush (buffer -> OS) and fsync (OS -> disk)
        // when policy instructs us to, update offset
        self.writer.write_all(&record)?;
        self.writer.flush()?;
        match self.config.fsync {
            FsyncPolicy::Always => {
                self.writer.get_ref().sync_data()?;
            }
            FsyncPolicy::Never => {}
        }
        self.write_offset += record_len as u64;
        self.stats.record_append(record_len as u64);

        let entry = Entry {
            file_id: self.active_id,
            pos,
            len: payload_len,
        };

        Ok(entry)
    }

    /// Close the active file, mark it immutable, and open `active_id + 1`.
    /// Returns the id of the file that was just closed.
    fn roll(&mut self) -> Result<u32> {
        // Flush buf writer and sync data of the current active file. An FsyncPolicy
        // of `Never` does not apply here. We need to fsync the file before declaring
        // it immutable.
        self.sync()?;

        let old_active_id = self.active_id;
        let new_active_id = old_active_id + 1;
        let log_file_path = log_path(&self.data_directory_path, new_active_id);

        // Create the new log file and a write handle for it.
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_file_path)?;

        // Create a read handle for the new log file.
        let active_file_read_handle = Arc::new(File::open(&log_file_path)?);

        // Replace the writer for the old log file with the new
        self.writer = BufWriter::new(file);
        // Insert the read handle of the new log file into the hash map
        self.reader
            .store
            .write()
            .expect("store lock poisoned")
            .files
            .insert(new_active_id, active_file_read_handle);

        // Update active id to point to new log file and set offset to 0 since nothing is written yet
        self.active_id = new_active_id;
        self.write_offset = 0;

        Ok(old_active_id)
    }

    /// Roll if the active file has passed `max_file_bytes` and is not empty.
    fn maybe_roll(&mut self) -> Result<()> {
        if self.write_offset > 0 && self.write_offset >= self.config.max_file_bytes {
            self.roll()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
