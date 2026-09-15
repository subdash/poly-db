use crate::{
    Command, Result,
    keydir::Entry,
    layout::{log_ids, log_path},
    record::{self, HEADER_LEN, MAX_PAYLOAD_BYTES},
    store::Store,
};
use std::{
    fs::File,
    io::{BufReader, ErrorKind, Read},
    path::Path,
    sync::Arc,
};

pub(crate) struct Replayed {
    pub(crate) store: Store,
    /// The highest log id if present, or 0 for an empty dir
    pub(crate) active_id: u32,
    /// Where the next append to the active file belongs
    pub(crate) write_offset: u64,
    /// The summed length of every log file, for the merge trigger
    pub(crate) total_bytes: u64,
}

pub(crate) fn build(dir: &Path) -> Result<Replayed> {
    let mut total_bytes: u64 = 0;
    let mut write_offset = 0;
    let log_id_vec = log_ids(dir)?;
    let active_id = log_id_vec.last().copied().unwrap_or(0);
    let mut store = Store::default();

    for id in log_id_vec {
        // For each log file, we open a read handle, store it in the files map,
        // and attempt to replay the file.
        let log_file_path = log_path(dir, id);
        let read_handle = Arc::new(File::open(&log_file_path)?);
        let len = read_handle.metadata()?.len();

        total_bytes += len;
        store.files.insert(id, read_handle.clone());

        let offset = replay_file(&mut store, &read_handle, id)?;
        let dropped_bytes = len - offset;

        if id == active_id {
            write_offset = offset;
        }

        if dropped_bytes > 0 {
            if id == active_id {
                tracing::debug!(
                    file_id = id,
                    offset,
                    dropped_bytes,
                    "the active log ends in a partial record; it will be truncated"
                );
            } else {
                tracing::warn!(
                    file_id = id,
                    offset,
                    dropped_bytes,
                    "an immutable log file is damaged; records after this offset were dropped"
                );
            }
        }
    }

    let replayed = Replayed {
        store,
        active_id,
        write_offset,
        total_bytes,
    };

    Ok(replayed)
}

fn replay_file(store: &mut Store, file: &File, id: u32) -> Result<u64> {
    let mut reader = BufReader::new(file);
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
        let payload_len = record::payload_len(header);

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
                    file_id: id,
                    len: payload_len,
                };
                store.key_dir.insert(key, entry);
            }

            Command::Remove { key } => {
                store.key_dir.remove(&key);
            }
        }

        offset += HEADER_LEN as u64 + u64::from(payload_len);
    }

    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{
        log_path,
        test_support::{locate, remove, set, write_log},
    };
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn replaying_an_empty_directory_yields_an_empty_store() {
        let dir = TempDir::new().expect("tempdir");
        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.key_dir.is_empty());
        assert_eq!(replayed.active_id, 0);
        assert_eq!(replayed.write_offset, 0);
        assert_eq!(replayed.total_bytes, 0);
    }

    #[test]
    fn a_single_file_replays_every_record() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("beta", "two")];
        let len = write_log(dir.path(), 0, &cmds);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.store.key_dir.len(), 2);
        assert_eq!(replayed.active_id, 0);
        assert_eq!(replayed.write_offset, len);
        assert_eq!(replayed.total_bytes, len);
    }

    #[test]
    fn a_later_file_wins_over_an_earlier_one() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "old")]);
        write_log(dir.path(), 1, &[set("alpha", "new")]);

        let replayed = build(dir.path()).expect("build");
        let entry = replayed.store.key_dir.get("alpha").expect("alpha");
        assert_eq!(entry.file_id, 1);
    }

    #[test]
    fn file_ids_replay_in_numeric_order_not_lexicographic_order() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 2, &[set("alpha", "second")]);
        write_log(dir.path(), 10, &[set("alpha", "tenth")]);

        let replayed = build(dir.path()).expect("build");
        let entry = replayed.store.key_dir.get("alpha").expect("alpha");
        assert_eq!(
            entry.file_id, 10,
            "\"10\" sorts before \"2\" as a
  string"
        );
        assert_eq!(replayed.active_id, 10);
    }
    #[test]
    fn a_remove_in_a_later_file_shadows_a_set_in_an_earlier_one() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[remove("alpha")]);

        let replayed = build(dir.path()).expect("build");
        assert!(!replayed.store.key_dir.contains_key("alpha"));
    }

    #[test]
    fn a_set_after_a_remove_brings_the_key_back() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one"), remove("alpha")]);
        write_log(dir.path(), 1, &[set("alpha", "again")]);

        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.key_dir.contains_key("alpha"));
    }

    #[test]
    fn entries_record_the_file_and_offset_they_came_from() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("beta", "two")];
        write_log(dir.path(), 4, &cmds);

        let replayed = build(dir.path()).expect("build");
        let (pos, len) = locate(&cmds, 1);
        let entry = replayed.store.key_dir.get("beta").expect("beta");
        assert_eq!((entry.file_id, entry.pos, entry.len), (4, pos, len));
    }

    #[test]
    fn every_replayed_file_gets_a_read_handle() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[set("beta", "two")]);
        write_log(dir.path(), 2, &[set("gamma", "three")]);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.store.files.len(), 3);
        for id in [0, 1, 2] {
            assert!(
                replayed.store.files.contains_key(&id),
                "missing
  handle for {id}"
            );
        }
    }

    #[test]
    fn total_bytes_is_the_sum_of_every_file() {
        let dir = TempDir::new().expect("tempdir");
        let first = write_log(dir.path(), 0, &[set("alpha", "one")]);
        let second = write_log(dir.path(), 1, &[set("beta", "two")]);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.total_bytes, first + second);
    }

    #[test]
    fn a_torn_tail_on_the_active_file_sets_the_write_offset_below_its_length() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one")];
        let good = write_log(dir.path(), 0, &cmds);

        // Append half a header — the shape a crash mid-write leaves  behind.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(log_path(dir.path(), 0))
            .expect("reopen");
        file.write_all(&[0u8; 4]).expect("append garbage");
        drop(file);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(
            replayed.write_offset, good,
            "the tail must not be
  counted"
        );
        assert!(replayed.store.key_dir.contains_key("alpha"));
    }

    #[test]
    fn corruption_in_an_immutable_file_does_not_discard_the_files_above_it() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("beta", "two")];
        write_log(dir.path(), 0, &cmds);
        write_log(dir.path(), 1, &[set("gamma", "three")]);

        // Corrupt the payload of the *second* record in file 0.
        let (pos, _) = locate(&cmds, 1);
        let path = log_path(dir.path(), 0);
        let mut bytes = std::fs::read(&path).expect("read");
        let target = pos as usize + crate::record::HEADER_LEN;
        bytes[target] ^= 0xff;
        std::fs::write(&path, &bytes).expect("rewrite");

        let replayed = build(dir.path()).expect("build");
        assert!(
            replayed.store.key_dir.contains_key("alpha"),
            "the good
  prefix survives"
        );
        assert!(
            !replayed.store.key_dir.contains_key("beta"),
            "the bad
  record is dropped"
        );
        assert!(
            replayed.store.key_dir.contains_key("gamma"),
            "file 1 is
  still replayed"
        );
        assert_eq!(replayed.active_id, 1);
    }
}
