use std::{collections::HashMap, fs::File, sync::Arc};

use crate::EngineError;
use crate::error::Result;
use crate::keydir::{Entry, KeyDir};

#[derive(Default)]
pub(crate) struct Store {
    pub(crate) key_dir: KeyDir,
    pub(crate) files: HashMap<u32, Arc<File>>,
}

impl Store {
    pub(crate) fn lookup(&self, key: &str) -> Result<(Entry, Arc<File>)> {
        let entry = self
            .key_dir
            .get(key)
            .copied()
            .ok_or(EngineError::KeyNotFound)?;

        let file = self
            .files
            .get(&entry.file_id)
            .ok_or(EngineError::Corrupt { offset: entry.pos })?;

        Ok((entry, Arc::clone(file)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EngineError, keydir::Entry};
    use tempfile::TempDir;

    fn store_with_one_file(dir: &TempDir, file_id: u32) -> Store {
        let path = dir.path().join(format!("{file_id}.log"));
        std::fs::write(&path, b"eight-by").expect("write");
        let handle = Arc::new(File::open(&path).expect("open"));

        let mut store = Store::default();
        store.files.insert(file_id, handle);
        store
    }

    #[test]
    fn a_new_store_is_empty() {
        let store = Store::default();
        assert!(store.key_dir.is_empty());
        assert!(store.files.is_empty());
    }

    #[test]
    fn a_lookup_returns_the_entry_and_its_file_together() {
        let dir = TempDir::new().expect("tempdir");
        let mut store = store_with_one_file(&dir, 7);
        store.key_dir.insert(
            "alpha".to_string(),
            Entry {
                file_id: 7,
                pos: 0,
                len: 3,
            },
        );

        let (entry, file) = store.lookup("alpha").expect(
            "alpha must
  resolve",
        );
        assert_eq!(entry.file_id, 7);
        assert_eq!(entry.pos, 0);
        assert_eq!(file.metadata().expect("metadata").len(), 8);
    }

    #[test]
    fn a_lookup_of_an_unknown_key_reports_key_not_found() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_with_one_file(&dir, 0);
        assert!(matches!(
            store.lookup("ghost"),
            Err(EngineError::KeyNotFound)
        ));
    }

    #[test]
    fn an_entry_naming_a_file_the_table_lacks_is_corrupt_not_missing() {
        let dir = TempDir::new().expect("tempdir");
        let mut store = store_with_one_file(&dir, 0);
        store.key_dir.insert(
            "alpha".to_string(),
            Entry {
                file_id: 9,
                pos: 128,
                len: 3,
            },
        );

        assert!(matches!(
            store.lookup("alpha"),
            Err(EngineError::Corrupt { offset: 128 })
        ));
    }

    #[test]
    fn two_keys_in_different_files_each_resolve_to_their_own_handle() {
        let dir = TempDir::new().expect("tempdir");
        let mut store = store_with_one_file(&dir, 0);

        let second = dir.path().join("1.log");
        std::fs::write(&second, b"sixteen-bytes-!!").expect("write");
        store
            .files
            .insert(1, Arc::new(File::open(&second).expect("open")));

        store.key_dir.insert(
            "alpha".into(),
            Entry {
                file_id: 0,
                pos: 0,
                len: 1,
            },
        );
        store.key_dir.insert(
            "beta".into(),
            Entry {
                file_id: 1,
                pos: 0,
                len: 1,
            },
        );

        let (_, first_file) = store.lookup("alpha").expect("alpha");
        let (_, second_file) = store.lookup("beta").expect("beta");
        assert_eq!(first_file.metadata().expect("metadata").len(), 8);
        assert_eq!(second_file.metadata().expect("metadata").len(), 16);
    }
}
