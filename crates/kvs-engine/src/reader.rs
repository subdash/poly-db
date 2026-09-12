use std::{
    os::unix::fs::FileExt,
    sync::{Arc, RwLock},
};

use crate::{
    Command, EngineError, Result,
    record::{self, HEADER_LEN},
    store::Store,
};

#[derive(Clone)]
pub struct Reader {
    pub(crate) store: Arc<RwLock<Store>>,
}

impl Reader {
    pub fn get(&self, key: &str) -> Result<String> {
        // Look up entry in key dir
        let (entry, file) = self
            .store
            .read()
            .expect("store lock poisoned")
            .lookup(key)?;

        // Read contents into buffer
        let mut record = vec![0u8; HEADER_LEN + entry.len as usize];
        file.read_exact_at(&mut record, entry.pos)?;

        // Decode and return
        match record::decode(&record, entry.pos)? {
            Command::Set { value, .. } => Ok(value),
            Command::Remove { .. } => Err(EngineError::Corrupt { offset: entry.pos }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Engine, EngineError, Reader};
    use tempfile::TempDir;

    #[test]
    fn a_reader_is_clone_send_and_sync() {
        fn assert_bounds<T: Clone + Send + Sync + 'static>() {}
        assert_bounds::<Reader>();
    }
    #[test]
    fn a_reader_observes_writes_made_after_it_was_created() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let reader = engine.reader();

        engine.set("alpha".into(), "one".into()).expect("set");
        assert_eq!(reader.get("alpha").expect("get"), "one");

        engine.set("alpha".into(), "two".into()).expect("overwrite");
        assert_eq!(reader.get("alpha").expect("get"), "two");

        engine.remove("alpha").expect("remove");
        assert!(matches!(reader.get("alpha"), Err(EngineError::KeyNotFound)));
    }

    #[test]
    fn a_reader_reports_key_not_found_for_unknown_keys() {
        let dir = TempDir::new().expect("tempdir");
        let engine = Engine::open(dir.path()).expect("open");
        let reader = engine.reader();
        assert!(matches!(reader.get("ghost"), Err(EngineError::KeyNotFound)));
    }

    #[test]
    fn readers_work_from_other_threads() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let reader = engine.reader();
                std::thread::spawn(move || reader.get("alpha"))
            })
            .collect();

        for handle in handles {
            let value = handle.join().expect("thread must not panic").expect("get");
            assert_eq!(value, "one");
        }
    }

    #[test]
    fn a_reader_sees_a_write_that_lands_while_other_readers_are_live() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let first = engine.reader();
        let second = first.clone();

        engine.set("alpha".into(), "one".into()).expect("set");
        assert_eq!(first.get("alpha").expect("get"), "one");
        assert_eq!(second.get("alpha").expect("get"), "one");
    }
}
