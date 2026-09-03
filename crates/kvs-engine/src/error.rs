#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt record at offset {offset}")]
    Corrupt { offset: u64 },
    #[error("key not found")]
    KeyNotFound,
    #[error("key too large: {len} bytes")]
    KeyTooLarge { len: usize },
    #[error("value too large: {len} bytes")]
    ValueTooLarge { len: usize },
    #[error("engine is shutting down")]
    ShuttingDown,
    #[error("failed to encode a record: {0}")]
    Encode(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_describe_themselves() {
        let err = EngineError::Corrupt { offset: 4096 };
        assert_eq!(err.to_string(), "corrupt record at offset 4096");
        assert_eq!(EngineError::KeyNotFound.to_string(), "key not found");
    }

    #[test]
    fn io_errors_convert_automatically() {
        let io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "nope");
        let err: EngineError = io.into();
        assert!(matches!(err, EngineError::Io(_)));
    }
}
