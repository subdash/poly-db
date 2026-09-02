use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_compare_by_value() {
        let a = Command::Set {
            key: "alpha".into(),
            value: "one".into(),
        };
        let b = Command::Set {
            key: "alpha".into(),
            value: "one".into(),
        };
        let c = Command::Remove {
            key: "alpha".into(),
        };

        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
