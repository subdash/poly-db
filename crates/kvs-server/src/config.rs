use std::{net::SocketAddr, path::PathBuf};

use kvs_engine::FsyncPolicy;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum FsyncArg {
    Always,
    Never,
}

impl From<FsyncArg> for FsyncPolicy {
    fn from(value: FsyncArg) -> Self {
        match value {
            FsyncArg::Always => FsyncPolicy::Always,
            FsyncArg::Never => FsyncPolicy::Never,
        }
    }
}

#[derive(clap::Parser, Debug)]
pub struct Config {
    #[arg(long, env = "KVS_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,
    #[arg(long, env = "KVS_ADDR", default_value = "127.0.0.1:3000")]
    pub addr: SocketAddr,
    #[arg(long, value_enum, env = "KVS_FSYNC", default_value_t = FsyncArg::Always)]
    pub fsync: FsyncArg,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn flags_set_every_field() {
        let config = Config::parse_from([
            "kvs-server",
            "--addr",
            "0.0.0.0:9999",
            "--data-dir",
            "/var/lib/kvs",
            "--fsync",
            "never",
        ]);

        assert_eq!(config.addr.to_string(), "0.0.0.0:9999");
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/kvs"));
        assert!(matches!(config.fsync, FsyncArg::Never));
    }

    #[test]
    fn the_defaults_are_loopback_and_a_relative_data_dir() {
        let config = Config::parse_from(["kvs-server"]);

        assert_eq!(
            config.addr.to_string(),
            "127.0.0.1:3000",
            "the default must stay loopback; the container overrides it"
        );
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert!(matches!(config.fsync, FsyncArg::Always));
    }
}
