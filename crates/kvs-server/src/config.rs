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
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:3000")]
    pub addr: SocketAddr,
    #[arg(long, value_enum, default_value_t = FsyncArg::Always)]
    pub fsync: FsyncArg,
}
