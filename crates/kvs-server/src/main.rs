use anyhow::Context;
use clap::Parser;
use kvs_engine::Engine;
use kvs_server::{config::Config, routes, writer};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::signal::unix::SignalKind;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = Config::parse();
    let engine = Engine::open_with(&config.data_dir, config.fsync.into())
        .context("opening the data directory")?;
    let (kv_handle, join_handle) = writer::spawn(engine, 1024);
    let router = routes::router(kv_handle);
    let listener = TcpListener::bind(config.addr)
        .await
        .context("binding tcp listener")?;

    tracing::info!(addr = %listener.local_addr()?, "kvs-server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            #[cfg(unix)]
            {
                let mut term = tokio::signal::unix::signal(SignalKind::terminate())
                    .expect("install SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = term.recv() => {},
                }
            }

            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c()
                    .await
                    .expect("install ctrl-c handler");
            }
        })
        .await?;

    // We need to join to block the main thread until its final flush and fsync have completed.
    join_handle
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))?;

    Ok(())
}
