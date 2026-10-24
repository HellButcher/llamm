mod api;
mod app;

use std::sync::Arc;

use anyhow::Result;
use app::App;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

struct RestoreDropGuard;

impl Drop for RestoreDropGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    initialize_logging()?;
    let client = api::Client::new("http://localhost:9090");
    let terminal = ratatui::init();
    let d = RestoreDropGuard;
    App::new().run(terminal, Arc::new(client)).await?;
    drop(d);
    Ok(())
}

fn initialize_logging() -> Result<()> {
    let Some(log_file) = std::env::var_os("RUST_LOG_FILE") else {
        return Ok(());
    };
    let log_file = std::fs::File::create(log_file)?;
    let file_subscriber = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_writer(log_file)
        .with_target(false)
        .with_ansi(false)
        .with_filter(tracing_subscriber::filter::EnvFilter::from_default_env());
    tracing_subscriber::registry().with(file_subscriber).init();
    Ok(())
}
