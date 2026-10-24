mod api;
mod app;

use std::sync::Arc;

use anyhow::Result;
use app::App;

struct RestoreDropGuard;

impl Drop for RestoreDropGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = api::Client::new("http://localhost:9090");
    let terminal = ratatui::init();
    let d = RestoreDropGuard;
    App::new().run(terminal, Arc::new(client)).await?;
    drop(d);
    Ok(())
}
