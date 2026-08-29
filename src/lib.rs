pub mod cli;
pub mod config;
pub mod daemon;
pub mod domain;
pub mod error;
pub mod herdr;
pub mod install;
pub mod notion;
pub mod report_spool;
pub mod store;
pub mod tui;

pub async fn run() -> anyhow::Result<()> {
    cli::run().await
}
