#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kbctl::run().await
}
