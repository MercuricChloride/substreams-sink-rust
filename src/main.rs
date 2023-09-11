use anyhow::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    geo::main().await?;
    Ok(())
}
