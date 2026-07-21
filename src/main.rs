use quant_bot::db_storage;
use serde::Deserialize;
use reqwest::header::{HeaderMap, HeaderValue};
pub mod config;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("test");
    db_storage::MarketDatabase::new().await?;
    println!("資料庫建立完成");
    Ok(())
}