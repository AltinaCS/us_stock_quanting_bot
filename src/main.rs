use serde::Deserialize;
use reqwest::header::{HeaderMap, HeaderValue};
pub mod config;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    config::init_config();
    println!("test");
    Ok(())
}