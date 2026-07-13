use quant_bot::config;
use quant_bot::trading::{get_account};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    config::init_config();
    let client = reqwest::Client::new();
    //place_order(&client, "NVDA", Side::Buy, OrderType::Market, TimeInForce::Gtc, OrderMethod::Qty(dec!(10))).await?;
    let account = get_account(&client).await?;
    println!("{:#?}",account);
    
    Ok(())
}