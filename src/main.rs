use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime,Datelike,NaiveDate};
use quant_bot::db_storage::{MarketDatabase,AppConfig,AlpacaClient, Preprocessor,FactorType};
use chrono_tz::America::New_York;
use quant_bot::trading::{OrderMethod, OrderTypeInput, Side, TimeInForce, place_order};
use quant_bot::{backtest, config, selector};
use quant_bot::db_storage::{TimeframeConfig};
use quant_bot::selector::{SelectionMode, SelectionPipeline};
use rust_decimal_macros::dec;
use rand::prelude::IndexedRandom;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeSet};
use std::fs;
use rand::RngExt;
use crate::backtest::AnalysisConfig;
use indicatif::{ProgressBar, ProgressStyle};

use std::env;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
   
   // 關鍵功能註解：建立資料庫連線實例與HTTP客戶端
    let db = MarketDatabase::new().await?;
    let client = reqwest::Client::new();
    db.load_sp500_assets().await?;
    
    // 關鍵功能註解：從投資組合資料表載入現有標的並提取代碼清單
    let targets = db.load_portfolio_targets().await?;
    let symbols: Vec<String> = targets
        .into_iter()
        .map(|t| t.symbol)
        .collect();
    let analysis_config:AnalysisConfig  = AnalysisConfig{..Default::default()};
    // 執行歷史數據統計分析流程
    backtest::run_analysis(&client, &db, &symbols, &analysis_config).await?;
    
    Ok(())
    /* 
    // 關鍵功能註解：線上抓取S&P500成分股並同步至資料庫                                                                                                         
        println!("[主程式] 開始線上抓取 S&P 500 成分股...");                                                                                                                   
        let sp500_symbols = MarketDatabase::fetch_sp500_symbols(&client).await?;                                                                                                               
        println!("[主程式] 成功取得 {} 檔 S&P 500 成分股名單！", sp500_symbols.len());                                                                                         
                                                                                                                                                                               
        db.sync_sp500_constituents(&sp500_symbols).await?;                                                                                                                     
        println!("[主程式] S&P 500 成分股對齊與同步完成！");                                                                                                                   
                                                                                                              
        // 關鍵功能註解：執行自動選股與歷史數據回填流程                                                                                                                          
        selector::SelectionPipeline::run_pipeline(&client, &db,SelectionMode::Sp500Only).await?;                                                                                                        
                                                                                                                                                                               
        println!("[主程式] 全套選股與歷史數據回填流程完成！");                                                                                                                                                                                                                            
        Ok(())   
        */
/* 
    let client = reqwest::Client::new();
    //get_assets(&client).await?;
   
    let db = MarketDatabase::new().await?;

    // TODO:vm031
    let config_str = fs::read_to_string("config.toml")?;
    let app_config: AppConfig = toml::from_str(&config_str)?;
    let trash_talks = app_config.idle_configs.trash_talks;

    // 關鍵功能註解：初始化計時器與隨機閒置定時器
    let mut fetch_interval = tokio::time::interval(std::time::Duration::from_secs(15 * 60));
    let idle_timer = tokio::time::sleep(std::time::Duration::from_secs(5));

    // 關鍵功能註解：將Sleep固定在Stack上以滿足Unpin特徵
    tokio::pin!(idle_timer); 

    let mut rng = rand::rng();
    let mut counter: u64 = 1; 
    if AlpacaClient::is_market_window_open() == false {
        println!("今天休市，股價抓取模組休息中...");
        return Ok(());
    }

    loop {
        tokio::select! {
            _ = fetch_interval.tick() => {
                counter = 1;
                //TODO:選標要改成從資料庫抓
                let config_str = fs::read_to_string("config.toml")?;
                let app_config: AppConfig = toml::from_str(&config_str)?;
                let mut raw_portfolio = std::collections::HashMap::new();

                for symbol in &app_config.market.symbols {
                    println!("正在抓取 {} 的資料...", symbol);
                    match AlpacaClient::fetch_5m_data(symbol,None,None).await {
                        Ok(klines) => { raw_portfolio.insert(symbol.clone(), klines); },
                        Err(e) => println!("抓取 {} 失敗: {}", symbol, e),
                    }
                }

                let (_timeline, aligned_portfolio) = Preprocessor::process_and_align(raw_portfolio).await?;

                for (symbol, klines) in aligned_portfolio {
                    let count = db.save_klines(symbol.clone(), klines).await.map_err(Into::<Box<dyn std::error::Error>>::into)?;
                    println!("已將 {} 筆 {} 的K線寫入資料庫", count, symbol);
                }
                db.list_all_klines(None).await.map_err(Into::<Box<dyn std::error::Error>>::into)?;
            }
            _ = &mut idle_timer => {
                // 關鍵功能註解：隨機輸出垃圾話並重設下次閒置時間
                if let Some(talk) = trash_talks.choose(&mut rng) {
                    println!("\n[系統碎碎念{}] {}\n", counter, talk);
                }
                counter += 1;
                let next_sleep = rng.random_range(5..=15);
                idle_timer.set(tokio::time::sleep(std::time::Duration::from_secs(next_sleep)));
            }
        }
    }
*/
}