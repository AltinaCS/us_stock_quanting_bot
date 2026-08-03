use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime,Datelike,NaiveDate,NaiveDateTime};
use quant_bot::db_storage::{MarketDatabase,AppConfig,AlpacaClient, Preprocessor,FactorType};
use chrono_tz::America::New_York;
use quant_bot::trading::{self, OrderMethod, OrderType, OrderTypeInput, Side, TimeInForce, place_order};
use quant_bot::{backtest, config, risk_manager, selector};
use quant_bot::db_storage::{TimeframeConfig};
use quant_bot::selector::{SelectionMode, SelectionPipeline};
use rust_decimal_macros::dec;
use rand::prelude::IndexedRandom;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeSet};
use std::fs;
use rand::RngExt;
use rust_decimal::Decimal;
use crate::backtest::AnalysisConfig;
use indicatif::{ProgressBar, ProgressStyle};
use quant_bot::risk_manager::{BarUpdate, RiskManager, start_websocket_listener,RiskConfig};

use std::env;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //###########################初始化################################################
    tracing_subscriber::fmt::init();
   
    let db = MarketDatabase::new().await?;
    let client = reqwest::Client::new();                                                                                                                                                                                                                                                         
     
    println!("[主程式] 開始線上抓取 S&P 500 成分股...");                                                                                                                                                                                                                                         
    let sp500_symbols = MarketDatabase::fetch_sp500_symbols(&client).await?;                                                                                                                                                                                                                   
    println!("[主程式] 成功取得 {} 檔 S&P 500 成分股名單！", sp500_symbols.len());                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                
    db.sync_sp500_constituents(&sp500_symbols).await?;                                                                                                                                                                                                                                         
    println!("[主程式] S&P 500 成分股對齊與同步完成！");                                                                                                                                                                                                                                         
                                                                                                                                                                                                                
    // 關鍵功能註解：執行自動選股與歷史數據回填流程                                                                                                                                                                                                                                             
    selector::SelectionPipeline::run_pipeline(&client, &db, SelectionMode::Sp500Only).await?;                                                                                                                                                                                                  
                                                                                                                                                                                                                                                                                                
    println!("[主程式] 全套選股與歷史數據回填流程完成！");
    db.load_sp500_assets().await?;
    
    // 關鍵功能註解：從投資組合資料表載入現有標的 (包含 asset_id 與 symbol)
    let targets = db.load_portfolio_targets().await?;
    let symbols: Vec<String> = targets
        .iter()
        .map(|t| t.symbol.clone())
        .collect();
    let analysis_config: AnalysisConfig = AnalysisConfig { ..Default::default() };
    //###########################初始化################################################

    // 關鍵功能註解：初始化風控管理器與取消停利點設定
    let mut risk_config = RiskConfig::default();
    risk_config.take_profit_pct = None; 

    let risk_manager = RiskManager::new(
        db.pool.clone(),
        client.clone(),
        risk_config,
    );

    // 關鍵功能註解：建立非同步 mpsc 通道與即時最高價紀錄雜湊表
    let (tx_price, mut rx_price) = tokio::sync::mpsc::channel::<BarUpdate>(100);
    let mut peak_prices = std::collections::HashMap::<String, Decimal>::new();

    // 關鍵功能註解：當日砍倉黑名單，當日已觸發風控之標的不重複 Rebalance
    let mut daily_blacklisted_symbols = std::collections::HashSet::<String>::new();

    // 關鍵功能註解：啟動背景 WebSocket 行情監聽任務
    let ws_symbols = symbols.clone();
    tokio::spawn(async move {
        if let Err(e) = start_websocket_listener(ws_symbols, tx_price).await {
            println!("WebSocket 監聽異常終止: {:?}", e);
        }
    });

    // 關鍵功能註解：設定 15 分鐘週期定時器並消耗首次立即觸發
    let mut rebalance_timer = tokio::time::interval(tokio::time::Duration::from_secs(900));
    rebalance_timer.tick().await;

    // 關鍵功能註解：雙軌事件主迴圈 (休市過濾 + A軌即時風控 + B軌慢速 Rebalance)
    loop {
        // 關鍵功能註解：休市時動態睡眠，避免跨開盤點時延遲
        if !AlpacaClient::is_market_window_open() {
            // 關鍵功能註解：休市時自動清空前一交易日的黑名單
            if !daily_blacklisted_symbols.is_empty() {
                daily_blacklisted_symbols.clear();
            }
            let sleep_secs = AlpacaClient::seconds_until_next_market_open().min(30);
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_secs)).await;
            continue;
        }

        tokio::select! {
            // A 軌 (Socket 高速)：僅處理硬性熔斷 (stop_loss_pct) 與移動停損 (trailing_stop_pct)
            Some(bar) = rx_price.recv() => {
                // 關鍵功能註解：若該標的已在當日黑名單中則不重複處理
                if !daily_blacklisted_symbols.contains(&bar.symbol) {
                    if let Err(e) = risk_manager.check_instant_stop_loss(&bar, &mut peak_prices).await {
                        println!("即時風控觸發砍倉: {:?}，將 {:?} 加入當日黑名單", e, bar.symbol);
                        daily_blacklisted_symbols.insert(bar.symbol.clone());
                    }
                }
            }

            // B 軌 (RESTful 慢速)：定時重算策略並帶入 should_recalculate 決定是否覆寫 DB
            _ = rebalance_timer.tick() => {
                println!("[排程] 執行週期性 K 線抓取與歷史數據分析...");

                // 關鍵功能註解：直接使用 targets 內的 asset_id 與 symbol 抓取價格資料
                for target in &targets {
                    if let Err(e) = AlpacaClient::fetch_price_data(&client, target.asset_id, &target.symbol, &TimeframeConfig::FiveMinutes, None, None).await {
                        println!("抓取 5m K線失敗 [{}]: {:?}", target.symbol, e);
                    }
                    if let Err(e) = AlpacaClient::fetch_price_data(&client, target.asset_id, &target.symbol, &TimeframeConfig::OneDay, None, None).await {
                        println!("抓取 1d K線失敗 [{}]: {:?}", target.symbol, e);
                    }
                }

                // 關鍵功能註解：取得最近一次開盤點的時間戳，避免跨日/盤前誤判
                let last_open_ts = {
                    let now_ny = Utc::now().with_timezone(&New_York);
                    now_ny
                        .date_naive()
                        .and_hms_opt(9, 30, 0)
                        .and_then(|naive_dt| naive_dt.and_local_timezone(New_York).single())
                        .map(|dt| dt.timestamp())
                        .unwrap_or_else(|| Utc::now().timestamp())
                };

                // 關鍵功能註解：查詢 DB 是否已有今日開盤後的基準，決定 run_analysis 最後一個參數
                let should_recalculate = match db.should_recalculate_weights(last_open_ts).await {
                    Ok(need_calc) => need_calc,
                    Err(e) => {
                        println!("查詢權重時間戳失敗: {:?}，預設不覆寫 DB", e);
                        false
                    }
                };
                let mut basic_weights = std::collections::HashMap::<String, Decimal>::new();
                // 關鍵功能註解：最後一個參數直接帶入動態算出的 should_recalculate 布林值
                match backtest::run_analysis(&client, &db, &symbols, &analysis_config, should_recalculate).await {
                    Ok(new_result) => {
                        if should_recalculate{
                            basic_weights =new_result.weights.clone();
                        }
                        // 關鍵功能註解：傳入當日黑名單，讓調倉邏輯忽略已被即時停損的股票
                        if let Err(e) = risk_manager.run_risk_manager(&client, new_result, &daily_blacklisted_symbols,&basic_weights).await {
                            println!("週期調倉失敗: {:?}", e);
                        }
                    }
                    Err(e) => println!("週期分析失敗: {:?}", e),
                }
            }
        }
    }
}
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
