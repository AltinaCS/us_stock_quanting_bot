use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime,Datelike,NaiveDate,NaiveDateTime};
use quant_bot::db_storage::{MarketDatabase,AppConfig,AlpacaClient, Preprocessor,FactorType};
use chrono_tz::America::New_York;
use quant_bot::trading::{self, OrderMethod, OrderType, OrderTypeInput, Side, TimeInForce, place_order};
use quant_bot::{backtest, config, risk_manager, selector};
use quant_bot::db_storage::{TimeframeConfig};
use quant_bot::selector::{SelectionMode, SelectionPipeline};
use rust_decimal_macros::dec;
use tracing::{debug, error, info, warn};
use rand::prelude::IndexedRandom;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeSet};
use anyhow::Result;
use std::fs;
use std::time::Duration;
use rand::RngExt;
use rust_decimal::Decimal;
use crate::backtest::AnalysisConfig;
use indicatif::{ProgressBar, ProgressStyle};
use quant_bot::risk_manager::{BarUpdate, RiskManager, start_websocket_listener,RiskConfig};
use tracing_subscriber::{fmt,layer::SubscriberExt, util::SubscriberInitExt,EnvFilter};
use std::path::Path;
use tracing_appender::non_blocking::WorkerGuard;
use std::panic;

pub fn init_logging() -> WorkerGuard {
    panic::set_hook(Box::new(|panic_info| {
        let location = panic_info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "未知位置".to_string());

        let payload = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "無詳細錯誤訊息".to_string()
        };

        error!(target: "panic", location = %location, message = %payload, "系統發生未預期的 panic！");
    }));

    // 關鍵功能註解：讀取設定檔等級並追加指令屏蔽第三方網路庫雜訊 Log
    let env_filter = EnvFilter::try_new(&*config::LOG_LEVEL)
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("hyper=warn".parse().unwrap())
        .add_directive("hyper_util=warn".parse().unwrap())
        .add_directive("h2=warn".parse().unwrap())
        .add_directive("reqwest=warn".parse().unwrap())
        .add_directive("sqlx=warn".parse().unwrap());

    // 關鍵功能註解：設定每日滾動日誌檔案輸出路徑與檔名
    let log_dir = "./logs";
    let file_appender = tracing_appender::rolling::daily(log_dir, "quant_bot.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    // 關鍵功能註解：組合控制台與非阻塞檔案寫入器並進行全域註冊
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer())
        .with(fmt::layer().with_writer(non_blocking))
        .init();

    // 關鍵功能註解：背景啟動每日定時清理舊日誌任務 (保留 7 天)
    tokio::spawn(async move {
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(86400));
        loop {
            cleanup_interval.tick().await;
            clean_old_logs(log_dir, 7);
        }
    });

    guard
}
fn clean_old_logs<P: AsRef<Path>>(dir_path: P, max_days: i64) {
    let today = chrono::Local::now().naive_local().date();

    let entries = match fs::read_dir(dir_path) {
        Ok(e) => e,
        Err(e) => {
            error!("讀取日誌目錄失敗: {:?}", e);
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                // 關鍵功能註解：從檔名尾端擷取 YYYY-MM-DD 格式日期 (例如 quant_bot.log.2026-08-04)
                if let Some(date_str) = file_name.split('.').last() {
                    if let Ok(log_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        let age_days = (today - log_date).num_days();
                        if age_days > max_days {
                            if let Err(e) = fs::remove_file(&path) {
                                error!("刪除過期日誌失敗 {:?}: {:?}", path, e);
                            } else {
                                info!("已自動清理過期日誌: {:?}", path);
                            }
                        }
                    }
                }
            }
        }
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    //###########################初始化################################################
    let _log_guard = init_logging();
   
    let db = MarketDatabase::new().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let client = reqwest::Client::new();                                                                                                                                                                                                                                                         
     
    info!("[主程式] 開始線上抓取 S&P 500 成分股...");                                                                                                                                                                                                                                         
    let sp500_symbols = MarketDatabase::fetch_sp500_symbols(&client).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;                                                                                                                                                                                                                   
    info!("[主程式] 成功取得 {} 檔 S&P 500 成分股名單！", sp500_symbols.len());                                                                                                                                                                                                            
                                                                                                                                                                                                                 
    db.sync_sp500_constituents(&sp500_symbols).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;                                                                                                                                                                                                                                         
    info!("[主程式] S&P 500 成分股對齊與同步完成！");                                                                                                                                                                                                                                         
                                                                                                                                                                                                                
    // 關鍵功能註解：執行自動選股與歷史數據回填流程                                                                                                                                                                                                                                             
    selector::SelectionPipeline::run_pipeline(&client, &db, SelectionMode::Sp500Only).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;                                                                                                                                                                                                  
                                                                                                                                                                                                                                                                                                
    info!("[主程式] 全套選股與歷史數據回填流程完成！");
    db.load_sp500_assets().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
     
    
    // 關鍵功能註解：從投資組合資料表載入現有標的 (包含 asset_id 與 symbol)
    // 關鍵功能註解：從投資組合資料表載入現有標的 (包含 asset_id 與 symbol)
let targets = db.load_portfolio_targets().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
let symbols: Vec<String> = targets
    .iter()
    .map(|t| t.symbol.clone())
    .collect();

// 關鍵功能註解：將現有持倉標的轉換為 basic_weights 雜湊表供風控偏離度比對
let basic_weights: std::collections::HashMap<String, Decimal> = targets
    .iter()
    .map(|t| (t.symbol.clone(), t.weight))
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
//初次計算權重 
backtest::run_analysis(&client, &db, &symbols, &analysis_config, true).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
info!("初始權重計算完成");
tokio::spawn(async move {
    if let Err(e) = start_websocket_listener(ws_symbols, tx_price).await {
        error!("WebSocket 監聽異常終止: {:?}", e);
    }
});

// 關鍵功能註解：設定 15 分鐘週期定時器並消耗首次立即觸發
let mut rebalance_timer = tokio::time::interval(tokio::time::Duration::from_secs(900));
rebalance_timer.tick().await;

// 關鍵功能註解：雙軌事件主迴圈 (休市過濾 + A軌即時風控 + B軌慢速 Rebalance)
// 關鍵功能註解：複製所需引數供雙軌獨立 Task 使用
let (symbols_a, symbols_b) = (symbols.clone(), symbols.clone());
let (targets_b, basic_weights_b) = (targets.clone(), basic_weights.clone());
let (db_a, db_b) = (db.clone(), db.clone());
let (client_a, client_b) = (client.clone(), client.clone());
let (risk_manager_a, risk_manager_b) = (risk_manager.clone(), risk_manager.clone());

// 關鍵功能註解：跨 Task 共享的當日黑名單 (使用 Arc + Mutex 確保執行緒安全)
let daily_blacklisted_symbols = std::sync::Arc::new(tokio::sync::Mutex::new(
    std::collections::HashSet::<String>::new()
));
let blacklisted_a = daily_blacklisted_symbols.clone();
let blacklisted_b = daily_blacklisted_symbols.clone();

// ==================== [A 軌：獨立 Socket 即時風控 Task] ====================
// ==================== [A 軌：獨立 Socket 即時風控 Task] ====================
tokio::spawn(async move {
    let mut peak_prices = std::collections::HashMap::<String, Decimal>::new();

    while let Some(bar) = rx_price.recv().await {
        // 關鍵功能註解：休市期間跳過即時風控監聽
        if !AlpacaClient::is_market_window_open() {
            continue;
        }

        let is_blacklisted = {
            let guard = blacklisted_a.lock().await;
            guard.contains(&bar.symbol)
        };

        if !is_blacklisted {
            if let Err(e) = risk_manager_a.check_instant_stop_loss(&bar, &mut peak_prices).await {
                info!("即時風控觸發砍倉: {:?}，將 {:?} 加入當日黑名單", e, bar.symbol);
                let mut guard = blacklisted_a.lock().await;
                guard.insert(bar.symbol.clone());
            }
        }
    }
});

// ==================== [B 軌：獨立定時 Rebalance Task] ====================
tokio::spawn(async move {
    let mut rebalance_timer = tokio::time::interval(tokio::time::Duration::from_secs(900));
    rebalance_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        rebalance_timer.tick().await;

        // 關鍵功能註解：休市時睡眠並自動清理黑名單
        if !AlpacaClient::is_market_window_open() {
            {
                let mut guard = blacklisted_b.lock().await;
                if !guard.is_empty() {
                    info!("休市中，正在清理盤中黑名單");
                    guard.clear();
                }
            }
            let sleep_secs = AlpacaClient::seconds_until_next_market_open().min(30);
            info!("目前休市中，B軌排程進入休眠狀態");
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_secs)).await;
            continue;
        }

        info!("[排程] 執行週期性 K 線抓取與歷史數據分析...");

        for target in &targets_b {
            if let Err(e) = AlpacaClient::fetch_price_data(&client_b, target.asset_id, &target.symbol, &TimeframeConfig::FiveMinutes, None, None).await {
                error!("抓取 5m K線失敗 [{}]: {:?}", target.symbol, e);
            }
            if let Err(e) = AlpacaClient::fetch_price_data(&client_b, target.asset_id, &target.symbol, &TimeframeConfig::OneDay, None, None).await {
                error!("抓取 1d K線失敗 [{}]: {:?}", target.symbol, e);
            }
        }

        // 關鍵功能註解：透過 run_analysis 進行資料分析與回測
        let analysis_res = backtest::run_analysis(&client_b, &db_b, &symbols_b, &analysis_config, false).await;

        match analysis_res {
            Ok(new_result) => {
                let current_blacklist = {
                    let guard = blacklisted_b.lock().await;
                    guard.clone()
                };

                if let Err(e) = risk_manager_b.run_risk_manager(&db_b, &client_b, new_result, &current_blacklist, &basic_weights_b).await {
                    error!("週期調倉失敗: {:?}", e);
                }
            }
            Err(e) => error!("週期分析失敗: {}", e),
        }
    }
});

// 關鍵功能註解：主線程保持存活 (例如等待 SIGINT 訊號)
tokio::signal::ctrl_c().await?;
info!("收到終止訊號，系統關閉中...");
Ok(())
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
