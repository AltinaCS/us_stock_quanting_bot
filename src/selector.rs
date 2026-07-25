use std::collections::HashMap;                                                                                                                                 
use serde::{Deserialize, Serialize};                                                                                                                           
use crate::db_storage::{AlpacaAsset, AlpacaClient, KLine, MarketDatabase, PortfolioTarget, TargetStatus, TargetType};                                          
use crate::portfolio::PortfolioManager;                                                                                                                        
use chrono::{DateTime, Utc};                                                                                                                                   
use rust_decimal::Decimal;                                                                                                                                     
use rust_decimal_macros::dec;                                                                                                                                  
use futures::stream::{self, StreamExt};                                                                                                                        
use tokio::sync::Semaphore;                                                                                                                                    
use std::sync::Arc;                                                                                                                                            
                                                                                                                                                                   
    // 關鍵功能註解：限制最大併發數並平行抓取評分資產數據                                                                                                          
                                                                                                                                                                    
#[derive(Debug, Clone, Serialize, Deserialize)]                                                                                                                
pub struct StockCandidate {                                                                                                                                    
    pub symbol: String,                                                                                                                                        
    pub sector: String,                                                                                                                                        
    pub avg_dollar_volume: f64,                                                                                                                                
    pub performance_score: f64,                                                                                                                                
}                                                                                                                                                              
                                                                                                                                                                
#[derive(Debug, Clone)]                                                                                                                                        
pub struct SelectorConfig {                                                                                                                                    
    pub total_socket_slots: usize,                                                                                                                             
    pub indicator_symbols: Vec<String>,                                                                                                                        
    pub max_per_sector: usize,                                                                                                                                 
    pub liquidity_weight: f64,                                                                                                                                 
    pub performance_weight: f64,                                                                                                                               
}                                                                                                                                                              
 #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]                                                                                                       
    pub enum SelectionMode {                                                                                                                                                   
        Sp500Only, // 僅針對 S&P 500 成分股選股                                                                                                                                
        AllAssets, // 對 Alpaca 資料庫中所有美股資產選股                                                                                                                       
    }                                                                                                                                                 
pub struct SelectionPipeline;                                                                                                                                  
                                                                                                                                                                
impl SelectionPipeline {                                                                                                                                       
    // 關鍵功能註解：執行自動選股與歷史數據回填全套資料流                                                                                                      
    pub async fn run_pipeline(                                                                                                                                 
            client: &reqwest::Client,                                                                                                                              
            db: &MarketDatabase,   
            mode: SelectionMode                                                                                                                                
        ) -> Result<(), Box<dyn std::error::Error>> {
            let existing_targets = db.load_portfolio_targets().await?;                                                                                        
                if !existing_targets.is_empty() {                                                                                                                 
                    let latest_selected_at = existing_targets.iter().map(|t| t.selected_at).max().unwrap_or(0);                                                   
                    let now_secs = Utc::now().timestamp();                                                                                                        
                    let retention_secs = *crate::config::CACHE_RETENTION_DAYS * 24 * 3600;                                                                        
                    if (now_secs - latest_selected_at) < retention_secs {                                                                                         
                        println!("[資料流 Step 0] 資料庫已有選股紀錄且未滿指定時間 ({} 天)，跳過選股 Pipeline 喵！", *crate::config::CACHE_RETENTION_DAYS);       
                        return Ok(());                                                                                                                            
                    }                                                                                                                                             
                }                                                                                                              
            // Step 1: 自資料庫載入可交易資產                                                                                                                      
             let assets = match mode {                                                                                                                                          
                SelectionMode::Sp500Only => {                                                                                                                                  
                    println!("[資料流 Step 1] 採用 S&P 500 模式選股...");                                                                                                      
                    db.load_sp500_assets().await?                                                                                                                              
                }                                                                                                                                                              
                SelectionMode::AllAssets => {                                                                                                                                  
                    println!("[資料流 Step 1] 採用全美股模式選股...");                                                                                                         
                    AlpacaClient::get_assets(client, db).await?                                                                                                                
                }                                                                                                                                                              
            };                                                                                              
            println!("[資料流 Step 1] 成功載入 {} 檔候選資產", assets.len());                                                                                      
                                                                                                                                                                   
            // Step 2: 前置過濾 (活躍、可交易、代碼長度 <= 5)                                                                                                      
            let filtered_assets: Vec<AlpacaAsset> = assets                                                                                                         
                .into_iter()                                                                                                                                       
                .filter(|a| a.status.eq_ignore_ascii_case("active") && a.tradable && a.symbol.len() <= 5)                                                          
                .collect();                                                                                                                                        
            println!("[資料流 Step 2] 前置過濾後剩餘 {} 檔優質標的", filtered_assets.len());                                                                       
                                                                                                                                                                   
            // Step 3: 平行化抓取近 30 天數據並評分                                                                                                                
            let now = Utc::now();                                                                                                                                  
            let start_30d = (now - chrono::Duration::days(30)).to_rfc3339();                                                                                       
            let max_concurrent = 50;                                                                                                                               
            let semaphore = Arc::new(Semaphore::new(max_concurrent));                                                                                              
                                                                                                                                                                   
            println!("[資料流 Step 3] 開啟 {} 併發池進行平行數據抓取與評分...", max_concurrent);                                                                   
                                                                                                                                                                   
            // 關鍵功能註解：平行化抓取資產數據並計算評分                                                                                                          
            let mut candidates_score: Vec<(AlpacaAsset, f64, f64)> = stream::iter(filtered_assets)                                                                 
                .map(|asset| {                                                                                                                                     
                    let sem = Arc::clone(&semaphore);                                                                                                              
                    let start = start_30d.clone();                                                                                                                 
                    async move {                                                                                                                                   
                        let _permit = sem.acquire().await.unwrap();                                                                                                
                        if let Ok(klines) = AlpacaClient::fetch_5m_data(client,asset.id, &asset.symbol, Some(&start), None).await {                                       
                            if klines.is_empty() {                                                                                                                 
                                return None;                                                                                                                       
                            }                                                                                                                                      
                            let total_dollar_volume: f64 = klines.iter().map(|k| k.close * k.volume as f64).sum();                                                 
                            let avg_dollar_volume = total_dollar_volume / klines.len() as f64;                                                                     
                                                                                                                                                                   
                            let first_close = klines.first().unwrap().close;                                                                                       
                            let last_close = klines.last().unwrap().close;                                                                                         
                            let perf_score = if first_close > 0.0 { (last_close - first_close) / first_close } else { 0.0 };                                       
                                                                                                                                                                   
                            Some((asset, avg_dollar_volume, perf_score))                                                                                           
                        } else {                                                                                                                                   
                            None                                                                                                                                   
                        }                                                                                                                                          
                    }                                                                                                                                              
                })                                                                                                                                                 
                .buffer_unordered(max_concurrent)                                                                                                                  
                .filter_map(|res| async move { res })                                                                                                              
                .collect()                                                                                                                                         
                .await;                                                                                                                                            
                                                                                                                                                                   
            // 進行權重加權與排序 (流動性 70%, 績效 30%)                                                                                                           
            let max_liq = candidates_score.iter().map(|x| x.1).fold(0.0, f64::max);                                                                                
            let max_perf = candidates_score.iter().map(|x| x.2).fold(0.0, f64::max);                                                                               
                                                                                                                                                                   
            candidates_score.sort_by(|a, b| {                                                                                                                      
                let score_a = if max_liq > 0.0 && max_perf > 0.0 { (a.1 / max_liq) * 0.7 + (a.2 / max_perf) * 0.3 } else { 0.0 };                                  
                let score_b = if max_liq > 0.0 && max_perf > 0.0 { (b.1 / max_liq) * 0.7 + (b.2 / max_perf) * 0.3 } else { 0.0 };                                  
                score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)                                                                                 
            });                                                                                                                                                    
                                                                                                                                                                   
            // 挑選前 30 檔交易標的                                                                                                                                
            let selected_trade_assets: Vec<AlpacaAsset> = candidates_score.into_iter().take(30).map(|x| x.0).collect();                                            
            let selected_at = Utc::now().timestamp();                                                                                                              
                                                                                                                                                                   
            let mut portfolio_targets = Vec::new();                                                                                                                
                                                                                                                                                                   
            for asset in &selected_trade_assets {                                                                                                                  
                portfolio_targets.push(PortfolioTarget {                                                                                                           
                    asset_id: asset.id,                                                                                                                            
                    symbol: asset.symbol.clone(),                                                                                                                  
                    weight: Decimal::ZERO,                                                                                                                         
                    target_type: TargetType::Trade,                                                                                                                
                    selected_at,                                                                                                                                   
                    status: TargetStatus::Active,                                                                                                                  
                });                                                                                                                                                
            }                                                                                                                                                      
                                                                                                                                                                   
            // Step 4: 寫入選出標的至投資組合資料表                                                                                                                
            db.save_portfolio_targets(&portfolio_targets).await?;                                                                                                  
            println!("[資料流 Step 4] 已將 {} 檔標的存入 Portfolio 資料庫", portfolio_targets.len());                                                              
                                                                                                                                                                   
            // Step 5: 抓取 2016-01-01 至今的 5 分鐘 K 線數據                                                                                                      
            let start_2016 = "2016-01-01T00:00:00Z";                                                                                                               
            println!("[資料流 Step 5] 開始抓取 2016 至今 5 分鐘 K 線歷史資料...");                                                                                 
                                                                                                                                                                   
            for target in &portfolio_targets {                                                                                                                     
                println!("正在回填歷史數據：{}", target.symbol);                                                                                                   
                match AlpacaClient::fetch_5m_data(client,target.asset_id, &target.symbol, Some(start_2016), None).await {                                                 
                    Ok(klines) => {                                                                                                                                
                        let count = db.save_klines(klines).await.map_err(|e| e.to_string())?;                                                                      
                        println!("成功儲存 {} 筆 {} 的歷史 K 線", count, target.symbol);                                                                           
                    }                                                                                                                                              
                    Err(e) => println!("抓取 {} 歷史數據失敗: {}", target.symbol, e),                                                                              
                }                                                                                                                                                  
            }                                                                                                                                                      
                                                                                                                                                                   
            Ok(())                                                                                                                                                 
        }          
        pub async fn backfill_portfolio_klines(    
            client: &reqwest::Client,                                                                                                               
            db: &MarketDatabase,                                                                                                                                  
            start_iso: Option<&str>,                                                                                                                              
        ) -> Result<(), Box<dyn std::error::Error>> {                                                                                                             
            let targets = db.load_portfolio_targets().await?;                                                                                                     
            println!("[補資料] 成功載入 {} 檔 Portfolio 標的進行 K 線回填...", targets.len());                                                                    
                                                                                                                                                                  
            let default_start = "2016-01-01T00:00:00Z";                                                                                                           
            let start_time = start_iso.unwrap_or(default_start);                                                                                                  
                                                                                                                                                                  
            for target in &targets {                                                                                                                              
                if target.target_type == TargetType::Cash {                                                                                                       
                    continue;                                                                                                                                     
                }                                                                                                                                                 
                println!("正在回填歷史數據：{}", target.symbol);                                                                                                  
                match AlpacaClient::fetch_5m_data(client,target.asset_id, &target.symbol, Some(start_time), None).await {                                                
                    Ok(klines) => {                                                                                                                               
                        let count = db.save_klines(klines).await.map_err(|e| e.to_string())?;                                                                     
                        println!("成功儲存 {} 筆 {} 的歷史 K 線", count, target.symbol);                                                                          
                    }                                                                                                                                             
                    Err(e) => println!("抓取 {} 歷史數據失敗: {}", target.symbol, e),                                                                             
                }                                                                                                                                                 
            }                                                                                                                                                     
                                                                                                                                                                  
            Ok(())                                                                                                                                                
        }                                                                                                                                                  
}