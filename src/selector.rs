use std::collections::HashMap;                                                                                                                                 
use serde::{Deserialize, Serialize};                                                                                                                           
use crate::db_storage::{AlpacaAsset, AlpacaClient, KLine, MarketDatabase, PortfolioTarget, TargetStatus, TargetType};                                          
use crate::portfolio::PortfolioManager;                                                                                                                        
use chrono::{DateTime, Utc};                                                                                                                                   
use rust_decimal::Decimal;                                                                                                                                     
use rust_decimal_macros::dec;                                                                                                                                  
                                                                                                                                                                
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
                                                                                                                                                                
pub struct StockSelector;                                                                                                                                      
                                                                                                                                                                
impl StockSelector {                                                                                                                                           
    // 關鍵功能註解：依據流動性與績效評分並實施產業分散選股                                                                                                    
    pub fn select_stocks(                                                                                                                                      
        candidates: &[StockCandidate],                                                                                                                         
        config: &SelectorConfig,                                                                                                                               
    ) -> Result<Vec<String>, String> {                                                                                                                         
        if config.liquidity_weight + config.performance_weight <= 0.0 {                                                                                        
            return Err("權重總合必須大於零".to_string());                                                                                                      
        }                                                                                                                                                      
                                                                                                                                                                
        let indicator_count = config.indicator_symbols.len();                                                                                                  
        if indicator_count >= config.total_socket_slots {                                                                                                      
            return Err("風向標數量超過 Socket 總數上限".to_string());                                                                                          
        }                                                                                                                                                      
                                                                                                                                                                
        let target_trade_count = config.total_socket_slots - indicator_count;                                                                                  
                                                                                                                                                                
        let max_liq = candidates.iter().map(|c| c.avg_dollar_volume).fold(0.0, f64::max);                                                                      
        let max_perf = candidates.iter().map(|c| c.performance_score).fold(0.0, f64::max);                                                                     
                                                                                                                                                                
        let mut scored_candidates: Vec<(&StockCandidate, f64)> = candidates                                                                                    
            .iter()                                                                                                                                            
            .map(|c| {                                                                                                                                         
                let norm_liq = if max_liq > 0.0 { c.avg_dollar_volume / max_liq } else { 0.0 };                                                                
                let norm_perf = if max_perf > 0.0 { c.performance_score / max_perf } else { 0.0 };                                                             
                let score = (norm_liq * config.liquidity_weight) + (norm_perf * config.performance_weight);                                                    
                (c, score)                                                                                                                                     
            })                                                                                                                                                 
            .collect();                                                                                                                                        
                                                                                                                                                                
        scored_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));                                                          
                                                                                                                                                                
        let mut selected_symbols = Vec::new();                                                                                                                 
        let mut sector_counts: HashMap<String, usize> = HashMap::new();                                                                                        
                                                                                                                                                                
        for (candidate, _score) in scored_candidates {                                                                                                         
            if selected_symbols.len() >= target_trade_count {                                                                                                  
                break;                                                                                                                                         
            }                                                                                                                                                  
                                                                                                                                                                
            let count = sector_counts.entry(candidate.sector.clone()).or_insert(0);                                                                            
            if *count < config.max_per_sector {                                                                                                                
                selected_symbols.push(candidate.symbol.clone());                                                                                               
                *count += 1;                                                                                                                                   
            }                                                                                                                                                  
        }                                                                                                                                                      
                                                                                                                                                                
        let mut final_watchlist = config.indicator_symbols.clone();                                                                                            
        final_watchlist.extend(selected_symbols);                                                                                                              
                                                                                                                                                                
        Ok(final_watchlist)                                                                                                                                    
    }                                                                                                                                                          
} 
pub struct SelectionPipeline;                                                                                                                                  
                                                                                                                                                                
impl SelectionPipeline {                                                                                                                                       
    // 關鍵功能註解：執行自動選股與歷史數據回填全套資料流                                                                                                      
    pub async fn run_pipeline(                                                                                                                                 
        client: &reqwest::Client,                                                                                                                              
        db: &MarketDatabase,                                                                                                                                   
    ) -> Result<(), Box<dyn std::error::Error>> {                                                                                                              
        // Step 1: 自資料庫載入可交易資產                                                                                                                      
        let assets = AlpacaClient::get_assets(client, db).await?;                                                                                              
        println!("[資料流 Step 1] 成功載入 {} 檔候選資產", assets.len());                                                                                      
                                                                                                                                                                
        // Step 2: 抓取近 30 天數據並評分                                                                                                                      
        let mut candidates_score = Vec::new();                                                                                                                 
        let now = Utc::now();                                                                                                                                  
        let start_30d = (now - chrono::Duration::days(30)).to_rfc3339();                                                                                       
                                                                                                                                                                
        for asset in &assets {                                                                                                                                 
            if let Ok(klines) = AlpacaClient::fetch_5m_data(asset.id, &asset.symbol, Some(&start_30d), None).await {                                           
                if klines.is_empty() {                                                                                                                         
                    continue;                                                                                                                                  
                }                                                                                                                                              
                                                                                                                                                                
                let total_dollar_volume: f64 = klines.iter().map(|k| k.close * k.volume as f64).sum();                                                         
                let avg_dollar_volume = total_dollar_volume / klines.len() as f64;                                                                             
                                                                                                                                                                
                let first_close = klines.first().unwrap().close;                                                                                               
                let last_close = klines.last().unwrap().close;                                                                                                 
                let perf_score = if first_close > 0.0 { (last_close - first_close) / first_close } else { 0.0 };                                               
                                                                                                                                                                
                candidates_score.push((asset.clone(), avg_dollar_volume, perf_score));                                                                         
            }                                                                                                                                                  
        }                                                                                                                                                      
                                                                                                                                                                
        // 進行權重加權與排序 (流動性 70%, 績效 30%)                                                                                                           
        let max_liq = candidates_score.iter().map(|x| x.1).fold(0.0, f64::max);                                                                                
        let max_perf = candidates_score.iter().map(|x| x.2).fold(0.0, f64::max);                                                                               
                                                                                                                                                                
        candidates_score.sort_by(|a, b| {                                                                                                                      
            let score_a = (a.1 / max_liq) * 0.7 + (a.2 / max_perf) * 0.3;                                                                                      
            let score_b = (b.1 / max_liq) * 0.7 + (b.2 / max_perf) * 0.3;                                                                                      
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)                                                                                 
        });                                                                                                                                                    
                                                                                                                                                                
        // 挑選前 29 檔交易標的與 1 檔風向標                                                                                                                   
        let selected_trade_assets: Vec<AlpacaAsset> = candidates_score.into_iter().take(29).map(|x| x.0).collect();                                            
        let selected_at = Utc::now().timestamp();                                                                                                              
                                                                                                                                                                
        let mut portfolio_targets = Vec::new();                                                                                                                
        let equal_weight = Decimal::from_str_exact("0.034").unwrap();                                                                                          
                                                                                                                                                                
        for asset in &selected_trade_assets {                                                                                                                  
            portfolio_targets.push(PortfolioTarget {                                                                                                           
                asset_id: asset.id,                                                                                                                            
                symbol: asset.symbol.clone(),                                                                                                                  
                weight: equal_weight,                                                                                                                          
                target_type: TargetType::Trade,                                                                                                                
                selected_at,                                                                                                                                   
                status: TargetStatus::Active,                                                                                                                  
            });                                                                                                                                                
        }                                                                                                                                                      
                                                                                                                                                                
        // Step 3: 寫入選出標的至投資組合資料表                                                                                                                
        // 關鍵功能註解：更新投資組合資料庫紀錄                                                                                                                
        db.save_portfolio_targets(&portfolio_targets).await?;                                                                                                  
        println!("[資料流 Step 3] 已將 {} 檔標的存入 Portfolio 資料庫", portfolio_targets.len());                                                              
                                                                                                                                                                
        // Step 4: 抓取 2016-01-01 至今的 5 分鐘 K 線數據                                                                                                      
        let start_2016 = "2016-01-01T00:00:00Z";                                                                                                               
        println!("[資料流 Step 4] 開始抓取 2016 至今 5 分鐘 K 線歷史資料...");                                                                                 
                                                                                                                                                                
        for target in &portfolio_targets {                                                                                                                     
            println!("正在回填歷史數據：{}", target.symbol);                                                                                                   
            match AlpacaClient::fetch_5m_data(target.asset_id, &target.symbol, Some(start_2016), None).await {                                                 
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