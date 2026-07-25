use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime,Datelike,Duration};

use chrono_tz::America::New_York;
use crate::config;
use serde::{Deserialize,Serialize};
use std::collections::{HashMap, BTreeSet,HashSet};
use sqlx::{any::AnyPoolOptions, AnyPool, Row};
use rust_decimal::{Decimal};
use uuid::Uuid;
#[derive(Debug, Deserialize)]
pub struct IdleConfigs {
    pub trash_talks: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize,sqlx::FromRow)]
pub struct AlpacaAsset {
    pub id: Uuid,
    pub symbol: String,
    pub name: Option<String>,
    pub exchange: String,
    #[serde(rename = "class")]
    pub asset_class: String,
    pub status: String,
    pub tradable: bool,
    pub shortable: bool,
    pub easy_to_borrow: bool,
}
#[derive(Deserialize)]
pub struct AppConfig {
    pub market: MarketConfig, // 你原本既有的欄位
    pub idle_configs: IdleConfigs, // 補上這個欄位
}

#[derive(Deserialize)]
pub struct MarketConfig {
    pub symbols: Vec<String>,
}
#[derive(Deserialize, Debug)]
pub struct AlpacaBarResponse {
    #[serde(rename = "bars", default)]
    bars: Vec<AlpacaBar>, // 直接定義為 Vec 
}

#[derive(Deserialize, Debug)]
pub struct AlpacaBar {
    #[serde(rename = "t")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "o")]
    open: f64,
    #[serde(rename = "h")]
    high: f64,
    #[serde(rename = "l")]
    low: f64,
    #[serde(rename = "c")]
    close: f64,
    #[serde(rename = "v")]
    volume: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSession {
    Regular,
    BeforeRegular,
    AfterRegular,
}

impl MarketSession {
    // 註解：將時段枚舉轉換為資料庫儲存整數
    pub fn to_int(self) -> i32 {
        match self {
            MarketSession::Regular => 0,
            MarketSession::BeforeRegular => 1,
            MarketSession::AfterRegular => 2,
        }
    }

    // 註解：將資料庫整數轉換回時段枚舉
    pub fn from_int(val: i32) -> Self {
        match val {
            1 => MarketSession::BeforeRegular,
            2 => MarketSession::AfterRegular,
            _ => MarketSession::Regular,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KLine {
    pub asset_id: Uuid,
    pub symbol: String,
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    pub session: MarketSession,
}

pub struct AlpacaClient;

impl AlpacaClient {
    pub async fn get_assets(
    client: &reqwest::Client,
    db: &MarketDatabase,
) -> Result<Vec<AlpacaAsset>, Box<dyn std::error::Error>> {
    // 關鍵功能註解：比對最新 updated_at 秒數，計算是否已超過 90 天
    let last_updated = db.get_latest_asset_updated_at().await?;
    let now_secs = chrono::Utc::now().timestamp();
    let three_months_secs = 90 * 24 * 3600;
    println!("成功抓取上次更新日");
    let is_expired = match last_updated {
        Some(updated_at) => (now_secs - updated_at) >= three_months_secs,
        None => true,
    };

    // 關鍵功能註解：未過期則直接載入 DB 資料，跳過 API 請求
    if !is_expired {
        let cached_assets = db.load_assets().await?;
        if !cached_assets.is_empty() {
            println!("[系統通知] 從資料庫載入快取的資產清單 (未滿 3 個月)");
            return Ok(cached_assets);
        }
    }

    // 關鍵功能註解：快取不存在或已過期，重新發起 Alpaca API 抓取
    println!("[系統通知] 資產快取過期或不存在，發起 Alpaca API 請求...");
    let url = format!("{}/v2/assets", &*config::BASE_URL);
    let response = client
        .get(&url)
        .header("APCA-API-KEY-ID", &*config::API_KEY)
        .header("APCA-API-SECRET-KEY", &*config::API_SECRET)
        .send()
        .await?;
    
    if !response.status().is_success() {
        let err_text = response.text().await?;
        return Err(format!("獲取資產表失敗: {}", err_text).into());
    }

    let pb = indicatif::ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.green} {msg}")?,
    );
    pb.set_message("資產抓取成功！正在處理與過濾數據中...");
    pb.enable_steady_tick(std::time::Duration::from_millis(80));

    let assets: Vec<AlpacaAsset> = response.json().await?;
    let active_assets: Vec<AlpacaAsset> = assets
        .into_iter()
        .filter(|a| a.status == "active" && a.tradable)
        .collect();

    // 關鍵功能註解：寫入或更新 DB 並呈現寫入筆數
    let saved_count = db.save_assets(&active_assets).await?;
    pb.finish_with_message(format!("[系統通知] 已更新並存入 {} 筆資產至資料庫", saved_count));

    Ok(active_assets)
}
    // 關鍵功能註解：向Alpaca請求K線並轉換為通用KLine結構
    pub async fn fetch_5m_data(client: &reqwest::Client,asset_id: Uuid,symbol: &str,start_iso: Option<&str>, 
        end_iso: Option<&str>) -> Result<Vec<KLine>, Box<dyn std::error::Error>> {
        let now = Utc::now();

        // 若未提供 end_iso，預設為當前 UTC 時間
        let mut end_dt = match end_iso {
            Some(e) => DateTime::parse_from_rfc3339(e)
                .map_err(|_| format!("end_iso 時間格式錯誤: '{}'，請使用 RFC3339 格式", e))?
                .with_timezone(&Utc),
            None => now,
        };

        // 2. 解析或設定預設 start 時間
        let start_dt = match start_iso {
            Some(s) => DateTime::parse_from_rfc3339(s)
                .map_err(|_| format!("start_iso 時間格式錯誤: '{}'，請使用 RFC3339 格式", s))?
                .with_timezone(&Utc),
            None => now - Duration::days(4),
        };
        // 防禦 1：邏輯倒置檢查 (start 比 end 還晚)
        if start_dt > end_dt {
            return Err(format!(
                "時間邏輯錯誤：起始時間 ({}) 不能晚於結束時間 ({}) 喵！",
                start_dt.to_rfc3339(),
                end_dt.to_rfc3339()
            ).into());
        }

        // 防禦 2：未來時間檢查 (start 時間直接超越現在)
        if start_dt > now {
            return Err(format!(
                "時間邏輯錯誤：起始時間 ({}) 為未來時間，不可超越當前時間 ({}) 喵！",
                start_dt.to_rfc3339(),
                now.to_rfc3339()
            ).into());
        }

        // 防禦 3：結束時間若超越現在，自動下修裁切為當前時間
        if end_dt > now {
            end_dt = now;
        }
        let start_str = start_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let end_str = end_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let url = format!(
            "https://data.alpaca.markets/v2/stocks/{}/bars?timeframe=5Min&start={}&end={}&limit=10000&feed=iex&sort=asc",
            symbol, start_str, end_str
        );
        
        let response = client.get(&url)
            .header("APCA-API-KEY-ID", &*config::API_KEY)
            .header("APCA-API-SECRET-KEY", &*config::API_SECRET)
            .send()
            .await?;
        if !response.status().is_success() {
        let err_text = response.text().await?;
        return Err(format!("API 請求失敗: {}", err_text).into());
     }
        let text = response.text().await?;
    
    // 關鍵功能註解：嘗試將原始回應反序列化並在失敗時印出偵錯資訊
        let parsed: AlpacaBarResponse = serde_json::from_str(&text).map_err(|e| {
            println!("偵錯 - 標的: {}, 解析失敗原因: {}", symbol, e);
            println!("偵錯 - 原始 API 回應內容: {}", text);
            e
        })?;
        let mut klines: Vec<KLine> = Vec::new();
        for b in parsed.bars {
        let ts = b.timestamp.timestamp();
        klines.push(KLine {
            asset_id: asset_id,
            symbol: symbol.to_string(),
            timestamp: ts,
            open: b.open,
            high: b.high,
            low: b.low,
            close: b.close,
            volume: b.volume,
            session: Preprocessor::determine_session(ts),
        });
    }
    Ok(klines)
    }
    pub fn is_market_window_open() -> bool {
    let now = Utc::now();
    
    // 關鍵功能註解：第一層過濾，週末（週六、週日）美股絕對休市
    if now.weekday() == chrono::Weekday::Sat || now.weekday() == chrono::Weekday::Sun {
        return false;
    }
    
    // 關鍵功能註解：第二層過濾，可限制在美股盤前至盤後交易時段內才放行 (UTC時間對齊)
    // 這裡可以依據你的策略需求（是否跑盤前外盤）動態調整小時區間
    let hour = now.hour();
    hour >= 8 && hour <= 22
}
}

pub struct Preprocessor;

impl Preprocessor {
    // 關鍵功能註解：依據紐約當地時間精準切分三大交易時段
    fn determine_session(timestamp: i64) -> MarketSession {
        if let Some(utc_dt) = Utc.timestamp_opt(timestamp, 0).single() {
            let ny_dt = utc_dt.with_timezone(&New_York);
            let current_time = ny_dt.time();

            let pre_market_start = NaiveTime::from_hms_opt(4, 0, 0).unwrap();//盤前開盤
            let regular_start = NaiveTime::from_hms_opt(9, 30, 0).unwrap(); //常規盤開盤 (盤前收盤)
            let regular_end = NaiveTime::from_hms_opt(16, 0, 0).unwrap(); //常規盤收盤 (盤前開盤)
            let post_market_end = NaiveTime::from_hms_opt(20, 0, 0).unwrap(); //盤後收盤 

            if current_time >= pre_market_start && current_time < regular_start {
                return MarketSession::BeforeRegular;
            } else if current_time >= regular_start && current_time < regular_end {
                return MarketSession::Regular;
            } else if current_time >= regular_end && current_time <= post_market_end {
                return MarketSession::AfterRegular;
            }
        }
        MarketSession::Regular
    }

    // 關鍵功能註解：過濾未完結或異常的髒K線資料
    fn clean_raw_klines(klines: Vec<KLine>) -> Vec<KLine> {
        klines.into_iter()
            .filter(|k| k.timestamp % 300 == 0)
            .map(|mut k| {
                k.session = Self::determine_session(k.timestamp);
                k
            })
            .collect()
    }

    // 關鍵功能註解：非同步對齊多檔標的時間軸並向前填充缺口
    pub async fn process_and_align(
        raw_portfolio: HashMap<Uuid, Vec<KLine>>
    ) -> Result<(Vec<i64>, HashMap<Uuid, Vec<KLine>>), String> {
        let mut cleaned_portfolio = HashMap::new();
        let mut all_timestamps = BTreeSet::new();

        for (asset_id, klines) in raw_portfolio {
            let cleaned = Self::clean_raw_klines(klines);
            for k in &cleaned {
                all_timestamps.insert(k.timestamp);
            }
            cleaned_portfolio.insert(asset_id, cleaned);
        }

        let union_timeline: Vec<i64> = all_timestamps.into_iter().collect();
        let mut aligned_portfolio = HashMap::new();

        for (asset_id, klines) in cleaned_portfolio {
            let sample_symbol = klines.first().map(|k| k.symbol.clone()).unwrap_or_default();
            let mut kline_map: HashMap<i64, KLine> = klines.into_iter().map(|k| (k.timestamp, k)).collect();
            let mut aligned_klines = Vec::new();
            let mut last_valid: Option<KLine> = None;

            for &ts in &union_timeline {
                if let Some(kline) = kline_map.remove(&ts) {
                    last_valid = Some(kline.clone());
                    aligned_klines.push(kline);
                } else if let Some(ref last) = last_valid {
                    let mut filled = last.clone();
                    filled.timestamp = ts;
                    filled.volume = 0;
                    filled.session = Self::determine_session(ts);
                    aligned_klines.push(filled);
                } else {
                    aligned_klines.push(KLine {
                        asset_id:asset_id,
                        symbol: sample_symbol.clone(),
                        timestamp: ts,
                        open: 0.0, high: 0.0, low: 0.0, close: 0.0, volume: 0,
                        session: Self::determine_session(ts),
                    });
                }
            }
            aligned_portfolio.insert(asset_id, aligned_klines);
        }

        Ok((union_timeline, aligned_portfolio))
    }
}

//=====================資料庫================================================//
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TargetType {
    Trade,     // 29 檔可交易標的
    Indicator, // VIX / SPY 大盤風向標 (不計入權重和)
    Cash,      // 保留現金 (符號固定為 "USD" 或 "CASH")
}

// 關鍵功能註解：標的狀態 Enum
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TargetStatus {
    Active,
    Blacklisted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioTarget {
    pub asset_id: Uuid,
    pub symbol: String,
    pub weight: Decimal, // 嚴格使用 Decimal 進行精確運算
    pub target_type: TargetType,
    pub selected_at: i64,
    pub status: TargetStatus,
}
pub struct MarketDatabase {
    pool: AnyPool,
}
impl MarketDatabase {
    // 關鍵功能註解：初始化資料庫並自動建立資料表防呆
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        sqlx::any::install_default_drivers();
        
        let db_url = &*config::DB_URL;

        let pool = AnyPoolOptions::new()
            .max_connections(5)
            .connect(db_url)
            .await?;
        /*
        ACTIVE：投資組合中有在追蹤的標的
        BLACKLISTED：投資組合中沒在被追蹤的標的
        +++++++
        TRADE：有在做交易的標的
        INDICATOR：參考指標(如：VIX)
        CASH：目前持有的現金
         */
        sqlx::query(
        "DO $$ BEGIN
            CREATE TYPE target_type_enum AS ENUM ('TRADE', 'INDICATOR', 'CASH'); 
            CREATE TYPE target_status_enum AS ENUM ('ACTIVE', 'BLACKLISTED');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;",
        ).execute(&pool).await?;
        // 1. 資產字典表 (必須先建立，因為 klines 與 portfolio_targets 都依賴它)
        sqlx::query(
                    "CREATE TABLE IF NOT EXISTS assets (
                id UUID PRIMARY KEY,                 -- Alpaca 原生 UUID 主鍵
                symbol VARCHAR(30) UNIQUE NOT NULL,  -- 股票代號（唯一且快速查詢）
                name TEXT,
                exchange VARCHAR(20) NOT NULL,
                asset_class VARCHAR(20) NOT NULL,   -- 對應 class
                status VARCHAR(20) NOT NULL,
                tradable BOOLEAN NOT NULL DEFAULT TRUE,
                shortable BOOLEAN NOT NULL DEFAULT FALSE,
                easy_to_borrow BOOLEAN NOT NULL DEFAULT FALSE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at BIGINT NOT NULL
            );",
        )
        .execute(&pool)
        .await?;
        sqlx::raw_sql(
            r#"
            CREATE INDEX IF NOT EXISTS idx_assets_symbol ON assets (symbol);
            CREATE INDEX IF NOT EXISTS idx_assets_tradable_active ON assets (is_active, tradable);
            "#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
        "CREATE TABLE IF NOT EXISTS klines (
            asset_id UUID NOT NULL,
            timestamp BIGINT NOT NULL,
            open NUMERIC(20,4) NOT NULL,
            high NUMERIC(20,4) NOT NULL,
            low NUMERIC(20,4) NOT NULL,
            close NUMERIC(20,4) NOT NULL,
            volume BIGINT NOT NULL,
            session INT NOT NULL,
            PRIMARY KEY (asset_id, timestamp),
            CONSTRAINT fk_klines_asset FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE RESTRICT ON UPDATE CASCADE
        );"
        ).execute(&pool).await?;

        // 關鍵功能註解：建立 K 線查詢索引提升 Symbol 時間範圍檢索效能
        sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_klines_asset_time ON klines(asset_id, timestamp DESC);"
        )
        .execute(&pool)
        .await?;
        // 2. 資產字典表 (關鍵功能註解：維護所有已知資產的主檔資訊)
        

        // 3. 投資組合選股與權重表 (關鍵功能註解：紀錄目前入選標的與目標權重)
        sqlx::query(
        "CREATE TABLE IF NOT EXISTS portfolio_targets (
            asset_id UUID PRIMARY KEY,
            symbol VARCHAR(30) NOT NULL,
            weight NUMERIC NOT NULL,
            target_type target_type_enum NOT NULL,
            selected_at BIGINT NOT NULL,
            status target_status_enum NOT NULL,
            CONSTRAINT fk_portfolio_asset FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE RESTRICT ON UPDATE CASCADE
        );"
        ).execute(&pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS positions (
                asset_id UUID PRIMARY KEY,
                symbol VARCHAR(30) NOT NULL,
                qty NUMERIC(18,9) NOT NULL,
                avg_entry_price NUMERIC(20,4) NOT NULL,
                current_price NUMERIC(20,4) NOT NULL,
                unrealized_pnl NUMERIC(20,4) NOT NULL,
                intraday_pnl NUMERIC(20,4) NOT NULL,
                updated_at BIGINT NOT NULL,
                CONSTRAINT fk_positions_asset FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE RESTRICT ON UPDATE CASCADE
            );"
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sp500_constituents (                                                                                                                
                asset_id UUID PRIMARY KEY,                                                                                                                                 
                symbol VARCHAR(30) NOT NULL,                                                                                                                               
                updated_at BIGINT NOT NULL,                                                                                                                                
                CONSTRAINT fk_sp500_asset FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE RESTRICT ON UPDATE CASCADE                                                
            );"
        )
        .execute(&pool)
        .await?;
    

        Ok(Self { pool })
    }
    //TODO:還沒找資料源
     pub async fn fetch_sp500_symbols(client: &reqwest::Client) -> Result<Vec<String>, Box<dyn std::error::Error>> {                                                            
        let url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv";                                                               
        let resp = client.get(url).send().await?.text().await?;                                                                                                                
        let mut symbols = Vec::new();                                                                                                                                          
        for (i, line) in resp.lines().enumerate() {                                                                                                                            
            if i == 0 { continue; }                                                                                                                                            
            if let Some(symbol) = line.split(',').next() {                                                                                                                     
                let trimmed = symbol.trim().replace('"', "");                                                                                                                  
                if !trimmed.is_empty() {                                                                                                                                       
                    symbols.push(trimmed);                                                                                                                                     
                }                                                                                                                                                              
            }                                                                                                                                                                  
        }                                                                                                                                                                      
        Ok(symbols)                                                                                                                                                            
    }
    pub async fn sync_sp500_constituents(                                                                                                                                  
            &self,                                                                                                                                                             
            raw_symbols: &[String],                                                                                                                                            
        ) -> Result<(), Box<dyn std::error::Error>> {                                                                                                                          
            let now = chrono::Utc::now().timestamp();                                                                                                                          
                                                                                                                                                                               
            // 1. 符號規範化：產生原始符號與連字號轉置符號                                                                                                                     
            let mut normalized_symbols: HashSet<String> = HashSet::new();                                                                                                      
            for s in raw_symbols {                                                                                                                                             
                let symbol_upper = s.trim().to_uppercase();                                                                                                                    
                normalized_symbols.insert(symbol_upper.clone());                                                                                                               
                normalized_symbols.insert(symbol_upper.replace('.', "-"));                                                                                                     
                normalized_symbols.insert(symbol_upper.replace('-', "."));                                                                                                     
            }                                                                                                                                                                  
                                                                                                                                                                               
            let symbol_vec: Vec<String> = normalized_symbols.into_iter().collect();                                                                                            
                                                                                                                                                                               
            if symbol_vec.is_empty() {                                                                                                                                         
                return Ok(());                                                                                                                                                 
            }                                                                                                                                                                  
                                                                                                                                                                               
            let placeholders = symbol_vec                                                                                                                                      
                .iter()                                                                                                                                                        
                .enumerate()                                                                                                                                                   
                .map(|(i, _)| format!("${}", i + 2))                                                                                                                           
                .collect::<Vec<_>>()                                                                                                                                           
                .join(",");                                                                                                                                                    
                                                                                                                                                                               
            let sql = format!(                                                                                                                                                 
                r#"                                                                                                                                                            
                INSERT INTO sp500_constituents (asset_id, symbol, updated_at)                                                                                                  
                SELECT id, symbol, $1                                                                                                                                          
                FROM assets                                                                                                                                                    
                WHERE symbol IN ({})                                                                                                                                           
                ON CONFLICT (asset_id)                                                                                                                                         
                DO UPDATE SET updated_at = EXCLUDED.updated_at                                                                                                                 
                "#,                                                                                                                                                            
                placeholders                                                                                                                                                   
            );                                                                                                                                                                 
                                                                                                                                                                               
            // 關鍵功能註解：動態構建占位符適配AnyPool資料庫                                                                                                                   
            let mut query = sqlx::query(&sql).bind(now);                                                                                                                       
            for s in &symbol_vec {                                                                                                                                             
                query = query.bind(s);                                                                                                                                         
            }                                                                                                                                                                  
                                                                                                                                                                               
            let rows_affected = query.execute(&self.pool).await?.rows_affected();                                                                                              
            println!("[S&P500 同步] 成功對齊並更新 {} 檔成分股至資料庫", rows_affected);                                                                                       
                                                                                                                                                                               
            // 關鍵功能註解：讀取資料庫對齊標的並進行防呆比對                                                                                                                  
            let matched_rows = sqlx::query("SELECT symbol FROM sp500_constituents")                                                                                            
                .fetch_all(&self.pool)                                                                                                                                         
                .await?;                                                                                                                                                       
                                                                                                                                                                               
            let matched_symbols: Vec<String> = matched_rows.iter().map(|r| r.get(0)).collect();                                                                                
            let matched_set: HashSet<String> = matched_symbols.into_iter().collect();                                                                                          
                                                                                                                                                                               
            let missing: Vec<&String> = raw_symbols                                                                                                                            
                .iter()                                                                                                                                                        
                .filter(|s| {                                                                                                                                                  
                    let u = s.to_uppercase();                                                                                                                                  
                    let alt1 = u.replace('.', "-");                                                                                                                            
                    let alt2 = u.replace('-', ".");                                                                                                                            
                    !matched_set.contains(&u) && !matched_set.contains(&alt1) && !matched_set.contains(&alt2)                                                                  
                })                                                                                                                                                             
                .collect();                                                                                                                                                    
                                                                                                                                                                               
            if !missing.is_empty() {                                                                                                                                           
                println!("[S&P500 告警] 共有 {} 檔標的未在 assets 主檔中對齊: {:?}", missing.len(), missing);                                                                  
            }                                                                                                                                                                  
                                                                                                                                                                               
            Ok(())                                                                                                                                                             
        }
        pub async fn load_sp500_assets(&self) -> Result<Vec<AlpacaAsset>, String> {                                                                                            
            let sql = r#"                                                                                                                                                      
                SELECT a.id::text, a.symbol, a.name, a.exchange, a.asset_class, a.status, a.tradable, a.shortable, a.easy_to_borrow                                            
                FROM assets a                                                                                                                                                  
                INNER JOIN sp500_constituents s ON a.id = s.asset_id                                                                                                           
            "#;                                                                                                                                                                
            let rows = sqlx::query(sql)                                                                                                                                        
                .fetch_all(&self.pool)                                                                                                                                         
                .await                                                                                                                                                         
                .map_err(|e| e.to_string())?;                                                                                                                                  
                                                                                                                                                                               
            let mut assets = Vec::new();                                                                                                                                       
            for row in rows {                                                                                                                                                  
                let id_str: String = row.get("id");                                                                                                                            
                let id = uuid::Uuid::parse_str(&id_str).map_err(|e| e.to_string())?;                                                                                           
                                                                                                                                                                               
                assets.push(AlpacaAsset {                                                                                                                                      
                    id,                                                                                                                                                        
                    symbol: row.get("symbol"),                                                                                                                                 
                    name: row.get("name"),                                                                                                                                     
                    exchange: row.get("exchange"),                                                                                                                             
                    asset_class: row.get("asset_class"),                                                                                                                       
                    status: row.get("status"),                                                                                                                                 
                    tradable: row.get("tradable"),                                                                                                                             
                    shortable: row.get("shortable"),                                                                                                                           
                    easy_to_borrow: row.get("easy_to_borrow"),                                                                                                                 
                });                                                                                                                                                            
            }                                                                                                                                                                  
                                                                                                                                                                               
            Ok(assets)                                                                                                                                                         
        }  
    // 關鍵功能註解：非同步寫入K線數據至本地資料庫
   pub async fn save_klines(&self, klines: Vec<KLine>) -> Result<usize, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let mut inserted = 0;

        for k in klines {
            let asset_id_str = k.asset_id.to_string();
            
            let res = sqlx::query(
                "INSERT INTO klines (asset_id, timestamp, open, high, low, close, volume, session) 
                 VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT(asset_id, timestamp) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume"
            )
            .bind(asset_id_str)
            .bind(k.timestamp)
            .bind(k.open)
            .bind(k.high)
            .bind(k.low)
            .bind(k.close)
            .bind(k.volume)
            .bind(k.session.to_int())
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            inserted += res.rows_affected() as usize;
        }

        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(inserted)
    }

    // 關鍵功能註解：非同步查詢對齊特定時間區間的K線數據
    pub async fn query_klines_by_range(
        &self,
        asset_id: Uuid,
        start: i64,
        end: i64,
    ) -> Result<Vec<KLine>, String> {
        let asset_id_str = asset_id.to_string();
        let rows = sqlx::query(
            "SELECT k.asset_id::uuid, a.symbol, k.timestamp, k.open, k.high, k.low, k.close, k.volume, k.session 
             FROM klines k
             JOIN assets a ON k.asset_id = a.id
             WHERE k.asset_id = $1 AND k.timestamp >= $2 AND k.timestamp <= $3
             ORDER BY k.timestamp ASC"
        )
        .bind(asset_id_str)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        let mut result = Vec::new();
        for row in rows {
            result.push(KLine {
                asset_id: row.get::<String, _>(0).parse().map_err(|e: uuid::Error| e.to_string())?,
                symbol: row.get(1),
                timestamp: row.get(2),
                open: row.get(3),
                high: row.get(4),
                low: row.get(5),
                close: row.get(6),
                volume: row.get(7),
                session: MarketSession::from_int(row.get(8)),
            });
        }

        Ok(result)
    }

    // 關鍵功能註解：列出資料庫中所有K線資料並印出狀態
    pub async fn list_all_klines(&self, limit_per_asset: Option<usize>) -> Result<(), String> {
        let limit = limit_per_asset.unwrap_or(1000) as i64;

        let assets_rows = sqlx::query(
            "SELECT DISTINCT k.asset_id, a.symbol 
             FROM klines k 
             JOIN assets a ON k.asset_id = a.id 
             ORDER BY a.symbol ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        for a_row in assets_rows {
            let asset_id_str: String = a_row.get(0);
            let asset_id: Uuid = asset_id_str.parse::<uuid::Uuid>().map_err(|e: uuid::Error| e.to_string())?;
            let symbol: String = a_row.get(1);

            let count_row = sqlx::query("SELECT COUNT(*) FROM klines WHERE asset_id = $1")
                .bind(&asset_id_str)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| e.to_string())?;

            let count: i64 = count_row.get(0);

            println!(
                "=== 標的: {} ({}) (資料庫內總計 {} 筆) ===",
                symbol, asset_id, count
            );

            let detail_rows = sqlx::query(
                "SELECT timestamp, open, high, low, close, volume, session
                 FROM klines 
                 WHERE asset_id = $1::uuid 
                 ORDER BY timestamp DESC 
                 LIMIT $2",
            )
            .bind(&asset_id_str)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

            for row in detail_rows {
                let ts: i64 = row.get(0);
                let o: f64 = row.get(1);
                let h: f64 = row.get(2);
                let l: f64 = row.get(3);
                let c: f64 = row.get(4);
                let v: i64 = row.get(5);
                let s_val: i32 = row.get(6);

                let datetime = Utc
                    .timestamp_opt(ts, 0)
                    .single()
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "Invalid Date".to_string());

                let session_str = match MarketSession::from_int(s_val) {
                    MarketSession::BeforeRegular => "盤前",
                    MarketSession::AfterRegular => "盤後",
                    MarketSession::Regular => "常規盤",
                };

                println!(
                    "[{}] 開: {:.2}, 高: {:.2}, 低: {:.2}, 收: {:.2}, 量: {}, 時段: {}",
                    datetime, o, h, l, c, v, session_str
                );
            }
            println!();
        }

        Ok(())
    }
    pub async fn load_portfolio_targets(&self) -> Result<Vec<PortfolioTarget>, String> {                                                                      
            let sql = r#"                                                                                                                                         
                SELECT asset_id::text, symbol, weight::text, target_type::text, selected_at, status::text                                                         
                FROM portfolio_targets                                                                                                                            
            "#;                                                                                                                                                   
            let rows = sqlx::query(sql)                                                                                                                           
                .fetch_all(&self.pool)                                                                                                                            
                .await                                                                                                                                            
                .map_err(|e| e.to_string())?;                                                                                                                     
                                                                                                                                                                  
            let mut targets = Vec::new();                                                                                                                         
            for row in rows {                                                                                                                                     
                let id_str: String = row.get("asset_id");                                                                                                         
                let asset_id = uuid::Uuid::parse_str(&id_str).map_err(|e| e.to_string())?;                                                                        
                let weight_str: String = row.get("weight");                                                                                                       
                let weight = weight_str.parse::<rust_decimal::Decimal>().map_err(|e| e.to_string())?;                                                             
                let target_type_str: String = row.get("target_type");                                                                                             
                let target_type = match target_type_str.as_str() {                                                                                                
                    "TRADE" => TargetType::Trade,                                                                                                                 
                    "INDICATOR" => TargetType::Indicator,                                                                                                         
                    "CASH" => TargetType::Cash,                                                                                                                   
                    _ => TargetType::Trade,                                                                                                                       
                };                                                                                                                                                
                let status_str: String = row.get("status");                                                                                                       
                let status = match status_str.as_str() {                                                                                                          
                    "ACTIVE" => TargetStatus::Active,                                                                                                             
                    "BLACKLISTED" => TargetStatus::Blacklisted,                                                                                                   
                    _ => TargetStatus::Active,                                                                                                                    
                };                                                                                                                                                
                let selected_at: i64 = row.get("selected_at");                                                                                                    
                                                                                                                                                                  
                targets.push(PortfolioTarget {                                                                                                                    
                    asset_id,                                                                                                                                     
                    symbol: row.get("symbol"),                                                                                                                    
                    weight,                                                                                                                                       
                    target_type,                                                                                                                                  
                    selected_at,                                                                                                                                  
                    status,                                                                                                                                       
                });                                                                                                                                               
            }                                                                                                                                                     
                                                                                                                                                                  
            Ok(targets)                                                                                                                                           
        }
    pub async fn get_latest_asset_updated_at(&self) -> Result<Option<i64>, String> {
        let row = sqlx::query("SELECT updated_at FROM assets ORDER BY updated_at DESC LIMIT 1")
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

    Ok(row.map(|r| r.get("updated_at")))
    }

    // 關鍵功能註解：從資料庫撈取所有現存的資產資料
        pub async fn load_assets(&self) -> Result<Vec<AlpacaAsset>, String> {
        // 關鍵功能註解：手動從 AnyRow 解構欄位並轉型，解決 Uuid 與 sqlx::Any 的 Decode 衝突
        let rows = sqlx::query("SELECT id::text, symbol, name, exchange, asset_class, status, tradable, shortable, easy_to_borrow FROM assets")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        let mut assets = Vec::new();
        for row in rows {
            let id_str: String = row.get("id");
            let id = uuid::Uuid::parse_str(&id_str).map_err(|e| e.to_string())?;

            assets.push(AlpacaAsset {
                id,
                symbol: row.get("symbol"),
                name: row.get("name"),
                exchange: row.get("exchange"),
                asset_class: row.get("asset_class"),
                status: row.get("status"),
                tradable: row.get("tradable"),
                shortable: row.get("shortable"),
                easy_to_borrow: row.get("easy_to_borrow"),
            });
        }

        Ok(assets)
    }
    pub async fn save_assets(&self, assets: &[AlpacaAsset]) -> Result<usize, String> {
    if assets.is_empty() {
        return Ok(0);
    }

    let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
    let now_secs = chrono::Utc::now().timestamp();
    let mut total_affected = 0;

    // 關鍵功能註解：每 500 筆切成一個批次，大幅減少與資料庫的往返次數
    for chunk in assets.chunks(500) {
        let mut query_builder = String::from(
            "INSERT INTO assets (id, symbol, name, exchange, asset_class, status, tradable, shortable, easy_to_borrow, is_active, updated_at) VALUES "
        );

        let mut query_params: Vec<String> = Vec::new();

        for (i, _) in chunk.iter().enumerate() {
            let offset = i * 11;
            query_params.push(format!(
                "(${}::uuid, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                offset + 1,  offset + 2,  offset + 3,  offset + 4,
                offset + 5,  offset + 6,  offset + 7,  offset + 8,
                offset + 9,  offset + 10, offset + 11
            ));
        }

        query_builder.push_str(&query_params.join(", "));
        query_builder.push_str(
            " ON CONFLICT(id) DO UPDATE SET
             symbol=excluded.symbol, name=excluded.name, exchange=excluded.exchange,
             asset_class=excluded.asset_class, status=excluded.status, tradable=excluded.tradable,
             shortable=excluded.shortable, easy_to_borrow=excluded.easy_to_borrow,
             is_active=excluded.is_active, updated_at=excluded.updated_at"
        );

        let mut query = sqlx::query(&query_builder);

        for a in chunk {
            query = query
                .bind(a.id.to_string())
                .bind(&a.symbol)
                .bind(&a.name)
                .bind(&a.exchange)
                .bind(&a.asset_class)
                .bind(&a.status)
                .bind(a.tradable)
                .bind(a.shortable)
                .bind(a.easy_to_borrow)
                .bind(true)
                .bind(now_secs);
        }

        let res = query.execute(&mut *tx).await.map_err(|e| e.to_string())?;
        total_affected += res.rows_affected() as usize;
    }

    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(total_affected)
}
    pub async fn save_portfolio_targets(                                                                                                                       
            &self,                                                                                                                                                 
            targets: &[PortfolioTarget],                                                                                                                           
        ) -> Result<usize, String> {                                                                                                                               
            let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;                                                                                      
            let mut count = 0;                                                                                                                                     
                                                                                                                                                                   
            for target in targets {                                                                                                                                
                let asset_id_str = target.asset_id.to_string();                                                                                                    
                let weight_str = target.weight.to_string();                                                                                                        
                                                                                                                                                                   
                let target_type_str = match target.target_type {                                                                                                   
                    TargetType::Trade => "TRADE",                                                                                                                  
                    TargetType::Indicator => "INDICATOR",                                                                                                          
                    TargetType::Cash => "CASH",                                                                                                                    
                };                                                                                                                                                 
                                                                                                                                                                   
                let status_str = match target.status {                                                                                                             
                    TargetStatus::Active => "ACTIVE",                                                                                                              
                    TargetStatus::Blacklisted => "BLACKLISTED",                                                                                                    
                };                                                                                                                                                 
                                                                                                                                                                   
                sqlx::query(                                                                                                                                       
                    "INSERT INTO portfolio_targets (asset_id, symbol, weight, target_type, selected_at, status)                                                    
                     VALUES ($1::uuid, $2, $3::numeric, $4::target_type_enum, $5, $6::target_status_enum)                                                          
                     ON CONFLICT(asset_id) DO UPDATE SET                                                                                                           
                     symbol=excluded.symbol,                                                                                                                       
                     weight=excluded.weight,                                                                                                                       
                     target_type=excluded.target_type,                                                                                                             
                     selected_at=excluded.selected_at,                                                                                                             
                     status=excluded.status"                                                                                                                       
                )                                                                                                                                                  
                .bind(asset_id_str)                                                                                                                                
                .bind(&target.symbol)                                                                                                                              
                .bind(weight_str)                                                                                                                                  
                .bind(target_type_str)                                                                                                                             
                .bind(target.selected_at)                                                                                                                          
                .bind(status_str)                                                                                                                                  
                .execute(&mut *tx)                                                                                                                                 
                .await                                                                                                                                             
                .map_err(|e| e.to_string())?;                                                                                                                      
                                                                                                                                                                   
                count += 1;                                                                                                                                        
            }                                                                                                                                                      
                                                                                                                                                                   
            tx.commit().await.map_err(|e| e.to_string())?;                                                                                                         
            Ok(count)                                                                                                                                              
        }
}