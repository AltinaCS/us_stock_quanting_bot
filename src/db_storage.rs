use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime,Datelike,Duration};

use chrono_tz::America::New_York;
use crate::config;
use serde::{Deserialize,Serialize};
use std::collections::{HashMap, BTreeSet};
use sqlx::{any::AnyPoolOptions, AnyPool, Row};
use rust_decimal::{Decimal};
#[derive(Debug, Deserialize)]
pub struct IdleConfigs {
    pub trash_talks: Vec<String>,
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
    symbol: String,
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    session: MarketSession,
}

pub struct AlpacaClient;

impl AlpacaClient {
    // 關鍵功能註解：向Alpaca請求K線並轉換為通用KLine結構
    pub async fn fetch_5m_data(symbol: &str,start_iso: Option<&str>, 
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
        let client = reqwest::Client::new();
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
    if hour >= 8 && hour <= 22 {
        return true;
    }
    
    false
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
            let regular_end = NaiveTime::from_hms_opt(16, 00, 0).unwrap(); //常規盤收盤 (盤前開盤)
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
        raw_portfolio: HashMap<String, Vec<KLine>>
    ) -> Result<(Vec<i64>, HashMap<String, Vec<KLine>>), String> {
        let mut cleaned_portfolio = HashMap::new();
        let mut all_timestamps = BTreeSet::new();

        for (symbol, klines) in raw_portfolio {
            let cleaned = Self::clean_raw_klines(klines);
            for k in &cleaned {
                all_timestamps.insert(k.timestamp);
            }
            cleaned_portfolio.insert(symbol, cleaned);
        }

        let union_timeline: Vec<i64> = all_timestamps.into_iter().collect();
        let mut aligned_portfolio = HashMap::new();

        for (symbol, klines) in cleaned_portfolio {
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
                        symbol: symbol.clone(),
                        timestamp: ts,
                        open: 0.0, high: 0.0, low: 0.0, close: 0.0, volume: 0,
                        session: Self::determine_session(ts),
                    });
                }
            }
            aligned_portfolio.insert(symbol, aligned_klines);
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
        sqlx::query(
        "DO $$ BEGIN
            CREATE TYPE target_type_enum AS ENUM ('TRADE', 'INDICATOR', 'CASH');
            CREATE TYPE target_status_enum AS ENUM ('ACTIVE', 'BLACKLISTED');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;"
        ).execute(&pool).await?;
        // 1. 資產字典表 (必須先建立，因為 klines 與 portfolio_targets 都依賴它)
        sqlx::query(
        "CREATE TABLE IF NOT EXISTS assets (
            symbol VARCHAR(30) PRIMARY KEY,
            name TEXT,
            exchange VARCHAR(20),
            asset_class VARCHAR(20),
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at BIGINT NOT NULL
        );"
        )
        .execute(&pool)
        .await?;
        sqlx::query(
        "CREATE TABLE IF NOT EXISTS klines (
            symbol VARCHAR(30) NOT NULL,
            timestamp BIGINT NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            volume BIGINT NOT NULL,
            session INT NOT NULL,
            PRIMARY KEY (symbol, timestamp),
            CONSTRAINT fk_klines_asset FOREIGN KEY (symbol) REFERENCES assets(symbol) ON DELETE RESTRICT ON UPDATE CASCADE
        );"
        ).execute(&pool).await?;

        // 關鍵功能註解：建立 K 線查詢索引提升 Symbol 時間範圍檢索效能
        sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_klines_symbol_time ON klines(symbol, timestamp DESC);"
        )
        .execute(&pool)
        .await?;
        // 2. 資產字典表 (關鍵功能註解：維護所有已知資產的主檔資訊)
        

        // 3. 投資組合選股與權重表 (關鍵功能註解：紀錄目前入選標的與目標權重)
        sqlx::query(
        "CREATE TABLE IF NOT EXISTS portfolio_targets (
            symbol VARCHAR(30) PRIMARY KEY,
            weight NUMERIC NOT NULL,
            target_type target_type_enum NOT NULL,
            selected_at BIGINT NOT NULL,
            status target_status_enum NOT NULL,
            CONSTRAINT fk_portfolio_asset FOREIGN KEY (symbol) REFERENCES assets(symbol) ON DELETE RESTRICT ON UPDATE CASCADE
        );"
        ).execute(&pool).await?;

        Ok(Self { pool })
    }

    // 關鍵功能註解：非同步寫入K線數據至本地資料庫
   pub async fn save_klines(&self, symbol: String, klines: Vec<KLine>) -> Result<usize, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let mut inserted = 0;

        for k in klines {
            let res = sqlx::query(
                "INSERT INTO klines (symbol, timestamp, open, high, low, close, volume, session) 
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT(symbol, timestamp) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume"
            )
            .bind(&symbol)
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
    pub async fn query_klines_by_range(&self, symbol: String, start: i64, end: i64) -> Result<Vec<KLine>, String> {
        let rows = sqlx::query(
            "SELECT symbol, timestamp, open, high, low, close, volume, session 
             FROM klines 
             WHERE symbol = $1 AND timestamp >= $2 AND timestamp <= $3
             ORDER BY timestamp ASC"
        )
        .bind(&symbol)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        let mut result = Vec::new();
        for row in rows {
            result.push(KLine {
                symbol: row.get(0),
                timestamp: row.get(1),
                open: row.get(2),
                high: row.get(3),
                low: row.get(4),
                close: row.get(5),
                volume: row.get(6),
                session: MarketSession::from_int(row.get(7)),
            });
        }

        Ok(result)
    }

    // 關鍵功能註解：列出資料庫中所有K線資料並印出狀態
    pub async fn list_all_klines(&self, limit_per_symbol: Option<usize>) -> Result<(), String> {
        let limit = limit_per_symbol.unwrap_or(1000) as i64;

        let symbols_rows = sqlx::query("SELECT DISTINCT symbol FROM klines ORDER BY symbol ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        for sym_row in symbols_rows {
            let symbol: String = sym_row.get(0);

            let count_row = sqlx::query("SELECT COUNT(*) FROM klines WHERE symbol = $1")
                .bind(&symbol)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| e.to_string())?;

            let count: i64 = count_row.get(0);

            println!("=== 標的: {} (資料庫內總計 {} 筆) ===", symbol, count);

            let detail_rows = sqlx::query(
                "SELECT timestamp, open, high, low, close, volume, session
                 FROM klines 
                 WHERE symbol = $1 
                 ORDER BY timestamp DESC 
                 LIMIT $2"
            )
            .bind(&symbol)
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

                let datetime = Utc.timestamp_opt(ts, 0)
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
}