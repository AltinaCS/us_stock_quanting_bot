use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime};
use chrono_tz::America::New_York;
use quant_bot::trading::{place_order, OrderMethod, OrderTypeInput, TimeInForce, Side};
use quant_bot::config;
use rust_decimal_macros::dec;
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeSet};
use std::fs;
#[derive(Deserialize)]
struct AppConfig {
    market: MarketConfig,
}

#[derive(Deserialize)]
struct MarketConfig {
    symbols: Vec<String>,
}
#[derive(Deserialize, Debug)]
struct AlpacaBarResponse {
    #[serde(rename = "bars")]
    bars: Vec<AlpacaBar>, // 直接定義為 Vec 而非 HashMap
}

#[derive(Deserialize, Debug)]
struct AlpacaBar {
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
struct KLine {
    symbol: String,
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    session: MarketSession,
}

struct AlpacaClient;

impl AlpacaClient {
    // 關鍵功能註解：向Alpaca請求K線並轉換為通用KLine結構
    pub async fn fetch_5m_data(symbol: &str) -> Result<Vec<KLine>, Box<dyn std::error::Error>> {
        let url = format!(
        "https://data.alpaca.markets/v2/stocks/{}/bars?timeframe=5Min&limit=1000",
        symbol
    );
        let client = reqwest::Client::new();
        let response = client.get(&url)
            .header("APCA-API-KEY-ID", config::API_KEY.get().unwrap())
            .header("APCA-API-SECRET-KEY", config::API_SECRET.get().unwrap())
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
        let mut klines = Vec::new();
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
}

struct Preprocessor;

impl Preprocessor {
    // 關鍵功能註解：依據紐約當地時間精準切分三大交易時段
    fn determine_session(timestamp: i64) -> MarketSession {
        if let Some(utc_dt) = Utc.timestamp_opt(timestamp, 0).single() {
            let ny_dt = utc_dt.with_timezone(&New_York);
            let current_time = ny_dt.time();

            let pre_market_start = NaiveTime::from_hms_opt(4, 0, 0).unwrap();
            let regular_start = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
            let regular_end = NaiveTime::from_hms_opt(16, 0, 0).unwrap();
            let post_market_end = NaiveTime::from_hms_opt(20, 0, 0).unwrap();

            if current_time >= pre_market_start && current_time < regular_start {
                return MarketSession::BeforeRegular;
            } else if current_time >= regular_start && current_time <= regular_end {
                return MarketSession::Regular;
            } else if current_time > regular_end && current_time <= post_market_end {
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

struct MarketDatabase {
    conn: Arc<Mutex<Connection>>,
}

impl MarketDatabase {
    // 關鍵功能註解：初始化資料庫並自動建立資料表防呆
    fn new(db_path: &str) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS klines (
                symbol TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                session INTEGER,
                PRIMARY KEY (symbol, timestamp)
            )",
            [],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    // 關鍵功能註解：非同步寫入K線數據至本地資料庫
    async fn save_klines(&self, symbol: String, klines: Vec<KLine>) -> Result<usize, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut conn_guard = conn.lock().map_err(|e| e.to_string())?;
            let tx = conn_guard.transaction().map_err(|e| e.to_string())?;
            let mut inserted = 0;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO klines (symbol, timestamp, open, high, low, close, volume, session) 
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        ON CONFLICT(symbol, timestamp) DO UPDATE SET
                        open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume"
                ).map_err(|e| e.to_string())?;

                for k in klines {
                    let affected = stmt.execute(params![
                        symbol,
                        k.timestamp,
                        k.open,
                        k.high,
                        k.low,
                        k.close,
                        k.volume,
                        k.session.to_int()
                    ]).map_err(|e| e.to_string())?;
                    inserted += affected;
                }
            }
            tx.commit().map_err(|e| e.to_string())?;
            Ok(inserted)
        }).await.map_err(|e| e.to_string())?
    }

    // 關鍵功能註解：非同步查詢對齊特定時間區間的K線數據
    async fn query_klines_by_range(&self, symbol: String, start: i64, end: i64) -> Result<Vec<KLine>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().map_err(|e| e.to_string())?;
            let mut stmt = conn_guard.prepare(
                "SELECT symbol, timestamp, open, high, low, close, volume, session 
                 FROM klines 
                 WHERE symbol = ?1 AND timestamp >= ?2 AND timestamp <= ?3
                 ORDER BY timestamp ASC"
            ).map_err(|e| e.to_string())?;

            let kline_iter = stmt.query_map(params![symbol, start, end], |row| {
                Ok(KLine {
                    symbol: row.get(0)?,
                    timestamp: row.get(1)?,
                    open: row.get(2)?,
                    high: row.get(3)?,
                    low: row.get(4)?,
                    close: row.get(5)?,
                    volume: row.get(6)?,
                    session: MarketSession::from_int(row.get(7)?),
                })
            }).map_err(|e| e.to_string())?;

            let mut result = Vec::new();
            for k in kline_iter {
                result.push(k.map_err(|e| e.to_string())?);
            }
            Ok(result)
        }).await.map_err(|e| e.to_string())?
    }

    // 關鍵功能註解：列出資料庫中所有K線資料並印出狀態
    async fn list_all_klines(&self, limit_per_symbol: Option<usize>) -> Result<(), String> {
        let conn = Arc::clone(&self.conn);
        let limit = limit_per_symbol.unwrap_or(1000);
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().map_err(|e| e.to_string())?;
            
            let mut stmt_symbols = conn_guard.prepare(
                "SELECT DISTINCT symbol FROM klines ORDER BY symbol ASC"
            ).map_err(|e| e.to_string())?;
            
            let symbol_iter = stmt_symbols.query_map([], |row| {
                row.get::<_, String>(0)
            }).map_err(|e| e.to_string())?;

            for symbol_res in symbol_iter {
                let symbol = symbol_res.map_err(|e| e.to_string())?;
                
                let count: i64 = conn_guard.query_row(
                    "SELECT COUNT(*) FROM klines WHERE symbol = ?1",
                    params![symbol],
                    |row| row.get(0),
                ).map_err(|e| e.to_string())?;
                
                println!("=== 標的: {} (資料庫內總計 {} 筆) ===", symbol, count);

                let mut stmt_detail = conn_guard.prepare(
                    "SELECT timestamp, open, high, low, close, volume, session
                     FROM klines 
                     WHERE symbol = ?1 
                     ORDER BY timestamp DESC 
                     LIMIT ?2"
                ).map_err(|e| e.to_string())?;

                let kline_iter = stmt_detail.query_map(params![symbol, limit], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, f64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i32>(6)?,
                    ))
                }).map_err(|e| e.to_string())?;

                for kline_res in kline_iter {
                    let (ts, o, h, l, c, v, s_val) = kline_res.map_err(|e| e.to_string())?;
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
        }).await.map_err(|e| e.to_string())?
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    config::init_config();
    let client = reqwest::Client::new();
    
    // 關鍵功能註解：讀取並解析本地設定檔以取得標的列表
    let config_str = fs::read_to_string("config.toml")?;
    let app_config: AppConfig = toml::from_str(&config_str)?;
    
    // 關鍵功能註解：初始化本地資料庫準備寫入歷史K線
    let db = MarketDatabase::new("market_data.db")?;
    let mut raw_portfolio = std::collections::HashMap::new();

    // 關鍵功能註解：遍歷標的列表並向Alpaca非同步抓取資料
    for symbol in &app_config.market.symbols {
        println!("正在抓取 {} 的資料...", symbol);
        match AlpacaClient::fetch_5m_data(symbol).await {
            Ok(klines) => {
                raw_portfolio.insert(symbol.clone(), klines);
            },
            Err(e) => println!("抓取 {} 失敗: {}", symbol, e),
        }
    }

    // 關鍵功能註解：對齊多檔標的的時間軸並填補停牌缺口
    let (_timeline, aligned_portfolio) = Preprocessor::process_and_align(raw_portfolio).await?;

    // 關鍵功能註解：將對齊後的乾淨資料寫入資料庫中
    for (symbol, klines) in aligned_portfolio {
        let count = db.save_klines(symbol.clone(), klines).await?;
        println!("已將 {} 筆 {} 的K線寫入資料庫", count, symbol);
    }
    db.list_all_klines(None).await?;
    Ok(())
    
}