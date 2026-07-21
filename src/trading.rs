use rust_decimal_macros::dec;
use rust_decimal;
use rust_decimal::Decimal;
use serde::{Serialize,Deserialize};
use reqwest::{Client, header::{HeaderMap, HeaderValue}};
use std::sync::OnceLock;
use crate::config;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use indicatif::{ProgressBar, ProgressStyle};
#[derive(Debug, Deserialize)]
pub struct Account {
    // 註解：總資產淨值（持倉市值 + 現金），計算動態權重的分母
    #[serde(rename = "equity")]
    pub total_equity: Decimal,
    
    // 註解：當前可用購買力
    pub buying_power: Decimal,
    
    // 註解：純現金餘額
    pub cash: Decimal,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaAsset {
    pub id: String,
    pub class: String,
    pub exchange: String,
    pub symbol: String,
    pub name: String,
    pub status: String,
    pub tradable: bool,
    pub shortable: bool,
    pub easy_to_borrow: bool,
}
#[derive(Debug, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub qty: Decimal,
    pub avg_entry_price: Decimal,
    pub current_price: Decimal,
    
    // 註解：利用 serde 將 Alpaca 的 pl 欄位精準映射至我們定義的變數
    #[serde(rename = "unrealized_pl")]
    pub unrealized_pnl: Decimal,
    
    // 註解：如果你想更細分當日損益，也可以選擇映射這個欄位
    #[serde(rename = "unrealized_intraday_pl")]
    pub intraday_pnl: Decimal,
}
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
// 註解：定義Alpaca支援的訂單類型
pub enum OrderType {
    Market,
    Limit,
}
#[derive(Debug, Clone, Copy)]
// 註解：外部呼叫使用的強型別訂單枚舉
pub enum OrderTypeInput {
    Market,
    Limit(Decimal),
}
#[derive(Debug, Clone, Copy)]
// 註解：定義交易計算方法（使用XOR枚舉避免同時傳入）
pub enum OrderMethod {
    Qty(Decimal),
    Notional(Decimal),
}
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeInForce {
    Day,
    Gtc,
    Opg,
    Cls,
    Ioc,
    Fok,
}
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Serialize, Deserialize)]
pub struct AssetsCache {
    pub updated_at: u64, // UNIX 時間戳記 (秒)
    pub assets: Vec<AlpacaAsset>,
}
// 關鍵功能：支援動態單類與XOR交易計算方法的通用下單函式
/// 關鍵功能：原生支援Decimal運算與自動格式化字串的通用下單函式
pub async fn place_order(
    client: &reqwest::Client,
    symbol: &str,
    side: Side,
    order_type_input: OrderTypeInput,
    mut tif: TimeInForce,
    method: OrderMethod,
    extended_hours: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    
    // 註解：依據輸入類型解構出API需要的類型與價格
    let (mut order_type, limit_price) = match order_type_input {
        OrderTypeInput::Market => (OrderType::Market, None),
        OrderTypeInput::Limit(price) => (OrderType::Limit, Some(price)),
    };

    // 註解：若啟動盤外交易則強制修正訂單類型為限價單
    if extended_hours {
        if let OrderType::Market = order_type {
            order_type = OrderType::Limit;
        }
    }

    // 1. 利用 Match 解構並依據碎股邏輯安全修正 TimeInForce
    let (method_key, method_val) = match method {
        OrderMethod::Notional(n) => {
            tif = TimeInForce::Day; 
            ("notional", n.to_string())
        }
        OrderMethod::Qty(q) => {
            // 註解：若有小數點（碎股）則強制約束效期為Day
            if q.scale() > 0 {
                tif = TimeInForce::Day;
            }
            ("qty", q.to_string())
        }
    };

    // 2. 組裝完全強型別映射的 JSON 欄位
    let mut order_body = serde_json::json!({
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "time_in_force": tif,
        "extended_hours": extended_hours
    });
    order_body[method_key] = serde_json::json!(method_val);

    // 註解：若為限價單則動態寫入限價價格欄位
    if let OrderType::Limit = order_type {
        if let Some(price) = limit_price {
            order_body["limit_price"] = serde_json::json!(price.to_string());
        }
    }

    let response = client
        .post(format!("{}/v2/orders", &*config::BASE_URL))
        .header("APCA-API-KEY-ID", &*config::API_KEY)
        .header("APCA-API-SECRET-KEY", &*config::API_SECRET)
        .json(&order_body)
        .send()
        .await?;

    let json: serde_json::Value = response.json().await?;
    let order_id = json["id"].as_str().unwrap_or("").to_string();
    
    println!("--- 送出結果 ({}) ---", symbol);
    println!("{}", serde_json::to_string_pretty(&json)?);
    
    Ok(order_id)
}
pub async fn get_orders(client: &reqwest::Client, order_id: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    // 根據是否有ID決定URL，None就是撈取全部掛單
    let url = match order_id {
        Some(id) => format!("{}/v2/orders/{}", &*config::BASE_URL, id),
        None => format!("{}/v2/orders", &*config::BASE_URL),
    };

    let response = client
        .get(&url)
        .header("APCA-API-KEY-ID", &*config::API_KEY)
        .header("APCA-API-SECRET-KEY",&* config::API_SECRET)
        .send()
        .await?;

    let json: serde_json::Value = response.json().await?;
    println!("--- 查詢結果 (全選: {}) ---", order_id.is_none());
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

// 關鍵功能：支援取消單筆或全選取消所有掛單
pub async fn cancel_orders(client: &reqwest::Client, order_id: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    // None時直接對/v2/orders發射DELETE就是大清空
    let url = match order_id {
        Some(id) => format!("{}/v2/orders/{}", &*config::BASE_URL, id),
        None => format!("{}/v2/orders", &*config::BASE_URL),
    };

    let response = client
        .delete(&url)
        .header("APCA-API-KEY-ID", &*config::API_KEY)
        .header("APCA-API-SECRET-KEY", &*config::API_SECRET)
        .send()
        .await?;

    if response.status().is_success() {
        match order_id {
            Some(id) => println!("成功取消單筆訂單：{}", id),
            None => println!("成功大清空！所有未實現掛單皆已取消喵！"),
        }
    } else {
        println!("操作失敗，狀態碼：{}", response.status());
    }
    Ok(())
}
// 關鍵功能：向 Alpaca 請求當前所有持倉的非同步函式
pub async fn get_positions(
    client: &reqwest::Client,
) -> Result<Vec<Position>, Box<dyn std::error::Error>> {
    let url = format!("{}/v2/positions", &*config::BASE_URL);

    // 1. 發送 GET 請求獲取持倉列表
    let response = client
        .get(url)
        .header("APCA-API-KEY-ID", &*config::API_KEY)
        .header("APCA-API-SECRET-KEY", &*config::API_SECRET)
        .send()
        .await?;

    // 2. 處理錯誤狀態碼
    if !response.status().is_success() {
        let err_text = response.text().await?;
        return Err(format!("獲取持倉失敗: {}", err_text).into());
    }

    // 3. 直接反序列化成高精度的 Vec<Position> 向量
    let positions: Vec<Position> = response.json().await?;
    
    Ok(positions)
}
pub async fn get_account(
    client: &reqwest::Client,
) -> Result<Account, Box<dyn std::error::Error>> {
    let url = format!("{}/v2/account", &*config::BASE_URL);

    let response = client
        .get(url)
        .header("APCA-API-KEY-ID", &*config::API_KEY)
        .header("APCA-API-SECRET-KEY", &*config::API_SECRET)
        .send()
        .await?;
   
    if !response.status().is_success() {
        let err_text = response.text().await?;
        return Err(format!("獲取帳戶失敗: {}", err_text).into());
    }

    let account: Account = response.json().await?;
    Ok(account)
}
pub async fn get_assets(
    client: &reqwest::Client,
) -> Result<Vec<AlpacaAsset>, Box<dyn std::error::Error>> {
    let cache_path = Path::new("assets_cache.json");
    let three_months_secs: u64 = 90 * 24 * 60 * 60; // 90 天的秒數
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();

    // 1. 檢查快取是否存在且未過期
    if cache_path.exists() {
        if let Ok(json_str) = fs::read_to_string(cache_path) {
            if let Ok(cache) = serde_json::from_str::<AssetsCache>(&json_str) {
                let age_secs = now_secs.saturating_sub(cache.updated_at);
                
                if age_secs < three_months_secs {
                    let days_left = (three_months_secs - age_secs) / 86400;
                    println!("[系統通知] 載入本地資產快取（剩餘有效期限：約 {} 天）", days_left);
                    return Ok(cache.assets);
                } else {
                    println!("[系統通知] 本地資產快取已超過 3 個月（已過期），準備更新...");
                }
            }
        }
    }
    else{
        println!("[系統通知] 本地無快取，正在從 Alpaca API 抓取龐大資產清單...");
    }

    let url = format!("{}/v2/assets", &*config::BASE_URL);
    // 這裡放你原本去 client.get(...) 戳 Alpaca API 的發送邏輯
    let response = client.get(&url)
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

    let cache_path_buf = cache_path.to_path_buf();
    let active_assets = tokio::task::spawn_blocking(move || -> Result<Vec<AlpacaAsset>, Box<dyn std::error::Error + Send + Sync>> {
        let active_assets: Vec<AlpacaAsset> = assets
            .into_iter()
            .filter(|a| a.status == "active" && a.tradable)
            .collect();

        let new_cache = AssetsCache {
            updated_at: now_secs,
            assets: active_assets.clone(),
        };

        let serialized = serde_json::to_string_pretty(&new_cache)?;
        fs::write(cache_path_buf, serialized)?;
        Ok(active_assets)
    }).await?
    .map_err(|e| e.to_string())?;

    pb.finish_with_message("[系統通知] 資產清單已成功持久化至 assets_cache.json");

    Ok(active_assets)
}