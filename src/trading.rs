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
