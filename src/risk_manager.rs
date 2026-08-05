use rust_decimal::Decimal;
use std::collections::HashMap;
use crate::db_storage::MarketDatabase;
use crate::trading;
use crate::backtest::AnalysisResult;
use crate::trading::{Account,Position,OrderMethod,OrderType,OrderTypeInput,Side,TimeInForce};
use crate::config;
use rust_decimal::prelude::ToPrimitive;
use tracing::{info,warn,error,debug};
use tokio::sync::mpsc;
use futures_util::StreamExt; // 關鍵功能註解：導入 StreamExt 以使用 split() 與 next() 方法
use futures_util::SinkExt;   // 若上方 send 還有用到，也可一併確認導入
/// 風控設定參數

#[derive(Debug, Clone)]
pub struct BarUpdate {
    pub symbol: String,
    pub close: Decimal,
    pub high: Decimal,
    pub low: Decimal,
}

// 關鍵功能註解：背景 WebSocket 監聽任務 (以 Alpaca WebSocket 為例)
pub async fn start_websocket_listener(
    symbols: Vec<String>,
    tx: mpsc::Sender<BarUpdate>,
) -> anyhow::Result<()>  {
    // 關鍵功能註解：請根據實際採用的交易所或行情源替換 Ws Url 與 認證 Token 實盤交易把iex換成sip
    let url = "wss://stream.data.alpaca.markets/v2/iex";
    let (ws_stream, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    // 關鍵功能註解：傳送認證訊息與訂閱頻道 (訂閱傳入的持倉標的)
    let auth_msg = serde_json::json!({
        "action": "auth",
        "key": &*config::API_KEY,
        "secret": &*config::API_SECRET
    });
    futures_util::SinkExt::send(&mut write, tokio_tungstenite::tungstenite::Message::Text(auth_msg.to_string().into())).await?;

    let sub_msg = serde_json::json!({
        "action": "subscribe",
        "bars": symbols
    });
    futures_util::SinkExt::send(&mut write, tokio_tungstenite::tungstenite::Message::Text(sub_msg.to_string().into())).await?;

    // 關鍵功能註解：持續接收訊息並解析為 BarUpdate 後經由 Channel 發送
    while let Some(msg) = futures_util::StreamExt::next(&mut read).await {
        if let Ok(tokio_tungstenite::tungstenite::Message::Text(text)) = msg {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(arr) = v.as_array() {
                    for item in arr {
                        if item["T"] == "b" {
                            let bar = BarUpdate {
                                symbol: item["S"].as_str().unwrap_or_default().to_string(),
                                close: Decimal::from_f64_retain(item["c"].as_f64().unwrap_or(0.0)).unwrap_or(Decimal::ZERO),
                                high: Decimal::from_f64_retain(item["h"].as_f64().unwrap_or(0.0)).unwrap_or(Decimal::ZERO),
                                low: Decimal::from_f64_retain(item["l"].as_f64().unwrap_or(0.0)).unwrap_or(Decimal::ZERO),
                            };
                            if tx.send(bar).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub max_day_leverage: Decimal,
    pub max_overnight_leverage: Decimal,
    pub max_single_weight: Decimal,
    pub min_rebalance_threshold: Decimal,
    pub stop_loss_pct: Decimal,                   // 硬性停損 (例如 5%)
    pub take_profit_pct: Option<Decimal>,         // 改為 Option，使用 pct_b 時可傳 None
    pub trailing_stop_pct: Option<Decimal>,       // 移動停損
}
impl RiskConfig{
    pub fn validate(&self) {
        let max_day_limit = Decimal::from(4);
        let max_overnight_limit = Decimal::from(2);

        assert!(
            self.max_day_leverage <= max_day_limit,
            "單日槓桿上限不可超過 4.0 (當前設定: {})",
            self.max_day_leverage
        );

        assert!(
            self.max_overnight_leverage <= max_overnight_limit,
            "隔夜槓桿上限不可超過 2.0 (當前設定: {})",
            self.max_overnight_leverage
        );
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_day_leverage: Decimal::ONE,
            max_overnight_leverage: Decimal::ONE,
            max_single_weight: Decimal::new(25, 2),
            min_rebalance_threshold: Decimal::new(1, 2),
            stop_loss_pct: Decimal::new(5, 2),
            take_profit_pct: None,                // 預設不使用固定%停利，交給 pct_b 等策略邏輯
            trailing_stop_pct: Some(Decimal::new(3, 2)),
        }
    }
}
#[derive(Clone)]
pub struct RiskManager {
    db_pool: sqlx::PgPool,
    http_client: reqwest::Client,
    config: RiskConfig,
}

impl RiskManager {
    pub fn new(
        db_pool: sqlx::PgPool, 
        http_client: reqwest::Client, 
        config: RiskConfig
    ) -> Self {
        config.validate();
        Self {
            db_pool,
            http_client,
            config,
        }
    }
    pub async fn check_instant_stop_loss(
    &self,
    bar: &BarUpdate,
    peak_prices: &mut HashMap<String, Decimal>,
) -> anyhow::Result<()> {
    // 關鍵功能註解：抓取目前帳戶持倉，若未持倉則直接跳過檢查
    let positions = trading::get_positions(&self.http_client).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let pos = match positions.iter().find(|p| p.symbol == bar.symbol) {
        Some(p) => p,
        None => return Ok(()),
    };

    let avg_entry_price = pos.avg_entry_price;
    if avg_entry_price <= Decimal::ZERO {
        return Ok(());
    }

    // 關鍵功能註解：更新歷史最高價用於計算移動停損 (Trailing Stop)
    let current_peak = peak_prices.entry(bar.symbol.clone()).or_insert(bar.close);
    if bar.close > *current_peak {
        *current_peak = bar.close;
    }

    // 關鍵功能註解：計算報酬率與從高點回撤率
    let return_pct = (bar.close - avg_entry_price) / avg_entry_price;
    let drawdown_pct = (*current_peak - bar.close) / *current_peak;

    let mut trigger_reason = None;

    // 關鍵功能註解：檢查固定硬性停損 (Hard Stop Loss)
    if return_pct <= -self.config.stop_loss_pct {
        trigger_reason = Some(format!("觸發固定停損 (虧損 {:.2}%)", return_pct * Decimal::from(100)));
    } 
    // 關鍵功能註解：安全解包檢查固定停利 (僅在 take_profit_pct 為 Some 時觸發)
    else if let Some(tp_pct) = self.config.take_profit_pct {
        if return_pct >= tp_pct {
            trigger_reason = Some(format!("觸發固定停利 (獲利 {:.2}%)", return_pct * Decimal::from(100)));
        }
    }

    // 關鍵功能註解：若未觸發固定停利/停損，進一步檢查移動停損
    if trigger_reason.is_none() {
        if let Some(trailing_pct) = self.config.trailing_stop_pct {
            if drawdown_pct >= trailing_pct && return_pct > Decimal::ZERO {
                trigger_reason = Some(format!("觸發移動停損 (從高點回撤 {:.2}%)", drawdown_pct * Decimal::from(100)));
            }
        }
    }

    // 關鍵功能註解：若滿足任一風控條件，發送市價單立即平倉並清除最高價紀錄
    if let Some(reason) = trigger_reason {
        info!("[即時風控攔截] 標的 {} {}，雙軌高速軌發送平倉單！", bar.symbol, reason);
        
        trading::place_order(
            &self.http_client,
            &bar.symbol,
            Side::Sell,
            OrderTypeInput::Market,
            TimeInForce::Day,
            OrderMethod::Qty(pos.qty.abs()),
            false,
        ).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;

        peak_prices.remove(&bar.symbol);
    }

    Ok(())
}
    /// 風控模組的核心進入點
pub async fn run_risk_manager(
    &self,
    db:&MarketDatabase,
    client: &reqwest::Client,
    analysis: AnalysisResult,
    daily_blacklisted_symbols: &std::collections::HashSet<String>,
    basic_weights: &std::collections::HashMap<String, Decimal>,
) -> anyhow::Result<()> {

    // 關鍵功能註解：開頭先撤銷所有未成交舊掛單，釋放購買力與持倉鎖定
    if let Err(e) = trading::cancel_orders(client, None).await {
        warn!("撤銷舊掛單時發生警告: {:?}", e);
    }

    // 關鍵功能註解：抓取 Alpaca 最新帳戶權益與現有持倉狀態
    let account = trading::get_account(client).await?;
    let positions = trading::get_positions(client).await?;
    
    // 關鍵功能註解：可調度總資產保留 0.5% Buffer 預防滑點與微量交易費
    let safe_equity = account.total_equity * rust_decimal_macros::dec!(0.995);

    // 關鍵功能註解：建立現有持倉 Map (Symbol -> Position)
    let current_positions: std::collections::HashMap<String, &Position> = positions
        .iter()
        .map(|p| (p.symbol.clone(), p))
        .collect();

    // 關鍵功能註解：目標權重永遠以最新分析出的 analysis.weights 為準
    let mut target_weights = analysis.weights.clone();

    // 關鍵功能註解：套用 features 動態指標 (如布林通道超買超賣區清倉)
    for (symbol, feat) in &analysis.features {
        if let Some(&pct_b) = feat.get("bollinger_pct_b") {
            if pct_b < -0.2 || pct_b > 1.2 {
                target_weights.insert(symbol.clone(), Decimal::ZERO);
            }
        }
    }

    // 關鍵功能註解：黑名單強制歸零，觸發風控之標的不再建倉並強制清倉
    for symbol in daily_blacklisted_symbols {
        target_weights.insert(symbol.clone(), Decimal::ZERO);
    }

    // 關鍵功能註解：設定偏離度觸發門檻 (1% = 0.01)
    let rebalance_threshold = rust_decimal_macros::dec!(0.01);

    // 關鍵功能註解：判斷是否需要執行 Rebalance（若 basic_weights 為空則強制執行）
    let mut should_rebalance = basic_weights.is_empty();

    if !should_rebalance {
        let mut check_symbols = std::collections::HashSet::new();
        for k in target_weights.keys() { check_symbols.insert(k.clone()); }
        for k in basic_weights.keys() { check_symbols.insert(k.clone()); }

        for symbol in check_symbols {
            let target_w = target_weights.get(&symbol).copied().unwrap_or(Decimal::ZERO);
            let basic_w = basic_weights.get(&symbol).copied().unwrap_or(Decimal::ZERO);

            // 關鍵功能註解：比較 analysis.weights 與 basic_weights 的偏離度是否達標
            if (target_w - basic_w).abs() >= rebalance_threshold {
                should_rebalance = true;
                break;
            }
        }
    }

    // 關鍵功能註解：偏離度未達門檻且 basic_weights 不為空時跳過 Rebalance
    if !should_rebalance {
        debug!("權重偏離度未達門檻且基準存在，跳過 Rebalance");
        return Ok(());
    }

    let mut sell_orders = Vec::new();
    let mut buy_orders = Vec::new();

    // 關鍵功能註解：聯集所有需要計算的標的 (現有持倉 + 目標標的)
    let mut all_symbols = std::collections::HashSet::new();
    for k in current_positions.keys() { all_symbols.insert(k.clone()); }
    for k in target_weights.keys() { all_symbols.insert(k.clone()); }

    for symbol in all_symbols {
        let current_pos = current_positions.get(&symbol);
        
        let current_qty = current_pos.map(|p| p.qty).unwrap_or(Decimal::ZERO);
        let current_price = current_pos
            .map(|p| p.current_price)
            .unwrap_or_else(|| {
                Decimal::from_f64_retain(
                    *analysis.features.get(&symbol).and_then(|f| f.get("close")).unwrap_or(&1.0)
                ).unwrap_or(Decimal::ONE)
            });

        let target_weight = target_weights.get(&symbol).copied().unwrap_or(Decimal::ZERO);
        let target_value = safe_equity * target_weight;
        let target_qty = (target_value / current_price).trunc();
        let delta_qty = target_qty - current_qty;

        if delta_qty < Decimal::ZERO {
            sell_orders.push((symbol, delta_qty.abs()));
        } else if delta_qty > Decimal::ZERO {
            buy_orders.push((symbol, delta_qty));
        }
    }

    debug!("目前的賣單：{:?}", sell_orders);
    debug!("目前的買單：{:?}", buy_orders);

    // 關鍵功能註解：優先執行賣單釋放資金
    for (symbol, qty) in sell_orders {
        if qty.is_zero() { continue; }
        trading::place_order(
            client,
            &symbol,
            Side::Sell,
            OrderTypeInput::Market,
            TimeInForce::Day,
            OrderMethod::Qty(qty),
            false,
        ).await?;
    }

    // 關鍵功能註解：賣單完成後執行買單
    for (symbol, qty) in buy_orders {
        if qty.is_zero() { continue; }
        trading::place_order(
            client,
            &symbol,
            Side::Buy,
            OrderTypeInput::Market,
            TimeInForce::Day,
            OrderMethod::Qty(qty),
            false,
        ).await?;
    }

    // 關鍵功能註解：觸發 Rebalance 並完成下單後，寫入 DB 更新權重基準
    if let Err(e) = db.save_portfolio_weights(&target_weights).await {
        error!("Rebalance 後更新資料庫權重失敗: {:?}", e);
    } else {
        info!("成功執行 Rebalance 並將最新權重覆寫至 DB！");
    }

    Ok(())
}
}