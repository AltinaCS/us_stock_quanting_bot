use std::collections::{BTreeMap,BTreeSet,HashMap};
use chrono::{DateTime, Utc};
use crate::{db_storage::{MarketDatabase, TimeframeConfig,TargetType,KLine}, selector::SelectionPipeline};
use nalgebra::DMatrix;
use tracing::warn;
use uuid::Uuid;
#[derive(Debug, Clone)]
pub struct DailyLogReturns {
    pub timestamps: Vec<i64>,
    /// 每檔標的對應的單期 Log Return 時間序列 (長度為 timestamps.len() - 1)
    pub returns: BTreeMap<String, Vec<Option<f64>>>,
}

/// 關鍵功能註解：3.1 基礎統計特徵結果，包含單資產年化統計與全資產共變異數
#[derive(Debug, Clone)]
pub struct BaseStatistics {
    /// 個股年化對數收益率均值
    pub annualized_means: BTreeMap<String, f64>,
    /// 個股年化波動率
    pub annualized_volatilities: BTreeMap<String, f64>,
    /// 標的對應順序的 Symbol 列表
    pub symbols: Vec<String>,
    /// 標的間的共變異數矩陣 (與 symbols 順序一致)
    pub covariance_matrix: DMatrix<f64>,
    /// 標的間的相關係數矩陣 (與 symbols 順序一致)
    pub correlation_matrix: DMatrix<f64>,
}

pub struct AlignedMarketData {
    // 時間軸標籤：所有有效的市場交易日
    pub timestamps: Vec<i64>,
    // 每個 symbol 對應的對齊後收盤價矩陣 (Options 代表 IPO 前為 None)
    pub prices: BTreeMap<String, Vec<Option<f64>>>,
}

pub struct DailyPriceSnapshot {
    pub timestamp: DateTime<Utc>,
    pub prices: BTreeMap<String, f64>,
}

/// 關鍵功能註解：純統計分析結果輸出容器
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    pub total_days: usize,
    pub covariance_matrix: Vec<Vec<f64>>,
    pub beta_map: BTreeMap<String, f64>,
    pub rolling_volatility: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct AnalysisConfig {
    /// 分析的基準時間點 (As-Of Date)，若為 None 則代表當前 Utc::now()
    pub as_of_ts: Option<i64>,
    /// 觀察的歷史的起始日
    pub start_iso: Option<String>,
    /// 分析的時間頻率 (日 K、週 K、月 K)
    pub timeframe: TimeframeConfig,
}

impl Default for AnalysisConfig {
    fn default() -> Self {
        Self {
            start_iso: Some("2000-01-01T00:00:00Z".to_string()),
            as_of_ts: None,
            timeframe: TimeframeConfig::OneDay,
        }
    }
}

pub fn align_and_forward_fill(
    raw_klines: &HashMap<String, Vec<KLine>>,
) -> AlignedMarketData {
    let mut global_timeline_set = BTreeSet::new();
    for klines in raw_klines.values() {
        for kline in klines {
            global_timeline_set.insert(kline.timestamp);
        }
    }
    let global_timeline: Vec<i64> = global_timeline_set.into_iter().collect();

    // 關鍵功能註解：使用 BTreeMap 自動按字母排序，確保資產欄位順序確定
    let mut aligned_prices = BTreeMap::new();

    let mut sorted_symbols: Vec<&String> = raw_klines.keys().collect();
    sorted_symbols.sort();

    for symbol in sorted_symbols {
        let klines = &raw_klines[symbol];
        if klines.is_empty() {
            continue;
        }

        let price_map: HashMap<i64, f64> = klines.iter().map(|k| (k.timestamp, k.close)).collect();
        let first_ts = klines.first().unwrap().timestamp;
        let last_ts = klines.last().unwrap().timestamp;

        let mut series = Vec::with_capacity(global_timeline.len());
        let mut last_valid_price: Option<f64> = None;

        for &t in &global_timeline {
            if t < first_ts || t > last_ts {
                series.push(None);
            } else if let Some(&price) = price_map.get(&t) {
                last_valid_price = Some(price);
                series.push(Some(price));
            } else {
                series.push(last_valid_price);
            }
        }

        aligned_prices.insert(symbol.clone(), series);
    }

    AlignedMarketData {
        timestamps: global_timeline,
        prices: aligned_prices,
    }
}

/// 關鍵功能註解：輸入時間對齊後的 K 線資料，計算各標的每日單期對數收益率矩陣
pub fn compute_daily_log_returns(aligned_data: &AlignedMarketData) -> DailyLogReturns {
    let mut returns = BTreeMap::new();
    let num_timestamps = aligned_data.timestamps.len();

    if num_timestamps <= 1 {
        return DailyLogReturns {
            timestamps: vec![],
            returns,
        };
    }

    for (symbol, prices) in &aligned_data.prices {
        let mut symbol_returns = Vec::with_capacity(num_timestamps - 1);
        
        for i in 0..(num_timestamps - 1) {
            match (prices[i], prices[i + 1]) {
                (Some(p_prev), Some(p_curr)) if p_prev > 0.0 && p_curr > 0.0 => {
                    symbol_returns.push(Some((p_curr / p_prev).ln()));
                }
                _ => {
                    symbol_returns.push(None);
                }
            }
        }
        returns.insert(symbol.clone(), symbol_returns);
    }

    DailyLogReturns {
        timestamps: aligned_data.timestamps[1..].to_vec(),
        returns,
    }
}

/// 關鍵功能註解：計算無偏協方差矩陣與相關係數矩陣，採用動態 Symbol 向量綁定行列索引
pub fn compute_base_statistics(log_returns: &DailyLogReturns) -> BaseStatistics {
    let trading_days_per_year = 252.0;
    let mut annualized_means = BTreeMap::new();
    let mut annualized_volatilities = BTreeMap::new();

    // 關鍵功能註解：動態取得經由 BTreeMap 排序後的 Symbol 索引清單
    let symbols: Vec<String> = log_returns.returns.keys().cloned().collect();
    let num_assets = symbols.len();
    let num_rows = log_returns.timestamps.len();

    if num_assets == 0 || num_rows == 0 {
        return BaseStatistics {
            annualized_means,
            annualized_volatilities,
            symbols,
            covariance_matrix: DMatrix::zeros(0, 0),
            correlation_matrix: DMatrix::zeros(0, 0),
        };
    }

    let mut clean_returns_list: Vec<Vec<f64>> = Vec::with_capacity(num_assets);

    for symbol in &symbols {
        let raw_returns = &log_returns.returns[symbol];
        let valid_rets: Vec<f64> = raw_returns.iter().filter_map(|&r| r).collect();
        let count = valid_rets.len();

        let mean = if count > 1 {
            let sum: f64 = valid_rets.iter().sum();
            let m = sum / (count as f64);
            let var = valid_rets.iter().map(|v| (v - m).powi(2)).sum::<f64>() / ((count - 1) as f64);
            let vol = var.sqrt();

            annualized_means.insert(symbol.clone(), m * trading_days_per_year);
            annualized_volatilities.insert(symbol.clone(), vol * trading_days_per_year.sqrt());
            m
        } else {
            annualized_means.insert(symbol.clone(), 0.0);
            annualized_volatilities.insert(symbol.clone(), 0.0);
            0.0
        };

        let cleaned: Vec<f64> = raw_returns
            .iter()
            .map(|r| r.map_or(0.0, |v| v - mean))
            .collect();
        clean_returns_list.push(cleaned);
    }

    // 關鍵功能註解：建構 Centered Data 矩陣 (時間列 x 資產欄)，動態映射 index
    let mut centered_matrix = DMatrix::zeros(num_rows, num_assets);
    for (col_idx, cleaned_rets) in clean_returns_list.iter().enumerate() {
        for row_idx in 0..num_rows {
            centered_matrix[(row_idx, col_idx)] = cleaned_rets[row_idx];
        }
    }

    // 關鍵功能註解：計算共變異數矩陣 (資產欄 x 資產欄)
    let covariance_matrix = if num_rows > 1 {
        (&centered_matrix.transpose() * &centered_matrix) / ((num_rows - 1) as f64)
    } else {
        DMatrix::zeros(num_assets, num_assets)
    };

    // 關鍵功能註解：建構相關係數矩陣
    let mut correlation_matrix = DMatrix::zeros(num_assets, num_assets);
    for i in 0..num_assets {
        for j in 0..num_assets {
            let std_i = covariance_matrix[(i, i)].sqrt();
            let std_j = covariance_matrix[(j, j)].sqrt();
            if std_i > 0.0 && std_j > 0.0 {
                correlation_matrix[(i, j)] = covariance_matrix[(i, j)] / (std_i * std_j);
            } else {
                correlation_matrix[(i, j)] = 0.0;
            }
        }
    }

    BaseStatistics {
        annualized_means,
        annualized_volatilities,
        symbols,
        covariance_matrix,
        correlation_matrix,
    }
}

pub fn calculate_historical_beta(
    _asset_returns: &[f64],
    _market_returns: &[f64]
) -> f64 {
    0.0
}

pub fn calculate_annualized_volatility(
    _daily_returns: &[f64],
    _trading_days: f64
) -> f64 {
    0.0
}

pub async fn verify_klines_integrity(
    db: &MarketDatabase,
    start_ts: i64,
    end_ts: i64,
) -> Result<BTreeMap<String, (usize, i64)>, String> {
    let targets = db.load_portfolio_targets().await?;
    println!("載入當前投資組合目標清單成功");
    let mut symbol_stats = BTreeMap::new();

    for target in targets {
        if target.target_type != TargetType::Trade {
            continue;
        }

        let klines = db
            .query_klines_by_range(target.asset_id, &TimeframeConfig::OneDay, start_ts, end_ts)
            .await?;

        if klines.is_empty() {
            warn!("Target {} has no KLine data in specified range", target.symbol);
            continue;
        }

        let first_ts = klines.first().unwrap().timestamp;
        let count = klines.len();

        if first_ts > start_ts + 86400 * 7 {
            warn!(
                "Target {} data starts late at timestamp {}, potential IPO sampling gap",
                target.symbol, first_ts
            );
        }

        symbol_stats.insert(target.symbol, (count, first_ts));
    }

    Ok(symbol_stats)
}

pub async fn run_analysis(
    client: &reqwest::Client,
    db: &MarketDatabase,
    _symbols: &[String],
    config: &AnalysisConfig
) -> Result<AnalysisResult, String> {
    let end_ts = config.as_of_ts.unwrap_or_else(|| Utc::now().timestamp());
    let start_iso_str = config.start_iso.as_deref();
    
    let start_ts = match start_iso_str {
        Some(iso) => DateTime::parse_from_rfc3339(iso)
            .map_err(|e| format!("無效的 ISO 時間格式 ({}): {}", iso, e))?
            .timestamp(),
        None => 946684800,
    };
    
    SelectionPipeline::backfill_portfolio_klines(client, db, start_iso_str, &config.timeframe)
        .await
        .map_err(|e| e.to_string())?; 
    println!("資料補齊完成");

    let _stats = verify_klines_integrity(db, start_ts, end_ts).await?;

    let targets = db.load_portfolio_targets().await?;
    let mut raw_klines_map = HashMap::new();

    for target in &targets {
        let klines = db
            .query_klines_by_range(target.asset_id, &config.timeframe, start_ts, end_ts)
            .await?;
        raw_klines_map.insert(target.symbol.clone(), klines);
    }

    let aligned_data = align_and_forward_fill(&raw_klines_map);
    let daily_log_returns = compute_daily_log_returns(&aligned_data);
    let base_stats = compute_base_statistics(&daily_log_returns);
    
    println!("跑完了 去睡覺吧");

    // 3.2 因子與歸因分析 (需要載入外部 Fama-French & Rf 數據)
    // 產出：個股對 Market, SMB, HML 的 Alpha & Factor Betas
    todo!();//let factor_data = db.load_market_factors_by_range(start_ts, end_ts).await?;
    todo!();//let factor_analysis = compute_fama_french_regression(&daily_log_returns, &factor_data)?;

    // 3.3 技術指標與市場熱度特徵 (直接從 aligned_data 計算)
    // 產出：每檔標的的動量與熱度特徵向量 (RSI, MACD, Bollinger Bands, ATR)
   todo!(); //let tech_features = compute_technical_indicators(&aligned_data);

    // 3.4 時序與動態模型 (條件波動率與協整)
    // 產出：GARCH 條件波動率預測、資產間 Cointegration 協整檢定矩陣
    todo!();//let time_series_models = compute_time_series_dynamics(&daily_log_returns)?;


    // ==========================================
    // 階段 4：理論投資組合建構 (Theoretical Portfolio Optimization)
    // ==========================================
    // 4.1 彙整觀點 (Views) 與市場均衡 (CAPM Anchor)
    // 4.2 Black-Litterman 模型計算 -> 產出理論目標權重 (w_BL) 與期望收益


    // ==========================================
    // 階段 5：幾何路徑與極端風險模擬 (Monte Carlo & Stress Testing)
    // ==========================================
    // 5.1 對理論組合執行 GBM / Jump-Diffusion 蒙地卡羅模擬 (10,000 次路徑)
    // 5.2 計算理論 VaR / CVaR 與權益穿透率 (Tail Risk Analysis)


    // ==========================================
    // 產出最終分析結果打包 (Analysis Result Output)
    // ==========================================
    // 提供給非交易時段分析報告，以及對接實盤風控模組做校準與對比
    
    Ok(AnalysisResult {
        total_days: 0,
        covariance_matrix: vec![],
        beta_map: BTreeMap::new(),
        rolling_volatility: BTreeMap::new(),
    })
}