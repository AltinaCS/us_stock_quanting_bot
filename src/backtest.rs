use std::collections::{BTreeMap,BTreeSet,HashMap};
use chrono::{DateTime, Utc,NaiveDate};
use serde::Deserialize;
use serde::Serialize;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use crate::{db_storage::{MarketDatabase, TimeframeConfig,TargetType,KLine}, selector::SelectionPipeline};
use tracing::warn;
use std::time::Instant;
use uuid::Uuid;
use anyhow::{anyhow,Context,Result};
pub type FeatureMatrix = HashMap<i64, HashMap<String, HashMap<String, f64>>>;
use ndarray::{Array1, Array2, Axis};
use nalgebra::{DMatrix, DVector};
use std::fs::File;
use std::io::BufWriter;
#[derive(Debug, Clone)]
pub struct BlackLittermanViews {
    pub p_matrix: Array2<f64>, // N x N 單位矩陣
    pub q_vector: Array1<f64>, // N x 1 觀點向量 (PCA Alpha 殘差)
    pub omega_matrix: Array2<f64>, // N x N 觀點不確定性對角矩陣
}
#[derive(Debug, Clone)]
pub struct BlackLittermanResult {
    pub expected_returns: HashMap<String, f64>,
    pub target_weights: HashMap<String, f64>,
}
// 關鍵功能註解：將 PCA 隱性因子 Alpha 轉化為 Black-Litterman 觀點矩陣 (P, Q, Ω)

#[derive(Debug, Clone,Serialize)]
pub struct DailyLogReturns {
    pub timestamps: Vec<i64>,
    /// 每檔標的對應的單期 Log Return 時間序列 (長度為 timestamps.len() - 1)
    pub returns: BTreeMap<String, Vec<Option<f64>>>,
}
#[derive(Debug, Clone)]
pub struct LatentFactorResult {
    pub factor_loadings: HashMap<String, Vec<f64>>,
    pub alpha_residuals: HashMap<String, f64>,
    pub explained_variance_ratio: Vec<f64>,
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
#[derive(Debug, Clone)]
struct GarchResult {
    pub last_variance: f64,
}
#[derive(Debug, Clone)]
pub struct AlignedMarketData {
    // 時間軸標籤：所有有效的市場交易日
    pub timestamps: Vec<i64>,
    // 每個 symbol 對應的對齊後收盤價矩陣 (Options 代表 IPO 前為 None)
    pub prices: BTreeMap<String, Vec<Option<f64>>>,
}
#[derive(Debug, Clone)]
pub struct DailyPriceSnapshot {
    pub timestamp: DateTime<Utc>,
    pub prices: BTreeMap<String, f64>,
}

/// 關鍵功能註解：純統計分析結果輸出容器
#[derive(Debug, Clone,Serialize,Deserialize)]
pub struct AnalysisResult {
   pub start_ts: i64, //統計起始時間
   pub end_ts: i64,   //統計終止時間
   pub weights: HashMap<String, Decimal>,
   pub features: HashMap<String, HashMap<String, f64>>
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
pub fn build_views_from_pca_alpha(
    latent_result: &LatentFactorResult,
    tech_features: &HashMap<String, HashMap<String, f64>>,
    symbols: &[String],
    garch_cov: &HashMap<String, HashMap<String, f64>>,
    tau: f64,
) -> Result<BlackLittermanViews> {
    let n = symbols.len();
    if n == 0 {
        return Err(anyhow!("資產列表為空，無法構建 BL 觀點"));
    }

    let p_matrix = Array2::eye(n);
    let mut q_vector = Array1::zeros(n);
    let mut omega_matrix = Array2::zeros((n, n));

    for (i, symbol) in symbols.iter().enumerate() {
        let raw_alpha = latent_result
            .alpha_residuals
            .get(symbol)
            .cloned()
            .unwrap_or(0.0);

        let var_i = garch_cov
            .get(symbol)
            .and_then(|row| row.get(symbol))
            .cloned()
            .unwrap_or(0.0001);

        let mut multiplier = 1.0;
        let mut uncertainty_scaler = 1.0;

        if let Some(feats) = tech_features.get(symbol) {
            let rsi = feats.get("rsi_14").cloned().unwrap_or(50.0);
            let amihud = feats.get("amihud_illiquidity").cloned().unwrap_or(0.0);

            if rsi > 70.0 && raw_alpha > 0.0 {
                multiplier *= 0.5;
            }
            if amihud > 0.001 {
                uncertainty_scaler *= 1.5;
            }
        }

        q_vector[i] = raw_alpha * multiplier;
        omega_matrix[[i, i]] = (tau * var_i * uncertainty_scaler).max(1e-8);
    }

    Ok(BlackLittermanViews {
        p_matrix,
        q_vector,
        omega_matrix,
    })
}

// 關鍵功能註解：執行 Black-Litterman 最佳化求解，產出理論目標權重 w_BL
pub fn run_black_litterman(
    symbols: &[String],
    garch_cov: &HashMap<String, HashMap<String, f64>>,
    views: &BlackLittermanViews,
    tau: f64,
    risk_aversion: f64,
) -> Result<BlackLittermanResult> {
    let n = symbols.len();
    if n == 0 {
        return Err(anyhow!("資產列表為空，無法執行 Black-Litterman"));
    }

    let mut cov_arr = Array2::zeros((n, n));
    for (i, sym_i) in symbols.iter().enumerate() {
        for (j, sym_j) in symbols.iter().enumerate() {
            cov_arr[[i, j]] = garch_cov
                .get(sym_i)
                .and_then(|row| row.get(sym_j))
                .cloned()
                .unwrap_or(0.0);
        }
    }

    let w_mkt = Array1::from_elem(n, 1.0 / (n as f64));
    let pi = risk_aversion * cov_arr.dot(&w_mkt);

    let cov_inv = symmetric_eigen(&cov_arr)
        .map(|(vals, vecs)| {
            let mut inv_diag = Array2::zeros((n, n));
            for k in 0..n {
                let val = if vals[k] > 1e-8 { 1.0 / vals[k] } else { 0.0 };
                inv_diag[[k, k]] = val;
            }
            vecs.dot(&inv_diag).dot(&vecs.t())
        })
        .unwrap_or_else(|_| Array2::eye(n));

    let mut omega_inv = Array2::zeros((n, n));
    for i in 0..n {
        let val = views.omega_matrix[[i, i]];
        omega_inv[[i, i]] = if val > 1e-8 { 1.0 / val } else { 0.0 };
    }

    let tau_cov_inv = &cov_inv / tau;
    let middle_inv = &tau_cov_inv + &omega_inv;

    let middle_inv_solved = symmetric_eigen(&middle_inv)
        .map(|(vals, vecs)| {
            let mut inv_diag = Array2::zeros((n, n));
            for k in 0..n {
                let val = if vals[k] > 1e-8 { 1.0 / vals[k] } else { 0.0 };
                inv_diag[[k, k]] = val;
            }
            vecs.dot(&inv_diag).dot(&vecs.t())
        })
        .unwrap_or_else(|_| Array2::eye(n));

    let right_term = tau_cov_inv.dot(&pi) + omega_inv.dot(&views.q_vector);
    let mu_bl = middle_inv_solved.dot(&right_term);

    let raw_w_bl = (1.0 / risk_aversion) * cov_inv.dot(&mu_bl);

    let mut clipped_w = raw_w_bl.mapv(|x| x.max(0.0));
    let sum_w: f64 = clipped_w.sum();
    if sum_w > 0.0 {
        clipped_w /= sum_w;
    } else {
        clipped_w = w_mkt;
    }

    let mut expected_returns = HashMap::new();
    let mut target_weights = HashMap::new();

    for (i, symbol) in symbols.iter().enumerate() {
        expected_returns.insert(symbol.clone(), mu_bl[i]);
        target_weights.insert(symbol.clone(), clipped_w[i]);
    }

    Ok(BlackLittermanResult {
        expected_returns,
        target_weights,
    })
}
fn fit_garch11(returns: &[f64]) -> GarchResult {
    let omega = 0.000005;
    let alpha = 0.08;
    let beta = 0.90;
    let n = returns.len();

    let mut cond_var = vec![0.0001; n];
    let sample_var = if n > 0 {
        returns.iter().map(|x| x * x).sum::<f64>() / n as f64
    } else {
        0.0001
    };

    cond_var[0] = sample_var.max(0.0001);
    for t in 1..n {
        cond_var[t] = omega + alpha * returns[t - 1].powi(2) + beta * cond_var[t - 1];
    }

    GarchResult {
        last_variance: *cond_var.last().unwrap_or(&0.0001),
    }
}

// 關鍵功能註解：對PCA因子擬合GARCH並構建因子動態協方差矩陣
pub fn compute_factor_garch_cov(
    daily_log_returns: &DailyLogReturns,
    latent_result: &LatentFactorResult,
    num_components: usize,
) -> Result<HashMap<String, HashMap<String, f64>>> {
    let symbols: Vec<String> = daily_log_returns.returns.keys().cloned().collect();
    let num_assets = symbols.len();
    let num_observations = if daily_log_returns.timestamps.len() > 1 {
        daily_log_returns.timestamps.len() - 1
    } else {
        0
    };

    if num_assets == 0 || num_observations < 5 {
        return Err(anyhow!("歷史觀察資料不足，無法執行 Factor GARCH"));
    }

    let mut matrix_data = Vec::with_capacity(num_observations * num_assets);
    for t in 0..num_observations {
        for symbol in &symbols {
            let ret = daily_log_returns.returns[symbol]
                .get(t)
                .and_then(|&opt| opt)
                .unwrap_or(0.0);
            matrix_data.push(ret);
        }
    }

    let returns_matrix = Array2::from_shape_vec((num_observations, num_assets), matrix_data)
        .context("無法轉換為 ndarray 矩陣")?;
    let means = returns_matrix
        .mean_axis(Axis(0))
        .ok_or_else(|| anyhow!("無法計算均值"))?;
    let centered_matrix = &returns_matrix - &means;

    let mut factor_variances = Vec::with_capacity(num_components);
    let mut factor_scores_matrix = Array2::zeros((num_observations, num_components));

    for k in 0..num_components {
        let mut loading_vec = Array1::zeros(num_assets);
        for (idx, symbol) in symbols.iter().enumerate() {
            if let Some(loadings) = latent_result.factor_loadings.get(symbol) {
                loading_vec[idx] = loadings[k];
            }
        }

        let f_k = centered_matrix.dot(&loading_vec);
        for t in 0..num_observations {
            factor_scores_matrix[[t, k]] = f_k[t];
        }

        let garch_res = fit_garch11(f_k.as_slice().unwrap_or(&[]));
        factor_variances.push(garch_res.last_variance);
    }

    let mut residual_variances = HashMap::new();
    for (idx, symbol) in symbols.iter().enumerate() {
        let actual = centered_matrix.column(idx);
        let mut recon:Array1<f64> = Array1::zeros(num_observations);
        if let Some(loadings) = latent_result.factor_loadings.get(symbol) {
            for k in 0..num_components {
                let f_k = factor_scores_matrix.column(k);
                recon = recon + (&f_k * loadings[k]);
            }
        }
        let res = &actual - &recon;
        let res_var = res.iter().map(|x| x * x).sum::<f64>() / (num_observations as f64);
        residual_variances.insert(symbol.clone(), res_var);
    }

    let mut cov_map: HashMap<String, HashMap<String, f64>> = HashMap::new();
    for symbol_i in &symbols {
        let loadings_i = latent_result
            .factor_loadings
            .get(symbol_i)
            .ok_or_else(|| anyhow!("缺少資產 {} 之因子載荷", symbol_i))?;
        let res_var_i = residual_variances.get(symbol_i).cloned().unwrap_or(0.0);

        let mut row_map = HashMap::new();
        for symbol_j in &symbols {
            let loadings_j = latent_result
                .factor_loadings
                .get(symbol_j)
                .ok_or_else(|| anyhow!("缺少資產 {} 之因子載荷", symbol_j))?;

            let mut sys_cov = 0.0;
            for k in 0..num_components {
                sys_cov += loadings_i[k] * factor_variances[k] * loadings_j[k];
            }

            if symbol_i == symbol_j {
                sys_cov += res_var_i;
            }

            row_map.insert(symbol_j.clone(), sys_cov);
        }
        cov_map.insert(symbol_i.clone(), row_map);
    }

    Ok(cov_map)
}
pub fn compute_cross_sectional_pca(
    daily_log_returns: &DailyLogReturns,
    num_components: usize,
) -> Result<LatentFactorResult> {
    if daily_log_returns.returns.is_empty() {
        return Err(anyhow!("輸入的 daily_log_returns 資料為空"));
    }

    // 1. 取得所有標的鍵值與觀察天數 (長度為 timestamps.len() - 1)
    let symbols: Vec<String> = daily_log_returns.returns.keys().cloned().collect();
    let num_assets = symbols.len();
    let num_observations = if daily_log_returns.timestamps.len() > 1 {
        daily_log_returns.timestamps.len() - 1
    } else {
        0
    };

    if num_assets < num_components {
        return Err(anyhow!(
            "資產數量 ({}) 少於要求的主成分數量 ({})",
            num_assets,
            num_components
        ));
    }

    if num_observations < 5 {
        return Err(anyhow!("歷史觀察天數過少，無法執行 PCA"));
    }

    // 2. 構建報酬率矩陣 R (T x N, T為天數, N為資產數)，將 Option<f64> 解包並補 0.0
    let mut matrix_data = Vec::with_capacity(num_observations * num_assets);
    for t in 0..num_observations {
        for symbol in &symbols {
            let ret = daily_log_returns.returns[symbol]
                .get(t)
                .and_then(|&opt| opt)
                .unwrap_or(0.0);
            matrix_data.push(ret);
        }
    }

    let returns_matrix = Array2::from_shape_vec((num_observations, num_assets), matrix_data)
        .context("無法將報酬率資料轉換為 ndarray 矩陣")?;

    // 3. 報酬率矩陣去中心化
    // 關鍵功能註解：對各標的報酬率序列進行中心化處理以計算標準協變異數
    let means = returns_matrix
        .mean_axis(Axis(0))
        .ok_or_else(|| anyhow!("無法計算報酬率均值"))?;
    let centered_matrix = &returns_matrix - &means;

    // 4. 計算資產協變異數矩陣 Cov = (X^T * X) / (T - 1)
    let t_minus_1 = (num_observations - 1) as f64;
    let cov_matrix = centered_matrix.t().dot(&centered_matrix) / t_minus_1;

    // 5. 執行特徵值分解
    // 關鍵功能註解：透過協變異數矩陣分解提取主成分特徵向量與解釋變異數
    let (eigenvalues, eigenvectors) = symmetric_eigen(&cov_matrix)?;

    // 6. 排序特徵值與特徵向量 (降冪)
    let mut eigen_pairs: Vec<(f64, Array1<f64>)> = eigenvalues
        .iter()
        .zip(eigenvectors.axis_iter(Axis(1)))
        .map(|(&val, vec)| (val, vec.to_owned()))
        .collect();

    eigen_pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let total_variance: f64 = eigen_pairs.iter().map(|(val, _)| val.max(0.0)).sum();

    // 7. 提取前 k 個主成分因子載荷與解釋比例
    let mut factor_loadings: HashMap<String, Vec<f64>> = HashMap::new();
    let mut explained_variance_ratio = Vec::with_capacity(num_components);

    for (symbol_idx, symbol) in symbols.iter().enumerate() {
        let mut loadings = Vec::with_capacity(num_components);
        for k in 0..num_components {
            loadings.push(eigen_pairs[k].1[symbol_idx]);
        }
        factor_loadings.insert(symbol.clone(), loadings);
    }

    for k in 0..num_components {
        let var_k = eigen_pairs[k].0.max(0.0);
        explained_variance_ratio.push(if total_variance > 0.0 { var_k / total_variance } else { 0.0 });
    }

    // 8. 重構系統報酬率並提取特質 Alpha 殘差
    // 關鍵功能註解：計算扣除共性因子後之個股殘差 Alpha 向量
    let mut alpha_residuals: HashMap<String, f64> = HashMap::new();

    for (symbol_idx, symbol) in symbols.iter().enumerate() {
        let actual_returns = centered_matrix.column(symbol_idx);
        
        // 修正型態標註：明確指定 Array1<f64> 型態以通過編譯器推導
        let mut reconstructed: Array1<f64> = Array1::zeros(num_observations);

        for k in 0..num_components {
            let loading = eigen_pairs[k].1[symbol_idx];
            let factor_score = centered_matrix.dot(&eigen_pairs[k].1);
            reconstructed = reconstructed + (&factor_score * loading);
        }

        let residual = &actual_returns - &reconstructed;
        let latest_alpha = residual[num_observations - 1];
        alpha_residuals.insert(symbol.clone(), latest_alpha);
    }

    Ok(LatentFactorResult {
        factor_loadings,
        alpha_residuals,
        explained_variance_ratio,
    })
}

/// 關鍵功能註解：對實對稱矩陣執行 Jacobi 特徵值分解與特徵向量求解
fn symmetric_eigen(cov: &Array2<f64>) -> Result<(Vec<f64>, Array2<f64>)> {
    let n = cov.nrows();
    let mut a = cov.clone();
    let mut v = Array2::<f64>::eye(n);

    for _ in 0..100 {
        let mut max_off_diag = 0.0;
        let mut p = 0;
        let mut q = 0;

        for i in 0..n {
            for j in (i + 1)..n {
                if a[[i, j]].abs() > max_off_diag {
                    max_off_diag = a[[i, j]].abs();
                    p = i;
                    q = j;
                }
            }
        }

        if max_off_diag < 1e-10 {
            break;
        }

        let app = a[[p, p]];
        let aqq = a[[q, q]];
        let apq = a[[p, q]];

        let theta = 0.5 * (aqq - app) / apq;
        let t = if theta >= 0.0 {
            1.0 / (theta + (theta * theta + 1.0).sqrt())
        } else {
            -1.0 / (-theta + (theta * theta + 1.0).sqrt())
        };

        let c = 1.0 / (t * t + 1.0).sqrt();
        let s = t * c;

        a[[p, p]] = c * c * app - 2.0 * s * c * apq + s * s * aqq;
        a[[q, q]] = s * s * app + 2.0 * s * c * apq + c * c * aqq;
        a[[p, q]] = 0.0;
        a[[q, p]] = 0.0;

        for i in 0..n {
            if i != p && i != q {
                let aip = a[[i, p]];
                let aiq = a[[i, q]];
                a[[i, p]] = c * aip - s * aiq;
                a[[p, i]] = a[[i, p]];
                a[[i, q]] = s * aip + c * aiq;
                a[[q, i]] = a[[i, q]];
            }

            let vip = v[[i, p]];
            let viq = v[[i, q]];
            v[[i, p]] = c * vip - s * viq;
            v[[i, q]] = s * vip + c * viq;
        }
    }

    let eigenvalues = (0..n).map(|i| a[[i, i]]).collect();
    Ok((eigenvalues, v))
}
/// 關鍵功能註解：依據日期字串或預設最新日計算純量價多維特徵矩陣
pub fn compute_pure_price_features(
    aligned_data: &AlignedMarketData,
    target_timestamp: i64,
) -> Result<HashMap<String, HashMap<String, f64>>> {
    if aligned_data.timestamps.is_empty() {
        return Err(anyhow!("AlignedMarketData 時間軸資料為空"));
    }

    let target_idx = aligned_data
        .timestamps
        .iter()
        .rposition(|&t| t <= target_timestamp)
        .ok_or_else(|| anyhow!("目標時間戳記 {} 未早於 aligned_data 時間軸中", target_timestamp))?;

    let max_window_size = 20usize;
    let min_required_days = 5usize;

    // 全局時間軸長度判斷：若歷史天數連最小要求都不到才 Err
    if target_idx < min_required_days {
        return Err(anyhow!("歷史資料天數不足 {} 日，無法計算滑動視窗特徵", min_required_days));
    }

    let mut symbol_features_map: HashMap<String, HashMap<String, f64>> = HashMap::new();

    for (symbol, price_series) in &aligned_data.prices {
        let actual_available_days = target_idx;
        let effective_window = actual_available_days.min(max_window_size);

        let window_slice = &price_series[target_idx - effective_window..=target_idx];
        let valid_prices: Vec<f64> = window_slice.iter().filter_map(|&p| p).collect();

        // 單一標的資料天數不足則安全跳過，由後續權重模組處理 (例如設為 0 權重)
        if valid_prices.len() < min_required_days {
            continue;
        }

        let current_len = valid_prices.len();
        let current_price = valid_prices[current_len - 1];
        let prev_price = valid_prices[current_len - 2];

        let mut feature_map: HashMap<String, f64> = HashMap::new();

        // 1. 1D 連續對數報酬率
        // 關鍵功能註解：計算單期對數報酬率以供動量與異常衝擊判斷
        let log_return = (current_price / prev_price).ln();
        feature_map.insert("log_return_1d".to_string(), log_return);

        // 2. 動態實現波動度 (Realized Volatility)
        // 關鍵功能註解：依據當前標的可用的歷史天數動態估算實現波動度
        let mut returns = Vec::with_capacity(current_len - 1);
        for i in 1..current_len {
            returns.push((valid_prices[i] / valid_prices[i - 1]).ln());
        }
        let ret_mean = returns.iter().sum::<f64>() / (returns.len() as f64);
        let ret_var = returns.iter().map(|r| (r - ret_mean).powi(2)).sum::<f64>() / (returns.len() as f64);
        feature_map.insert("realized_vol".to_string(), ret_var.sqrt());

        // 3. 動態相對強弱指標 (RSI)
        // 關鍵功能註解：自動調整週期計算 RSI 以反映極端超買超賣
        let rsi_period = (current_len - 1).min(14);
        let mut gains = 0.0;
        let mut losses = 0.0;
        for i in (current_len - rsi_period)..current_len {
            let diff = valid_prices[i] - valid_prices[i - 1];
            if diff > 0.0 {
                gains += diff;
            } else {
                losses += diff.abs();
            }
        }
        let avg_gain = gains / (rsi_period as f64);
        let avg_loss = losses / (rsi_period as f64);
        let rsi = if avg_loss == 0.0 {
            100.0
        } else {
            100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))
        };
        feature_map.insert("rsi".to_string(), rsi);

        // 4. 動態 MA 乖離率 (BIAS) 與 布林 %B
        // 關鍵功能註解：計算價格偏離動態成本線之比例與通道相對位置
        let ma = valid_prices.iter().sum::<f64>() / (current_len as f64);
        let bias = (current_price - ma) / ma;
        feature_map.insert("bias".to_string(), bias);

        let price_std = (valid_prices.iter().map(|p| (p - ma).powi(2)).sum::<f64>() / (current_len as f64)).sqrt();
        let upper_band = ma + 2.0 * price_std;
        let lower_band = ma - 2.0 * price_std;
        let bollinger_pct_b = if (upper_band - lower_band).abs() < 1e-8 {
            0.5
        } else {
            (current_price - lower_band) / (upper_band - lower_band)
        };
        feature_map.insert("bollinger_pct_b".to_string(), bollinger_pct_b);

        symbol_features_map.insert(symbol.clone(), feature_map);
    }

    Ok(symbol_features_map)
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
pub fn apply_risk_overlay_and_convert(
    bl_weights: &HashMap<String, f64>,
    tech_features: &HashMap<String, HashMap<String, f64>>,
    scale: u32,
    min_weight_threshold: f64,
) -> HashMap<String, Decimal> {
    let mut scaled_weights = HashMap::new();
    let mut total_f64_weight = 0.0;

    for (symbol, &w) in bl_weights {
        if w <= 0.0 {
            scaled_weights.insert(symbol.clone(), 0.0);
            continue;
        }

        let mut scale_factor = 1.0;
        if let Some(feats) = tech_features.get(symbol) {
            let atr = feats.get("atr_14").cloned().unwrap_or(0.0);
            if atr > 0.05 {
                scale_factor *= 0.8;
            }
        }

        let final_w = w * scale_factor;
        scaled_weights.insert(symbol.clone(), final_w);
        total_f64_weight += final_w;
    }

    let mut decimal_weights = HashMap::new();
    let mut total_decimal_sum = Decimal::ZERO;
    let mut max_symbol: Option<String> = None;
    let mut max_weight = Decimal::ZERO;

    for (symbol, &w) in &scaled_weights {
        let normalized_w = if total_f64_weight > 0.0 { w / total_f64_weight } else { 0.0 };

        if normalized_w < min_weight_threshold {
            decimal_weights.insert(symbol.clone(), Decimal::ZERO);
            continue;
        }

        if let Some(dec) = Decimal::from_f64_retain(normalized_w) {
            let rounded = dec.round_dp(scale);
            if rounded > max_weight {
                max_weight = rounded;
                max_symbol = Some(symbol.clone());
            }
            total_decimal_sum += rounded;
            decimal_weights.insert(symbol.clone(), rounded);
        } else {
            decimal_weights.insert(symbol.clone(), Decimal::ZERO);
        }
    }

    let target_sum = Decimal::ONE;
    if total_decimal_sum > Decimal::ZERO && total_decimal_sum != target_sum {
        let diff = target_sum - total_decimal_sum;
        if let Some(symbol) = max_symbol {
            if let Some(w) = decimal_weights.get_mut(&symbol) {
                *w += diff;
            }
        }
    }
    let final_sum: Decimal = decimal_weights.values().sum();
    assert_eq!(
        final_sum,
        Decimal::ONE,
        "權重總和校正失敗，當前總和為: {}",
        final_sum
    );
    decimal_weights
}

pub async fn run_analysis(
    client: &reqwest::Client,
    db: &MarketDatabase,
    _symbols: &[String],
    config: &AnalysisConfig,
    save_to_db: bool
) -> Result<AnalysisResult, String> {
    let end_ts = config.as_of_ts.unwrap_or_else(|| Utc::now().timestamp());
    let start_iso_str = config.start_iso.as_deref();
    
    let start_ts = match start_iso_str {
        Some(iso) => DateTime::parse_from_rfc3339(iso)
            .map_err(|e| format!("無效的 ISO 時間格式 ({}): {}", iso, e))?
            .timestamp(),
        None => 946684800,
    };
      // 階段 1：補資料與資料對齊
    // ==========================================
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
    //階段2 初步計算歷史對數日報酬率與共變異數矩陣
    // ==========================================
    let aligned_data = align_and_forward_fill(&raw_klines_map);
    let daily_log_returns = compute_daily_log_returns(&aligned_data);
    let base_stats = compute_base_statistics(&daily_log_returns);
    let file = File::create("daily_log_returns_data.json").map_err(|e|e.to_string())?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &daily_log_returns).map_err(|e|e.to_string())?;


// ==========================================
    // 階段 3：實時特徵工程與動態模型 (Feature Engineering & Latent Factor Model)
    // ==========================================

    // 3.1 純量價特徵與市場熱度 (直接從 aligned_data 計算)
    // 產出：每檔標的之動量、極端波動與流動性衝擊 (RSI, ATR, Parkinson Vol, Volume Z-Score, Amihud)
    // 用途：ATR / Volatility 用於風險預算 (Risk Budgeting) 與頭寸縮放，RSI/Volume 用於風控與Timing overlay
    // 關鍵功能註解：從 aligned_data 提取純量價多維特徵矩陣
    let tech_features: HashMap<String, HashMap<String, f64>> = compute_pure_price_features(&aligned_data,end_ts)
    .with_context(|| format!("執行 run_analysis 階段 3 失敗 [end_ts: {}, timeframe: {:?}]", end_ts, config.timeframe))
    .map_err(|e| format!("{:?}", e))?;
    // 3.2 橫截面 PCA 主成分分析 (替代傳統 Fama-French 因子)
    // 產出：本地即時 SVD 分解出的隱性市場因子 (PC1 大盤共性, PC2 板塊輪動) 與個股 Alpha 殘差
    // 用途：完全排除外部財報延遲與黑盒子，100% 零延遲生成客觀的隱性因子與 Alpha 觀點 vector (Q)
    // 關鍵功能註解：對價格報酬率矩陣執行 PCA 降維並提取個股殘差 Alpha

    let latent_factor_analysis = compute_cross_sectional_pca(&daily_log_returns, 3)
    .with_context(|| "執行 run_analysis 階段 3.2 PCA 隱性因子分解失敗")
    .map_err(|e| format!("{:?}", e))?;
    //println!("{:#?}",latent_factor_analysis);
    // 3.3 時序與動態模型 (條件波動率與協整)
    // 產出：GARCH-Student-t / DCC-GARCH 動態協方差矩陣 Σ (考慮肥尾與波動聚集)
    // 用途：替代傳統 Sample Covariance，精準捕捉暴跌時的波動性激增與尾端風險
    // 關鍵功能註解：擬合動態協方差矩陣以捕捉市場非對稱波動
    let garch_cov_matrix = compute_factor_garch_cov(&daily_log_returns,&latent_factor_analysis,3)
    .with_context(|| "執行 run_analysis 階段 3.3 GARCH時序與動態模型失敗")
    .map_err(|e| format!("{:?}", e))?;
    //println!("PCA 因子解釋比例: {:#?}", garch_cov_matrix);

    // ==========================================
    // 階段 4：理論投資組合建構 (Theoretical Portfolio Optimization)
    // ==========================================

    let symbols: Vec<String> = daily_log_returns.returns.keys().cloned().collect();
    let tau = 0.025; // Tau 參數預設 0.025
    let risk_aversion = 2.5; // 風險趨避係數 Gamma 預設 2.5

    // 4.1 彙整觀點 (Views) 與市場均衡
    let bl_views = build_views_from_pca_alpha(&latent_factor_analysis, &tech_features, &symbols, &garch_cov_matrix, tau)
        .with_context(|| "執行 run_analysis 階段 4.1 構建 BL 觀點矩陣失敗")
        .map_err(|e| format!("{:?}", e))?;

    // 4.2 Black-Litterman 模型計算
    let bl_result = run_black_litterman(&symbols, &garch_cov_matrix, &bl_views, tau, risk_aversion)
        .with_context(|| "執行 run_analysis 階段 4.2 Black-Litterman 求解失敗")
        .map_err(|e| format!("{:?}", e))?;

    // 4.3 風控與動態 Overlay 縮放
    let target_decimal_weights: HashMap<String, Decimal> = apply_risk_overlay_and_convert(
        &bl_result.target_weights,
        &tech_features,
        8,      // 保留 8 位小數
        0.0001, // 低於 0.01% 的微小頭寸過濾
    );
    // ==========================================
    // 階段 5：幾何路徑與極端風險模擬 (Monte Carlo & Stress Testing) (這部分會開新的thread 然後暫時先不處理)
    // ==========================================

    // 5.1 對理論組合執行 Jump-Diffusion 蒙地卡羅模擬 (10,000 次路徑)
    // 5.2 計算理論 99% VaR / CVaR 與極端情境穿透率 (Tail Risk Analysis)
    // 5.3 風控熔斷與技術面 Timing Overlay 修正，產出最終可執行權重 w_target
    // 關鍵功能註解：執行 10,000 次蒙地卡羅模擬評估尾端風險 (VaR / CVaR)
    //let mc_results = run_monte_carlo_simulation(&theoretical_weights, &garch_cov_matrix, 10000)?;
    //let tail_risk = compute_tail_risk(&mc_results);

    // 關鍵功能註解：結合 CVaR 風控預算上限與極端 Z-Score / ATR 斷路器，產出最終目標組合
    //let final_portfolio = apply_risk_budget_and_timing(&theoretical_weights, &tail_risk, &tech_features);


    // ==========================================
    // 產出最終分析結果打包 (Analysis Result Output)
    // ==========================================
    // 提供給非交易時段分析報告，以及對接實盤風控模組做校準與對比
    //TODO：
    if save_to_db{
        db.save_portfolio_weights(&target_decimal_weights).await?;
        println!("[DB] 成功存入當日開盤基準權重與分析數據");
    }
    
    Ok(AnalysisResult{
        start_ts:start_ts,
        end_ts:end_ts,
        features:tech_features,
        weights:target_decimal_weights,
    }
    )
}