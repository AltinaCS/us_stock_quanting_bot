# Rust Automated Trading System

> **⚠️ 免責聲明 **
> 
> 本專案目前僅於**模擬盤（Paper Trading）**環境進行測試，**尚未進行實盤交易**。本專案僅供自我學習與教育參考，不構成任何投資建議。金融市場交易風險極高，使用者須自負盈虧，開發者不負擔任何法律與財務責任。
>
> **⚠️ Disclaimer **
> This project is currently in **paper trading** status for self-learning and educational purposes only and does not constitute financial advice. Trading involves substantial risk; users assume full responsibility for their own results, and the developer(s) assume no liability whatsoever.

---

## 專案簡介 (Overview)
這是一套用 Rust 開發的自動化股票交易系統，透過 Alpaca API 進行即時下單與行情串接，並將交易資料存放在 PostgreSQL 資料庫中。

## 系統架構 (Architecture)
系統採用非同步雙軌機制來處理不同的任務：
* **A 軌（即時風控）**：透過 WebSocket 監控價格，負責即時停損、停利與移動停損。
* **B 軌（週期調倉）**：每 15 分鐘執行一次，計算目標權重並進行資產重組。


## 核心模組 (Core Modules)
* **特質萃取與優化**：結合 PCA 提取 Alpha，並透過 Black-Litterman 模型調整資產權重。
* **風險控管**：內建 MDD 與夏普比率濾網，下單時預留資金緩衝與偏離度門檻以減少磨損。
