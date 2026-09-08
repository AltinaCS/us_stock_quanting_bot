use chrono::{DateTime, TimeZone, Timelike, Utc, NaiveTime,Datelike,NaiveDate,NaiveDateTime};
use quant_bot::db_storage::{MarketDatabase,AppConfig,AlpacaClient, Preprocessor,FactorType};
use chrono_tz::America::New_York;
use quant_bot::trading::{self, OrderMethod, OrderType, OrderTypeInput, Side, TimeInForce, place_order};
use quant_bot::{backtest, config, risk_manager, selector};
use quant_bot::db_storage::{TimeframeConfig};
use quant_bot::selector::{SelectionMode, SelectionPipeline};
use rust_decimal_macros::dec;
use tracing::{debug, error, info, warn};
use rand::prelude::IndexedRandom;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeSet};
use anyhow::Result;
use std::fs;
use std::time::Duration;
use rand::RngExt;
use rust_decimal::Decimal;
use crate::backtest::AnalysisConfig;
use indicatif::{ProgressBar, ProgressStyle};
use quant_bot::risk_manager::{BarUpdate, RiskManager, start_websocket_listener,RiskConfig};
use tracing_subscriber::{fmt,layer::SubscriberExt, util::SubscriberInitExt,EnvFilter};
use std::path::Path;
use tracing_appender::non_blocking::WorkerGuard;
use std::panic;
use anyhow::Context;
#[tokio::main]
async fn main()->anyhow::Result<()>{
    let client = reqwest::Client::new();
    trading::cancel_orders(&client, None).await.context("平倉前撤銷舊掛單失敗")?;
    Ok(())
}