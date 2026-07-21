// 省略無關內容
use crate::db_storage::{PortfolioTarget, TargetStatus, TargetType};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
pub struct PortfolioManager;

impl PortfolioManager {
    // 關鍵功能註解：驗證投資組合權重總和是否精確等於 1.0 (100%)
    pub fn validate_weights(targets: &[PortfolioTarget]) -> Result<(), String> {
        let mut total_weight = dec!(0.0);
        let mut has_cash = false;

        for target in targets {
            match target.target_type {
                TargetType::Trade | TargetType::Cash => {
                    total_weight += target.weight;
                    if target.target_type == TargetType::Cash {
                        has_cash = true;
                    }
                }
                TargetType::Indicator => {
                    // 關鍵功能註解：風向標指標權重必須為 0，不參與資金分配
                    if target.weight != dec!(0.0) {
                        return Err(format!("風向標 {} 的權重必須為 0", target.symbol));
                    }
                }
            }
        }

        if !has_cash {
            return Err("投資組合中必須包含 CASH (現金) 項目喵！".to_string());
        }

        // 嚴格使用 Decimal 比對 1.0，杜絕 0.99999999 的浮點數問題
        if total_weight != dec!(1.0) {
            return Err(format!(
                "投資組合總權重異常：目前為 {}，必須精確等於 1.0 (100%)",
                total_weight
            ));
        }

        Ok(())
    }
}