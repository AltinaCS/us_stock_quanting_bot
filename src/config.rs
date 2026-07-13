// 註解：專屬的全域環境變數與設定值模組
use std::sync::OnceLock;

pub static API_KEY: OnceLock<String> = OnceLock::new();
pub static API_SECRET: OnceLock<String> = OnceLock::new();
pub static BASE_URL: OnceLock<String> = OnceLock::new();

// 關鍵功能：由系統入口呼叫的環境變數初始化程序
pub fn init_config() {
    dotenvy::dotenv().ok();
    // 使用 get_or_init 確保全程式生命週期只會執行這一次
    API_KEY.get_or_init(|| {
        std::env::var("APCA_API_KEY_ID")
            .expect("找不到 APCA_API_KEY_ID 環境變數喵！")
    });
    
    API_SECRET.get_or_init(|| {
        std::env::var("APCA_API_SECRET_KEY")
            .expect("找不到 APCA_API_SECRET_KEY 環境變數喵！")
    });
    
    BASE_URL.get_or_init(|| {
        std::env::var("APCA_TRADING_ENDPOINT")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_string())
    });
}