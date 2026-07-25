use std::sync::LazyLock;

// 關鍵功能註解：全域環境變數，第一次存取時自動載入並讀取
pub static API_KEY: LazyLock<String> = LazyLock::new(|| {
    let _ = dotenvy::dotenv();
    std::env::var("APCA_API_KEY_ID")
        .expect("找不到 APCA_API_KEY_ID 環境變數喵！")
});

pub static API_SECRET: LazyLock<String> = LazyLock::new(|| {
    let _ = dotenvy::dotenv();
    std::env::var("APCA_API_SECRET_KEY")
        .expect("找不到 APCA_API_SECRET_KEY 環境變數喵！")
});

pub static BASE_URL: LazyLock<String> = LazyLock::new(|| {
    "https://paper-api.alpaca.markets".to_string()
});

pub static DB_URL: LazyLock<String> = LazyLock::new(|| {
    let _ = dotenvy::dotenv();
    std::env::var("DB_URL")
        .expect("找不到 DB_URL 環境變數喵！")
});
// 關鍵功能註解：設定選股與資產快取有效天數                                                                                                                   
pub static CACHE_RETENTION_DAYS: LazyLock<i64> = LazyLock::new(|| {                                                                                           
    let _ = dotenvy::dotenv();                                                                                                                                
    std::env::var("CACHE_RETENTION_DAYS")                                                                                                                     
        .ok()                                                                                                                                                 
        .and_then(|v| v.parse().ok())                                                                                                                         
        .unwrap_or(90)                                                                                                                                        
});