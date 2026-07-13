### Account API Response Example
- **Endpoint:** `/v2/account`
- **Method:** `GET`
- **Response Body:**
\`\`\`json
{
  "account_blocked": false,
  "account_number": "PA3SZB18TSRC",
  "accrued_fees": "0",
  "admin_configurations": {},
  "balance_asof": "2026-07-10",
  "buying_power": "400000",
  "cash": "100000",
  "created_at": "2026-07-10T17:13:37.972665Z",
  "crypto_status": "ACTIVE",
  "crypto_tier": 1,
  "currency": "USD",
  "effective_buying_power": "400000",
  "equity": "100000",
  "id": "28660b4a-6eb1-4ffa-8a05-6405735a88e0",
  "initial_margin": "0",
  "intraday_adjustments": "0",
  "last_equity": "100000",
  "last_maintenance_margin": "0",
  "long_market_value": "0",
  "maintenance_margin": "0",
  "multiplier": "4",
  "non_marginable_buying_power": "100000",
  "options_approved_level": 3,
  "options_buying_power": "100000",
  "options_trading_level": 3,
  "pending_reg_taf_fees": "0",
  "portfolio_value": "100000",
  "position_market_value": "0",
  "regt_buying_power": "200000",
  "short_market_value": "0",
  "shorting_enabled": true,
  "sma": "0",
  "status": "ACTIVE",
  "trade_suspended_by_user": false,
  "trading_blocked": false,
  "transfers_blocked": false,
  "user_configurations": null
}
- **Endpoint:** `/v2/assets`
- **Method:** `GET`
- **Response Body:**
\`\`\`json
[
  {
    "attributes": [],
    "class": "crypto",
    "easy_to_borrow": false,
    "exchange": "CRYPTO",
    "fractionable": true,
    "id": "39d2df3b-4273-46a9-956d-c23634be1e38",
    "maintenance_margin_requirement": 100,
    "margin_requirement_long": "100",
    "margin_requirement_short": "100",
    "marginable": false,
    "min_order_size": "0.271598902",
    "min_trade_increment": "0.000000001",
    "name": "Uniswap / US Dollar",
    "price_increment": "0.000000001",
    "shortable": false,
    "status": "active",
    "symbol": "UNI/USD",
    "tradable": true
  },
  ...
  ,
  {
    "attributes": [],
    "class": "crypto",
    "easy_to_borrow": false,
    "exchange": "CRYPTO",
    "fractionable": true,
    "id": "77c6f47f-0939-4b23-b41e-47b4469c4bc8",
    "maintenance_margin_requirement": 100,
    "margin_requirement_long": "100",
    "margin_requirement_short": "100",
    "marginable": false,
    "min_order_size": "0.010566356",
    "min_trade_increment": "0.000000001",
    "name": "Litecoin / USD Tether",
    "price_increment": "0.000000001",
    "shortable": false,
    "status": "active",
    "symbol": "LTC/USDT",
    "tradable": true
  }
]
- **Endpoint:** `/v2/orders/(order_id)`
- **Method:** `GET`
- **Response Body:**
\`\`\`json
{
  "asset_class": "us_equity",
  "asset_id": "4ce9353c-66d1-46c2-898f-fce867ab0247",
  "canceled_at": null,
  "client_order_id": "36b59e66-1619-43f9-9d72-d523ceccdb85",
  "created_at": "2026-07-12T15:59:24.771767582Z",
  "expired_at": null,
  "expires_at": "2026-10-09T20:00:00Z",
  "extended_hours": false,
  "failed_at": null,
  "filled_at": null,
  "filled_avg_price": null,
  "filled_qty": "0",
  "hwm": null,
  "id": "c582f1da-5c3b-49d3-8cb1-38a15df5837c",
  "legs": null,
  "limit_price": null,
  "notional": null,
  "order_class": "",
  "order_type": "market",
  "position_intent": "buy_to_open",
  "qty": "1",
  "replaced_at": null,
  "replaced_by": null,
  "replaces": null,
  "side": "buy",
  "source": null,
  "status": "accepted",
  "stop_price": null,
  "submitted_at": "2026-07-12T15:59:24.771767582Z",
  "subtag": null,
  "symbol": "NVDA",
  "time_in_force": "gtc",
  "trail_percent": null,
  "trail_price": null,
  "type": "market",
  "updated_at": "2026-07-12T15:59:24.772863412Z"
}
- **Endpoint:** `/v2/orders`
- **Method:** `GET`
- **Response Body:**
\`\`\`json
[
  {
    "asset_class": "us_equity",
    "asset_id": "4ce9353c-66d1-46c2-898f-fce867ab0247",
    "canceled_at": null,
    "client_order_id": "36b59e66-1619-43f9-9d72-d523ceccdb85",
    "created_at": "2026-07-12T15:59:24.771767582Z",
    "expired_at": null,
    "expires_at": "2026-10-09T20:00:00Z",
    "extended_hours": false,
    "failed_at": null,
    "filled_at": null,
    "filled_avg_price": null,
    "filled_qty": "0",
    "hwm": null,
    "id": "c582f1da-5c3b-49d3-8cb1-38a15df5837c",
    "legs": null,
    "limit_price": null,
    "notional": null,
    "order_class": "",
    "order_type": "market",
    "position_intent": "buy_to_open",
    "qty": "1",
    "replaced_at": null,
    "replaced_by": null,
    "replaces": null,
    "side": "buy",
    "source": null,
    "status": "accepted",
    "stop_price": null,
    "submitted_at": "2026-07-12T15:59:24.771767582Z",
    "subtag": null,
    "symbol": "NVDA",
    "time_in_force": "gtc",
    "trail_percent": null,
    "trail_price": null,
    "type": "market",
    "updated_at": "2026-07-12T15:59:24.772863412Z"
  },
  ...
]
- **Endpoint:** `/v2/positions`
- **Method:** `GET`
- **Response Body:**
\`\`\`json
[
    Position {
        symbol: "AAPL",
        qty: 0.5,
        avg_entry_price: 316.92,
        current_price: 317.255,
        unrealized_pnl: 0.1675,
        intraday_pnl: 0.1675,
    },
    Position {
        symbol: "NVDA",
        qty: 10,
        avg_entry_price: 203.98,
        current_price: 203.74,
        unrealized_pnl: -2.4,
        intraday_pnl: -2.4,
    },
]
- **Endpoint:** `/v2/account`
- **Method:** `GET`
- **Response Body:**
\`\`\`json
Account {
    total_equity: 100000.19,
    buying_power: 397362.61,
    cash: 97801.74,
}