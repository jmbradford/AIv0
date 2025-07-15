import os
from enum import Enum
# MEXC WebSocket Configuration
MEXC_WS_URL = "wss://contract.mexc.com/edge"
PING_INTERVAL = 15  # seconds (MEXC recommends 10-20s)
RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_ATTEMPTS = 10

# ClickHouse Configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_HTTP_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = 'ch_mexc'
CLICKHOUSE_TABLE = 'mexc_data'
CLICKHOUSE_BUFFER_TABLE = 'market_data_buffer'

# Message Types
class MessageType(Enum):
    TICKER = 't'
    DEAL = 'd'
    DEPTH = 'dp'
    DEADLETTER = 'dl'

# Data Processing Configuration
BUFFER_SIZE = 2000  # Emergency buffer size
STATS_INTERVAL = 15  # seconds
MAX_ERROR_COUNT = 100  # Maximum errors before emergency shutdown

# Field Mappings
TICKER_FIELDS = ['lastPrice', 'fairPrice', 'indexPrice', 'holdVol', 'fundingRate']
DEAL_FIELDS = ['p', 'v', 'T']  # price, volume, trade direction (T: 1=buy, 2=sell)
DEPTH_FIELDS = ['bids', 'asks']  # Each contains [price, amount, count] arrays

# Symbol-specific configurations with rotating tables
BTC_CONFIG = {
    "symbol": "BTC_USDT",
    "table_name": "btc_current",
    "base_name": "btc",
    "subscriptions": [
        {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}},
        {"method": "sub.deal", "param": {"symbol": "BTC_USDT"}},
        {"method": "sub.depth.full", "param": {"symbol": "BTC_USDT", "limit": 20}}
    ]
}

ETH_CONFIG = {
    "symbol": "ETH_USDT",
    "table_name": "eth_current",
    "base_name": "eth",
    "subscriptions": [
        {"method": "sub.ticker", "param": {"symbol": "ETH_USDT"}},
        {"method": "sub.deal", "param": {"symbol": "ETH_USDT"}},
        {"method": "sub.depth.full", "param": {"symbol": "ETH_USDT", "limit": 20}}
    ]
}

SOL_CONFIG = {
    "symbol": "SOL_USDT",
    "table_name": "sol_current",
    "base_name": "sol",
    "subscriptions": [
        {"method": "sub.ticker", "param": {"symbol": "SOL_USDT"}},
        {"method": "sub.deal", "param": {"symbol": "SOL_USDT"}},
        {"method": "sub.depth.full", "param": {"symbol": "SOL_USDT", "limit": 20}}
    ]
}