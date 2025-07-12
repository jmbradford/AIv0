# config.py
# This file centralizes all static configuration for the application.
# This makes it easy to change settings like symbols or topics without editing the main logic.

import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the root directory
load_dotenv()

# -- Environment Settings
# Defines the environment for the pipeline, e.g., 'local' or 'production'.
# This helps in managing environment-specific configurations.
PIPELINE_ENVIRONMENT = 'local'

# -- MEXC API Configuration
# The base URL for the MEXC Futures WebSocket API.
MEXC_WSS_URL = "wss://contract.mexc.com/edge"

# The symbol for which to fetch data.
# Format: 'SYMBOL_USDT'
SYMBOL = 'ETH_USDT'

# -- Kafka Configuration
# List of Kafka brokers. For local Docker setup, this is the port exposed to the host.
# For the ClickHouse container, the address is 'kafka:29092'.
KAFKA_BROKERS_HOST = ['localhost:9092']
KAFKA_BROKERS_INTERNAL = ['kafka:29092']

# Kafka topics for different data streams.
# Using distinct topics allows for better data separation and management.
TOPICS = {
    "deals": "mexc_deals",
    "klines": "mexc_klines",
    "tickers": "mexc_tickers",
    "depth": "mexc_depth",
}

# -- ClickHouse Configuration
# Connection details for the ClickHouse database.
# These should match the environment variables in docker-compose.yml.
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = 'mexc_data'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

# -- Data Subscription Details
# Defines the WebSocket subscription channels and parameters.
# This structure makes it easy to add or remove subscriptions.
SUBSCRIPTIONS = {
    "klines": {"method": "sub.kline", "param": {"symbol": SYMBOL, "interval": "Min1"}},
    "deals": {"method": "sub.deal", "param": {"symbol": SYMBOL}},
    "tickers": {"method": "sub.ticker", "param": {"symbol": SYMBOL}},
    "depth": {"method": "sub.depth.full", "param": {"symbol": SYMBOL, "compress": False}},
}

# --- Data Retention Policy ---
DATA_RETENTION_DAYS = 30