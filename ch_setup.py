# ch_setup.py
# ClickHouse database setup and table creation
# Creates schema for MEXC data pipeline

import time
import logging
from clickhouse_driver import Client
import config
from clickhouse_driver.errors import ServerException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_clickhouse_client():
    """Creates and returns a ClickHouse client connection."""
    try:
        client = Client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password='',
            database=config.CLICKHOUSE_DB
        )
        client.execute('SELECT 1')
        logging.info("Successfully connected to ClickHouse.")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to ClickHouse: {e}")
        return None

def create_database(client):
    """Creates the database if it doesn't exist."""
    logging.info(f"Ensuring database '{config.CLICKHOUSE_DB}' exists.")
    client.execute(f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DB}")
    client.execute(f"USE {config.CLICKHOUSE_DB}")
    logging.info(f"Switched to database '{config.CLICKHOUSE_DB}'.")

def create_database_tables(client):
    """Creates optimized table schemas for maximum storage efficiency."""
    
    # Specialized tables with typed columns for optimal compression and performance
    
    # Ticker table - Financial market data with appropriate numeric types
    ticker_schema = """
    CREATE TABLE mexc_ticker (
        ts DateTime64(3, 'UTC'),
        symbol LowCardinality(String),
        lastPrice Float64,
        riseFallRate Float32,
        riseFallValue Float32,
        fairPrice Float64,
        indexPrice Float64,
        volume24 UInt64,
        amount24 Decimal64(8),
        high24Price Float64,
        lower24Price Float64,
        maxBidPrice Float64,
        minAskPrice Float64,
        fundingRate Float64,
        bid1 Float64,
        ask1 Float64,
        holdVol UInt64,
        timestamp UInt64,
        zone LowCardinality(String),
        riseFallRates Array(Float32),
        riseFallRatesOfTimezone Array(Float32)
    ) ENGINE = MergeTree 
    ORDER BY ts
    SETTINGS index_granularity = 8192
    """
    
    # Kline table - OHLCV candlestick data
    kline_schema = """
    CREATE TABLE mexc_kline (
        ts DateTime64(3, 'UTC'),
        symbol LowCardinality(String),
        interval LowCardinality(String),
        startTime UInt64,
        open Float64,
        close Float64,
        high Float64,
        low Float64,
        amount Decimal64(8),
        quantity UInt64,
        realOpen Float64,
        realClose Float64,
        realHigh Float64,
        realLow Float64
    ) ENGINE = MergeTree 
    ORDER BY ts
    SETTINGS index_granularity = 8192
    """
    
    # Deal table - Trade execution data
    deal_schema = """
    CREATE TABLE mexc_deal (
        ts DateTime64(3, 'UTC'),
        symbol LowCardinality(String),
        price Float64,
        volume Decimal32(4),
        side LowCardinality(String),
        tradeType UInt8,
        orderType UInt8,
        matchType UInt8,
        tradeTime UInt64
    ) ENGINE = MergeTree 
    ORDER BY ts
    SETTINGS index_granularity = 8192
    """
    
    # Depth table - Order book data with optimized array storage
    depth_schema = """
    CREATE TABLE mexc_depth (
        ts DateTime64(3, 'UTC'),
        symbol LowCardinality(String),
        version UInt64,
        bestBidPrice Float64,
        bestBidQty UInt32,
        bestAskPrice Float64,
        bestAskQty UInt32,
        bidLevels UInt8,
        askLevels UInt8,
        asks Array(Tuple(Float64, UInt32, UInt8)),
        bids Array(Tuple(Float64, UInt32, UInt8))
    ) ENGINE = MergeTree 
    ORDER BY ts
    SETTINGS index_granularity = 8192
    """
    
    schemas = {
        "mexc_ticker": ticker_schema,
        "mexc_kline": kline_schema, 
        "mexc_deal": deal_schema,
        "mexc_depth": depth_schema
    }
    
    # Create dead letter table for error handling (with raw message storage)
    dead_letter_schema = """
    CREATE TABLE dead_letter (
        timestamp DateTime DEFAULT now(), 
        table_name String, 
        raw_data String, 
        error_message String
    ) ENGINE = MergeTree() 
    ORDER BY timestamp
    SETTINGS index_granularity = 1024
    """
    
    logging.info("--- Preparing Dead-Letter Table ---")
    client.execute("DROP TABLE IF EXISTS dead_letter")
    client.execute(dead_letter_schema)
    logging.info("Dead-letter table created.")
    
    # Drop existing tables to start fresh
    old_tables = ["deals", "klines", "tickers", "depth", "mexc_messages", "mexc_ticker", "mexc_kline", "mexc_deal", "mexc_depth"]
    for table_name in old_tables:
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(f"Dropped existing table: {table_name}")
        except Exception as e:
            logging.info(f"No existing table to drop: {table_name}")
    
    # Create optimized specialized tables
    for table_name, schema in schemas.items():
        logging.info(f"--- Creating optimized table: {table_name} ---")
        client.execute(schema)
        logging.info(f"Created optimized table: {table_name}")
    
    # Create unified temporal sequence view across all specialized tables
    logging.info("--- Creating unified temporal sequence view ---")
    
    try:
        client.execute("DROP VIEW IF EXISTS message_sequence")
        message_sequence_sql = """
        CREATE VIEW message_sequence AS
        SELECT ts, 'ticker' as message_type, symbol FROM mexc_ticker
        UNION ALL
        SELECT ts, 'kline' as message_type, symbol FROM mexc_kline  
        UNION ALL
        SELECT ts, 'deal' as message_type, symbol FROM mexc_deal
        UNION ALL
        SELECT ts, 'depth' as message_type, symbol FROM mexc_depth
        ORDER BY ts DESC
        """
        client.execute(message_sequence_sql)
        logging.info("Created unified temporal sequence view")
    except Exception as e:
        logging.warning(f"Could not create message_sequence view: {e}")
    
    logging.info("✅ All database tables and views created successfully.")

def main():
    """Main function to setup ClickHouse database and tables."""
    print("===== STARTING CLICKHOUSE SETUP =====")
    
    client = create_clickhouse_client()
    if not client:
        print("Aborting due to connection failure.")
        return
    
    try:
        create_database(client)
        create_database_tables(client)
        
        print("\n✅ ClickHouse setup completed successfully!")
        print("Database tables created with proper schema.")
        print("Ready to receive MEXC data through client.py")
        print("Use specialized tables for data types: mexc_ticker, mexc_kline, mexc_deal, mexc_depth")
        print("Use message_sequence view for temporal ordering across all message types")
        
    except Exception as e:
        logging.error(f"Setup failed: {e}", exc_info=True)
        print(f"\n❌ Setup failed: {e}")
    finally:
        if client:
            client.disconnect()
            logging.info("Disconnected from ClickHouse.")
    
    print("\n===== CLICKHOUSE SETUP FINISHED =====")

if __name__ == "__main__":
    main()