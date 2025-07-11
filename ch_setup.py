# clickhouse_setup_realtime.py
# This script configures the data pipeline for real-time, per-change inserts.
# Uses direct MergeTree tables without buffering for immediate data visibility.

import time
import logging
from clickhouse_driver import Client
import config
from clickhouse_driver.errors import ServerException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_clickhouse_client():
    """Establishes a connection to the ClickHouse database."""
    try:
        client = Client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD
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

def create_realtime_tables(client):
    """Creates MergeTree tables optimized for real-time inserts and raw data access."""
    
    # MergeTree tables with optimized settings for real-time ingestion
    # Using smaller index_granularity for better real-time performance
    deals_schema = """
    CREATE TABLE deals (
        ts DateTime64(3, 'UTC'),
        timestamp DateTime64(3, 'UTC'), 
        symbol LowCardinality(String), 
        price Float64, 
        volume Int64, 
        side LowCardinality(String),
        raw_message String  -- Store original JSON for complete raw access
    ) ENGINE = MergeTree() 
    ORDER BY (ts, symbol)
    SETTINGS 
        index_granularity = 1024,           -- Smaller for faster real-time queries
        min_rows_for_wide_part = 0,         -- Always use wide parts for better performance
        min_bytes_for_wide_part = 0,
        max_parts_in_total = 100000,        -- Allow more parts for high-frequency inserts
        parts_to_delay_insert = 10000,      -- Higher threshold before delays
        parts_to_throw_insert = 50000       -- Higher threshold before rejecting
    """
    
    klines_schema = """
    CREATE TABLE klines (
        ts DateTime64(3, 'UTC'),
        timestamp DateTime64(3, 'UTC'), 
        symbol LowCardinality(String), 
        kline_type LowCardinality(String), 
        open Float64, 
        close Float64, 
        high Float64, 
        low Float64, 
        volume Float64,
        raw_message String  -- Store original JSON for complete raw access
    ) ENGINE = MergeTree() 
    ORDER BY (ts, symbol, kline_type)
    SETTINGS 
        index_granularity = 1024,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        max_parts_in_total = 100000,
        parts_to_delay_insert = 10000,
        parts_to_throw_insert = 50000
    """
    
    tickers_schema = """
    CREATE TABLE tickers (
        ts DateTime64(3, 'UTC'),
        timestamp DateTime64(3, 'UTC'),
        symbol LowCardinality(String),
        last_price Float64,
        bid1_price Float64,
        ask1_price Float64,
        volume_24h Float64,
        hold_vol Float64,
        fair_price_from_ticker Nullable(Float64),
        index_price_from_ticker Nullable(Float64),
        funding_rate_from_ticker Nullable(Decimal(18, 8)),
        raw_message String  -- Store original JSON for complete raw access
    ) ENGINE = MergeTree() 
    ORDER BY (ts, symbol)
    SETTINGS 
        index_granularity = 1024,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        max_parts_in_total = 100000,
        parts_to_delay_insert = 10000,
        parts_to_throw_insert = 50000
    """
    
    depth_schema = """
    CREATE TABLE depth (
        ts DateTime64(3, 'UTC'),
        timestamp DateTime64(3, 'UTC'), 
        symbol LowCardinality(String), 
        version Int64, 
        bids Array(Tuple(price Float64, volume Float64)), 
        asks Array(Tuple(price Float64, volume Float64)),
        raw_message String  -- Store original JSON for complete raw access
    ) ENGINE = MergeTree() 
    ORDER BY (ts, symbol)
    SETTINGS 
        index_granularity = 1024,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        max_parts_in_total = 100000,
        parts_to_delay_insert = 10000,
        parts_to_throw_insert = 50000
    """
    
    schemas = {"deals": deals_schema, "klines": klines_schema, "tickers": tickers_schema, "depth": depth_schema}
    
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
    for table_name in schemas.keys():
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(f"Dropped existing table: {table_name}")
        except Exception as e:
            logging.info(f"No existing table to drop: {table_name}")
    
    # Create real-time MergeTree tables
    for table_name, schema in schemas.items():
        logging.info(f"--- Creating real-time table: {table_name} ---")
        client.execute(schema)
        logging.info(f"Created real-time table: {table_name}")
    
    # Create views for easy access to just parsed data (without raw_message)
    logging.info("--- Creating convenience views for parsed data ---")
    
    for table_name in schemas.keys():
        view_sql = f"""
        CREATE VIEW {table_name}_parsed AS 
        SELECT * EXCEPT (raw_message) FROM {table_name}
        """
        try:
            client.execute(f"DROP VIEW IF EXISTS {table_name}_parsed")
            client.execute(view_sql)
            logging.info(f"Created view: {table_name}_parsed")
        except Exception as e:
            logging.warning(f"Could not create view {table_name}_parsed: {e}")
    
    # Create view for raw data access across all tables
    logging.info("--- Creating raw data access view ---")
    try:
        client.execute("DROP VIEW IF EXISTS all_raw_messages")
        raw_view_sql = """
        CREATE VIEW all_raw_messages AS
        SELECT 'deals' as table_name, ts, timestamp, symbol, raw_message FROM deals
        UNION ALL
        SELECT 'klines' as table_name, ts, timestamp, symbol, raw_message FROM klines  
        UNION ALL
        SELECT 'tickers' as table_name, ts, timestamp, symbol, raw_message FROM tickers
        UNION ALL
        SELECT 'depth' as table_name, ts, timestamp, symbol, raw_message FROM depth
        ORDER BY ts DESC
        """
        client.execute(raw_view_sql)
        logging.info("Created raw data access view: all_raw_messages")
    except Exception as e:
        logging.warning(f"Could not create raw data view: {e}")
    
    logging.info("✅ All real-time tables and views created successfully.")

def main():
    """Main function to run the real-time ClickHouse setup."""
    print("===== STARTING REAL-TIME CLICKHOUSE SETUP =====")
    
    client = get_clickhouse_client()
    if not client:
        print("Aborting due to connection failure.")
        return
    
    try:
        create_database(client)
        create_realtime_tables(client)
        
        print("\n✅ Real-time ClickHouse setup completed successfully!")
        print("Tables are optimized for immediate per-change inserts.")
        print("Raw JSON messages are preserved in 'raw_message' column.")
        print("Use *_parsed views for structured data without raw JSON.")
        print("Use 'all_raw_messages' view for complete raw data access.")
        
    except Exception as e:
        logging.error(f"Setup failed: {e}", exc_info=True)
        print(f"\n❌ Setup failed: {e}")
    finally:
        if client:
            client.disconnect()
            logging.info("Disconnected from ClickHouse.")
    
    print("\n===== REAL-TIME CLICKHOUSE SETUP FINISHED =====")

if __name__ == "__main__":
    main()