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
    """Creates the main table with required schema: ts, ticker, kline, deal, depth, dl columns."""
    
    # Single wide table with temporal sequencing design
    # ts column always populated, data columns sparsely populated based on message type
    # Each data column stores structured JSON with extracted values (no raw messages)
    mexc_messages_schema = """
    CREATE TABLE mexc_messages (
        ts DateTime64(3, 'UTC'),        -- ALWAYS populated from message timestamp
        ticker Nullable(String),        -- Structured JSON with all ticker fields, NULL otherwise  
        kline Nullable(String),         -- Structured JSON with all kline fields, NULL otherwise
        deal Nullable(String),          -- Structured JSON with all deal fields, NULL otherwise
        depth Nullable(String),         -- Structured JSON with all depth fields, NULL otherwise
        dl Nullable(String)             -- Structured JSON with error info, NULL otherwise
    ) ENGINE = MergeTree()
    ORDER BY ts
    SETTINGS 
        index_granularity = 1024,           -- Optimized for timestamp-based queries
        min_rows_for_wide_part = 0,         -- Always use wide parts for better performance
        min_bytes_for_wide_part = 0,
        max_parts_in_total = 100000,        -- Allow more parts for high-frequency inserts
        parts_to_delay_insert = 10000,      -- Higher threshold before delays
        parts_to_throw_insert = 50000       -- Higher threshold before rejecting
    """
    
    schemas = {"mexc_messages": mexc_messages_schema}
    
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
    old_tables = ["deals", "klines", "tickers", "depth", "mexc_messages"]
    for table_name in old_tables:
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(f"Dropped existing table: {table_name}")
        except Exception as e:
            logging.info(f"No existing table to drop: {table_name}")
    
    # Create the single wide table
    for table_name, schema in schemas.items():
        logging.info(f"--- Creating temporal sequencing table: {table_name} ---")
        client.execute(schema)
        logging.info(f"Created temporal sequencing table: {table_name}")
    
    # Create filtered views for each message type
    logging.info("--- Creating filtered message type views ---")
    
    # Deals view - filters for deal messages only with extracted fields
    try:
        client.execute("DROP VIEW IF EXISTS deals_view")
        deals_view_sql = """
        CREATE VIEW deals_view AS
        SELECT 
            ts,
            JSONExtractString(deal, 'symbol') as symbol,
            JSONExtractFloat(deal, 'price') as price,
            JSONExtractFloat(deal, 'volume') as volume,
            JSONExtractString(deal, 'side') as side,
            JSONExtractUInt(deal, 'tradeType') as trade_type,
            JSONExtractUInt(deal, 'orderType') as order_type,
            JSONExtractUInt(deal, 'matchType') as match_type,
            deal as deal_data
        FROM mexc_messages 
        WHERE deal IS NOT NULL
        ORDER BY ts DESC
        """
        client.execute(deals_view_sql)
        logging.info("Created view: deals_view")
    except Exception as e:
        logging.warning(f"Could not create deals_view: {e}")
    
    # Klines view - filters for kline messages only with extracted fields
    try:
        client.execute("DROP VIEW IF EXISTS klines_view")
        klines_view_sql = """
        CREATE VIEW klines_view AS
        SELECT 
            ts,
            JSONExtractString(kline, 'symbol') as symbol,
            JSONExtractString(kline, 'interval') as interval,
            JSONExtractFloat(kline, 'open') as open,
            JSONExtractFloat(kline, 'close') as close,
            JSONExtractFloat(kline, 'high') as high,
            JSONExtractFloat(kline, 'low') as low,
            JSONExtractFloat(kline, 'amount') as amount,
            JSONExtractFloat(kline, 'quantity') as quantity,
            kline as kline_data
        FROM mexc_messages 
        WHERE kline IS NOT NULL
        ORDER BY ts DESC
        """
        client.execute(klines_view_sql)
        logging.info("Created view: klines_view")
    except Exception as e:
        logging.warning(f"Could not create klines_view: {e}")
    
    # Tickers view - filters for ticker messages only with extracted fields
    try:
        client.execute("DROP VIEW IF EXISTS tickers_view")
        tickers_view_sql = """
        CREATE VIEW tickers_view AS
        SELECT 
            ts,
            JSONExtractString(ticker, 'symbol') as symbol,
            JSONExtractFloat(ticker, 'lastPrice') as last_price,
            JSONExtractFloat(ticker, 'fairPrice') as fair_price,
            JSONExtractFloat(ticker, 'indexPrice') as index_price,
            JSONExtractFloat(ticker, 'fundingRate') as funding_rate,
            JSONExtractFloat(ticker, 'riseFallRate') as rise_fall_rate,
            JSONExtractFloat(ticker, 'volume24') as volume_24h,
            JSONExtractFloat(ticker, 'amount24') as amount_24h,
            JSONExtractFloat(ticker, 'high24Price') as high_24h,
            JSONExtractFloat(ticker, 'lower24Price') as low_24h,
            JSONExtractFloat(ticker, 'bid1') as bid1,
            JSONExtractFloat(ticker, 'ask1') as ask1,
            ticker as ticker_data
        FROM mexc_messages 
        WHERE ticker IS NOT NULL
        ORDER BY ts DESC
        """
        client.execute(tickers_view_sql)
        logging.info("Created view: tickers_view")
    except Exception as e:
        logging.warning(f"Could not create tickers_view: {e}")
    
    # Depth view - filters for depth messages only with extracted fields
    try:
        client.execute("DROP VIEW IF EXISTS depth_view")
        depth_view_sql = """
        CREATE VIEW depth_view AS
        SELECT 
            ts,
            JSONExtractString(depth, 'symbol') as symbol,
            JSONExtractFloat(depth, 'bestBidPrice') as best_bid_price,
            JSONExtractFloat(depth, 'bestBidQty') as best_bid_qty,
            JSONExtractFloat(depth, 'bestAskPrice') as best_ask_price,
            JSONExtractFloat(depth, 'bestAskQty') as best_ask_qty,
            JSONExtractUInt(depth, 'bidLevels') as bid_levels,
            JSONExtractUInt(depth, 'askLevels') as ask_levels,
            JSONExtractUInt(depth, 'version') as version,
            depth as depth_data
        FROM mexc_messages 
        WHERE depth IS NOT NULL
        ORDER BY ts DESC
        """
        client.execute(depth_view_sql)
        logging.info("Created view: depth_view")
    except Exception as e:
        logging.warning(f"Could not create depth_view: {e}")
    
    # Temporal sequence view - shows message flow chronologically
    try:
        client.execute("DROP VIEW IF EXISTS message_sequence")
        message_sequence_sql = """
        CREATE VIEW message_sequence AS
        SELECT 
            ts,
            CASE 
                WHEN deal IS NOT NULL THEN 'deal'
                WHEN kline IS NOT NULL THEN 'kline' 
                WHEN ticker IS NOT NULL THEN 'ticker'
                WHEN depth IS NOT NULL THEN 'depth'
                WHEN dl IS NOT NULL THEN 'dead_letter'
                ELSE 'unknown'
            END as message_type,
            CASE 
                WHEN deal IS NOT NULL THEN JSONExtractString(deal, 'symbol')
                WHEN kline IS NOT NULL THEN JSONExtractString(kline, 'symbol')
                WHEN ticker IS NOT NULL THEN JSONExtractString(ticker, 'symbol')
                WHEN depth IS NOT NULL THEN JSONExtractString(depth, 'symbol')
                ELSE 'N/A'
            END as symbol
        FROM mexc_messages
        ORDER BY ts DESC
        """
        client.execute(message_sequence_sql)
        logging.info("Created view: message_sequence")
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
        print("Use filtered views for specific data types: deals_view, klines_view, tickers_view, depth_view")
        
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