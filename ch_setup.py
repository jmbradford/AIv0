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
    """Creates optimized single-table schema for maximum storage efficiency."""
    
    # Single optimized table with multi-line string format for all message types
    mexc_data_schema = """
    CREATE TABLE mexc_data (
        ts DateTime64(3, 'UTC'),
        ticker String,
        kline String,
        deal String,
        depth String,
        dl String
    ) ENGINE = MergeTree()
    ORDER BY ts
    SETTINGS index_granularity = 8192
    """
    
    schemas = {
        "mexc_data": mexc_data_schema
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
    old_tables = ["deals", "klines", "tickers", "depth", "mexc_messages", "mexc_ticker", "mexc_kline", "mexc_deal", "mexc_depth", "mexc_data"]
    for table_name in old_tables:
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(f"Dropped existing table: {table_name}")
        except Exception as e:
            logging.info(f"No existing table to drop: {table_name}")
    
    # Create optimized single table
    for table_name, schema in schemas.items():
        logging.info(f"--- Creating optimized table: {table_name} ---")
        client.execute(schema)
        logging.info(f"Created optimized table: {table_name}")
    
    # Create unified temporal sequence view for the single table
    logging.info("--- Creating unified temporal sequence view ---")
    
    try:
        client.execute("DROP VIEW IF EXISTS message_sequence")
        message_sequence_sql = """
        CREATE VIEW message_sequence AS
        SELECT 
            ts, 
            CASE 
                WHEN ticker != '' THEN 'ticker'
                WHEN kline != '' THEN 'kline'
                WHEN deal != '' THEN 'deal'
                WHEN depth != '' THEN 'depth'
                ELSE 'unknown'
            END as message_type
        FROM mexc_data
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
        print("Database table created with optimized single-table schema.")
        print("Ready to receive MEXC data through client.py")
        print("Use mexc_data table with multi-line string format for all message types")
        print("Use message_sequence view for temporal ordering and message type identification")
        
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