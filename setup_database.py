#!/usr/bin/env python3
import sys
import time
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, 
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE, CLICKHOUSE_BUFFER_TABLE
)

def drop_system_log_tables():
    """Drop system log tables to prevent storage bloat."""
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD if CLICKHOUSE_PASSWORD else ""
    )
    
    system_log_tables = [
        'metric_log', 'query_log', 'trace_log', 'asynchronous_metric_log',
        'processors_profile_log', 'query_thread_log', 'part_log', 'text_log',
        'asynchronous_insert_log', 'opentelemetry_span_log', 'session_log',
        'zookeeper_log', 'transaction_log', 'crash_log'
    ]
    
    print("Dropping system log tables to prevent storage bloat...")
    for table in system_log_tables:
        try:
            client.execute(f"DROP TABLE IF EXISTS system.{table}")
            print(f"  Dropped system.{table}")
        except Exception as e:
            print(f"  Could not drop system.{table}: {e}")
    
    client.disconnect()

def create_database_and_table():
    """Create pure append-only ClickHouse tables for continuous file growth."""
    
    # Connect to default database first  
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD if CLICKHOUSE_PASSWORD else ""
    )
    
    try:
        # Create database if not exists
        print(f"Creating database '{CLICKHOUSE_DATABASE}' if not exists...")
        client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
        
        # Switch to the database
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD if CLICKHOUSE_PASSWORD else "",
            database=CLICKHOUSE_DATABASE
        )
        
        # Drop any existing tables/views for clean setup
        print("Dropping existing tables if they exist...")
        client.execute("DROP VIEW IF EXISTS market_data_ticker")
        client.execute("DROP VIEW IF EXISTS market_data_deal") 
        client.execute("DROP VIEW IF EXISTS market_data_depth")
        client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_BUFFER_TABLE}")
        client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_TABLE}")
        client.execute("DROP TABLE IF EXISTS ticker")
        client.execute("DROP TABLE IF EXISTS deal")
        client.execute("DROP TABLE IF EXISTS depth")
        client.execute("DROP TABLE IF EXISTS btc")
        client.execute("DROP TABLE IF EXISTS eth")
        client.execute("DROP TABLE IF EXISTS sol")
        client.execute("DROP TABLE IF EXISTS btc_current")
        client.execute("DROP TABLE IF EXISTS eth_current")
        client.execute("DROP TABLE IF EXISTS sol_current")
        client.execute("DROP TABLE IF EXISTS btc_previous")
        client.execute("DROP TABLE IF EXISTS eth_previous")
        client.execute("DROP TABLE IF EXISTS sol_previous")
        
        # Create rotating StripeLog tables for hourly directory rotation
        print("Creating btc_current table (StripeLog - rotating)...")
        client.execute("""
        CREATE TABLE btc_current
        (
            ts DateTime64(3),
            mt Enum8('t' = 1, 'd' = 2, 'dp' = 3, 'dl' = 4),
            m String
        )
        ENGINE = StripeLog
        """)
        
        print("Creating eth_current table (StripeLog - rotating)...")
        client.execute("""
        CREATE TABLE eth_current
        (
            ts DateTime64(3),
            mt Enum8('t' = 1, 'd' = 2, 'dp' = 3, 'dl' = 4),
            m String
        )
        ENGINE = StripeLog
        """)
        
        print("Creating sol_current table (StripeLog - rotating)...")
        client.execute("""
        CREATE TABLE sol_current
        (
            ts DateTime64(3),
            mt Enum8('t' = 1, 'd' = 2, 'dp' = 3, 'dl' = 4),
            m String
        )
        ENGINE = StripeLog
        """)
        
        # Create export tracking table for hourly exports
        print("Creating export tracking table...")
        client.execute("""
        CREATE TABLE IF NOT EXISTS export_log
        (
            symbol String,
            hour_start DateTime,
            export_time DateTime,
            filepath String,
            row_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (symbol, hour_start)
        PARTITION BY toYYYYMM(hour_start)
        """)
        
        print("StripeLog symbol tables and export tracking created successfully!")
        
        # Verify setup
        tables = client.execute("SHOW TABLES")
        print(f"\nTables in database '{CLICKHOUSE_DATABASE}':")
        for table in tables:
            if table[0].endswith('_current'):
                print(f"  - {table[0]} (StripeLog - rotating)")
            else:
                print(f"  - {table[0]} (MergeTree)")
        
        print(f"\nSetup Summary:")
        print(f"  BTC data: btc_current table (StripeLog → rotating UUID directories)")
        print(f"  ETH data: eth_current table (StripeLog → rotating UUID directories)")
        print(f"  SOL data: sol_current table (StripeLog → rotating UUID directories)")
        print(f"  Schema: ts (timestamp), mt (message type), m (message data)")
        print(f"  Storage: StripeLog engine with hourly table rotation")
        print(f"  Architecture: 3 current tables, hourly rotation with complete directory cleanup")
        print("\nRotating StripeLog database with hourly directory cleanup completed successfully!")
        
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)
    finally:
        client.disconnect()

if __name__ == "__main__":
    # Wait a bit for ClickHouse to be ready if just started
    print("Setting up ClickHouse database...")
    time.sleep(2)
    
    # First drop system log tables to prevent storage bloat
    drop_system_log_tables()
    
    # Then create our database and tables
    create_database_and_table()