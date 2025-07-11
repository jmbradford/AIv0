# verif_optimized.py
# Lightweight verification script using only clickhouse-driver (no pandas dependency)

import time
import logging
from clickhouse_driver import Client
import config
from clickhouse_driver.errors import ServerException
import json
import datetime
import decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
TABLES_TO_VERIFY = ["klines", "deals", "tickers", "depth"]
NUM_RECORDS_TO_FETCH = 3

def get_clickhouse_client():
    """Establishes and returns a connection to the ClickHouse database."""
    try:
        client = Client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB
        )
        client.execute('SELECT 1')
        logging.info("✅ Successfully connected to ClickHouse for verification.")
        return client
    except Exception as e:
        logging.error(f"❌ Failed to connect to ClickHouse: {e}")
        return None

def json_converter(o):
    """Custom JSON converter to handle datetime and Decimal objects."""
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return str(o)
    if isinstance(o, tuple):
        return list(o)  # Convert tuples to lists for JSON serialization
    return str(o)

def verify_data_optimized(client):
    """
    Fetches up to 3 most recent records from each table and shows total counts.
    """
    if not client:
        logging.error("ClickHouse client is not available. Aborting verification.")
        return False

    all_tables_successful = True

    for table in TABLES_TO_VERIFY:
        print(f"\n--- Table: {table} ---")
        try:
            # Use parsed views for cleaner output (without raw_message)
            parsed_table = f"{table}_parsed"
            
            # Get total count for this table
            count_query = f"SELECT count() FROM {parsed_table} WHERE symbol = '{config.SYMBOL}'"
            total_count = client.execute(count_query)[0][0]
            print(f"Total entries: {total_count}")
            
            if total_count == 0:
                print("No data found for this table.")
                continue
            
            # Get most recent records
            if table == 'tickers':
                query = f"""
                SELECT * FROM {parsed_table}
                WHERE symbol = '{config.SYMBOL}'
                ORDER BY funding_rate_from_ticker IS NULL, timestamp DESC
                LIMIT {NUM_RECORDS_TO_FETCH}
                """
            else:
                query = f"""
                SELECT * FROM {parsed_table}
                WHERE symbol = '{config.SYMBOL}'
                ORDER BY timestamp DESC
                LIMIT {NUM_RECORDS_TO_FETCH}
                """

            # Execute query and get column info
            results, columns = client.execute(query, with_column_types=True)

            # Convert to list of dictionaries
            column_names = [col[0] for col in columns]
            records = [dict(zip(column_names, row)) for row in results]

            # Print recent records
            print(f"Most recent {len(records)} entries:")
            print(json.dumps(records, indent=4, default=json_converter))
            logging.info(f"✅ Successfully verified table '{table}': {total_count} total, {len(records)} shown")

        except ServerException as e:
            logging.error(f"❌ A ClickHouse server error occurred while querying table '{table}': {e}")
            all_tables_successful = False
        except Exception as e:
            logging.error(f"❌ An unexpected error occurred for table '{table}': {e}", exc_info=True)
            all_tables_successful = False
            
    return all_tables_successful

def verify_raw_data_access(client):
    """Demonstrate raw data access capabilities."""
    print(f"\n--- Raw Data Access Verification ---")
    try:
        # Show raw data statistics
        query = "SELECT table_name, count() as count FROM all_raw_messages GROUP BY table_name ORDER BY table_name"
        results = client.execute(query)
        
        print("Raw message counts by table:")
        for table_name, count in results:
            print(f"  {table_name}: {count} raw messages")
        
        # Show latest raw message
        query = "SELECT timestamp, table_name, raw_message FROM all_raw_messages ORDER BY timestamp DESC LIMIT 1"
        result = client.execute(query)
        
        if result:
            timestamp, table_name, raw_msg = result[0]
            print(f"\nLatest raw message from {table_name} at {timestamp}:")
            
            # Parse and pretty-print the raw JSON
            try:
                parsed_raw = json.loads(raw_msg)
                print(json.dumps(parsed_raw, indent=2))
            except:
                print(f"Raw: {raw_msg[:500]}...")  # Show first 500 chars if parsing fails
        
        logging.info("✅ Raw data access verified successfully.")
        return True
        
    except Exception as e:
        logging.error(f"❌ Raw data access verification failed: {e}")
        return False


def main():
    """
    Main function to run data verification checks.
    """
    print("===== STARTING DATA VERIFICATION =====")
    client = get_clickhouse_client()
    if not client:
        print("Aborting due to connection failure.")
        return

    try:
        # Verify structured data
        data_success = verify_data_optimized(client)
        
        # Verify raw data access
        raw_success = verify_raw_data_access(client)
        
        if data_success and raw_success:
            print("\n✅ All verification checks passed!")
        else:
            print("\n⚠️ Some verification checks failed. Please check the logs.")
            
    finally:
        if client:
            client.disconnect()
            logging.info("Disconnected from ClickHouse.")
    
    print("\n===== VERIFICATION FINISHED =====")

if __name__ == "__main__":
    main()