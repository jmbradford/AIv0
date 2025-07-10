# verify_data.py
# A comprehensive script to verify that data is being ingested into all ClickHouse tables.
# It fetches the most recent record from each table to confirm end-to-end data flow.

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
NUM_RECORDS_TO_FETCH = 10 # Increased for better verification coverage

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
        # Convert Decimal to string to preserve precision.
        return str(o)

def verify_data(client):
    """
    Fetches the last record from each specified table and prints it as a JSON object.
    """
    if not client:
        logging.error("ClickHouse client is not available. Aborting verification.")
        return

    all_tables_successful = True

    for table in TABLES_TO_VERIFY:
        print(f"\n--- Verifying table: {table} ---")
        try:
            # For the 'tickers' table, we use a special query to prioritize finding records
            # that contain a funding rate, as this value is not sent with every update.
            # `funding_rate_from_ticker IS NULL` evaluates to 0 for non-null (sorted first)
            # and 1 for null (sorted last).
            if table == 'tickers':
                print(f"Querying for last {NUM_RECORDS_TO_FETCH} records, prioritizing those with a funding rate...")
                query = f"""
                SELECT * FROM tickers
                WHERE symbol = '{config.SYMBOL}'
                ORDER BY funding_rate_from_ticker IS NULL, timestamp DESC
                LIMIT {NUM_RECORDS_TO_FETCH}
                """
            else:
                # For all other tables, we fetch the most recent records.
                query = f"""
                SELECT * FROM (
                    SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                    FROM {table}
                    WHERE symbol = '{config.SYMBOL}'
                )
                WHERE rn <= {NUM_RECORDS_TO_FETCH}
                ORDER BY timestamp DESC
                """

            results, columns = client.execute(query, with_column_types=True)

            if not results:
                logging.warning(f"No data found for symbol '{config.SYMBOL}' in table '{table}'.")
                all_tables_successful = False
                continue

            column_names = [col[0] for col in columns]
            
            records = [dict(zip(column_names, row)) for row in results]

            print(json.dumps(records, indent=4, default=json_converter))
            logging.info(f"✅ Successfully verified and displayed {len(records)} records from '{table}'.")

        except ServerException as e:
            logging.error(f"❌ A ClickHouse server error occurred while querying table '{table}': {e}")
            all_tables_successful = False
        except Exception as e:
            logging.error(f"❌ An unexpected error occurred for table '{table}': {e}", exc_info=True)
            all_tables_successful = False
            
    return all_tables_successful

def main():
    """
    Main function to run all data verification checks.
    """
    print("===== STARTING DATA VERIFICATION =====")
    client = get_clickhouse_client()
    if not client:
        print("Aborting due to connection failure.")
        return

    try:
        success = verify_data(client)
        if success:
            print("\n✅ All tables verified successfully!")
        else:
            print("\n⚠️ Some tables could not be verified. Please check the logs.")
    finally:
        if client:
            client.disconnect()
            logging.info("Disconnected from ClickHouse.")
    
    print("\n===== DATA VERIFICATION FINISHED =====")

if __name__ == "__main__":
    main() 