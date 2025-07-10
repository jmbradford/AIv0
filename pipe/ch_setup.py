# clickhouse_setup.py
# This script configures the entire data pipeline, from Kafka topics to ClickHouse tables and views.

import time
import logging
from clickhouse_driver import Client
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import config
from clickhouse_driver.errors import ServerException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_kafka_topics():
    """Creates the necessary Kafka topics if they don't already exist."""
    try:
        logging.info(f"Connecting to Kafka for topic creation: {config.KAFKA_BROKERS_HOST}")
        admin_client = KafkaAdminClient(
            bootstrap_servers=config.KAFKA_BROKERS_HOST,
            client_id='mexc_topic_creator',
            request_timeout_ms=5000
        )
        
        existing_topics = admin_client.list_topics()
        logging.info(f"Found existing Kafka topics: {existing_topics}")

        topic_list = []
        # Get unique topic names to avoid duplicate creation requests
        unique_topic_names = set(config.TOPICS.values())
        
        for topic_name in unique_topic_names:
            if topic_name not in existing_topics:
                logging.info(f"Topic '{topic_name}' not found. Scheduling for creation.")
                # Create topic with retention policies for automatic cleanup - optimized for memory efficiency
                topic_config = {
                    'retention.ms': '600000',  # 10 minute retention (reduced from 1 hour)
                    'retention.bytes': '104857600',  # 100MB per topic (reduced from 500MB)
                    'segment.bytes': '10485760',  # 10MB segments for more frequent cleanup
                    'cleanup.policy': 'delete',
                    'delete.retention.ms': '30000',  # 30 second delay before deletion
                    'min.cleanable.dirty.ratio': '0.01'  # Clean more frequently
                }
                topic_list.append(NewTopic(
                    name=topic_name, 
                    num_partitions=1, 
                    replication_factor=1,
                    topic_configs=topic_config
                ))
            else:
                logging.info(f"Topic '{topic_name}' already exists. No action needed.")

        if topic_list:
            try:
                logging.info(f"Attempting to create topics: {[t.name for t in topic_list]}")
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logging.info("Successfully created new Kafka topics.")
            except TopicAlreadyExistsError:
                logging.warning("One or more topics were created by another process in the meantime. Safe to ignore.")
            except Exception as e:
                logging.error(f"FATAL: Failed to create Kafka topics: {e}")
                return False
        else:
            logging.info("All required topics already exist.")
        
        admin_client.close()
        return True

    except Exception as e:
        logging.error(f"FATAL: Could not connect to Kafka AdminClient: {e}")
        return False

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

def create_tables_and_views(client):
    """Creates all necessary tables, Kafka engine tables, and materialized views."""
    deals_schema = """
    CREATE TABLE deals (
        timestamp DateTime64(3, 'UTC'), symbol LowCardinality(String), price Float64, volume Int64, side LowCardinality(String)
    ) ENGINE = MergeTree() ORDER BY (symbol, timestamp)
    """
    klines_schema = """
    CREATE TABLE klines (
        timestamp DateTime64(3, 'UTC'), symbol LowCardinality(String), kline_type LowCardinality(String), open Float64, close Float64, high Float64, low Float64, volume Float64
    ) ENGINE = MergeTree() ORDER BY (symbol, kline_type, timestamp)
    """
    tickers_schema = """
    CREATE TABLE tickers (
        timestamp DateTime64(3, 'UTC'),
        symbol LowCardinality(String),
        last_price Float64,
        bid1_price Float64,
        ask1_price Float64,
        volume_24h Float64,
        hold_vol Float64,
        fair_price_from_ticker Nullable(Float64),
        index_price_from_ticker Nullable(Float64),
        funding_rate_from_ticker Nullable(Decimal(18, 8))
    ) ENGINE = MergeTree() ORDER BY (symbol, timestamp)
    """
    depth_schema = """
    CREATE TABLE depth (
        timestamp DateTime64(3, 'UTC'), symbol LowCardinality(String), version Int64, bids Array(Tuple(price Float64, volume Float64)), asks Array(Tuple(price Float64, volume Float64))
    ) ENGINE = MergeTree() ORDER BY (symbol, timestamp)
    """
    
    schemas = {"deals": deals_schema, "klines": klines_schema, "tickers": tickers_schema, "depth": depth_schema}
    
    # --- Create a Dead-Letter Table for Failed Messages ---
    dead_letter_schema = "CREATE TABLE dead_letter (topic LowCardinality(String), raw String) ENGINE = MergeTree() ORDER BY topic"
    logging.info("--- Preparing Dead-Letter Table ---")
    client.execute("DROP TABLE IF EXISTS dead_letter")
    client.execute(dead_letter_schema)
    logging.info("Dead-letter table created.")

    # Pre-emptively attach any detached views to allow them to be dropped cleanly.
    for table_name in schemas.keys():
        try:
            logging.info(f"Attempting to re-attach view for '{table_name}' if it was detached...")
            client.execute(f"ATTACH TABLE {table_name}_mv")
        except ServerException as e:
            # We can safely ignore errors that the table does not exist (390), is already attached, or already exists (57).
            if e.code in [57, 390] or "already attached" in e.message:
                pass
            else:
                raise e

    kafka_brokers = ','.join(config.KAFKA_BROKERS_INTERNAL)

    # First, create all target tables and drop old objects
    for table_name, schema in schemas.items():
        logging.info(f"--- Preparing schema for '{table_name}' ---")
        logging.info(f"Dropping old objects for '{table_name}' stream...")
        client.execute(f"DROP TABLE IF EXISTS {table_name}_mv")
        client.execute(f"DROP VIEW IF EXISTS {table_name}_mv")
        client.execute(f"DROP TABLE IF EXISTS {table_name}_kafka")
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
        time.sleep(0.5)
        logging.info(f"Creating table: '{table_name}'")
        client.execute(schema)

    # Second, create the necessary Kafka Engine tables for each topic.
    for table_name, topic in config.TOPICS.items():
        kafka_brokers = ','.join(config.KAFKA_BROKERS_INTERNAL)
        # The consumer group name is updated to ensure it reads fresh data after a schema change.
        kafka_table_sql = f"""
        CREATE TABLE {table_name}_kafka (raw String) ENGINE = Kafka
        SETTINGS kafka_broker_list = '{kafka_brokers}', 
                 kafka_topic_list = '{topic}', 
                 kafka_group_name = 'clickhouse_{table_name}_consumer_final_v8', 
                 kafka_format = 'JSONAsString', 
                 kafka_num_consumers = 1;
        """
        logging.info(f"Creating Kafka engine table for topic '{topic}' mapping to table '{table_name}'")
        client.execute(kafka_table_sql)

    # Third, create the materialized views with the correct sourcing logic.
    mv_column_selectors = {
        "deals": "toFloat64(JSONExtract(raw, 'data', 'p', 'String')) as price, toInt64(JSONExtract(raw, 'data', 'v', 'String')) as volume, if(JSONExtractInt(raw, 'data', 'T') = 1, 'buy', 'sell') as side",
        "klines": "JSONExtractString(raw, 'data', 'interval') as kline_type, toFloat64(JSONExtract(raw, 'data', 'o', 'String')) as open, toFloat64(JSONExtract(raw, 'data', 'c', 'String')) as close, toFloat64(JSONExtract(raw, 'data', 'h', 'String')) as high, toFloat64(JSONExtract(raw, 'data', 'l', 'String')) as low, toFloat64(JSONExtract(raw, 'data', 'q', 'String')) as volume",
        "tickers": """
            toFloat64(JSONExtract(raw, 'data', 'lastPrice', 'String')) as last_price,
            toFloat64(JSONExtract(raw, 'data', 'bid1', 'String')) as bid1_price,
            toFloat64(JSONExtract(raw, 'data', 'ask1', 'String')) as ask1_price,
            toFloat64(JSONExtract(raw, 'data', 'volume24', 'String')) as volume_24h,
            toFloat64(JSONExtract(raw, 'data', 'holdVol', 'String')) as hold_vol,
            if(JSONHas(raw, 'data', 'fairPrice'), toFloat64(JSONExtract(raw, 'data', 'fairPrice', 'String')), NULL) as fair_price_from_ticker,
            if(JSONHas(raw, 'data', 'indexPrice'), toFloat64(JSONExtract(raw, 'data', 'indexPrice', 'String')), NULL) as index_price_from_ticker,
            if(JSONHas(raw, 'data', 'fundingRate'), toDecimal64(JSONExtractString(raw, 'data', 'fundingRate'), 8), NULL) as funding_rate_from_ticker
        """,
        "depth": "toInt64(JSONExtract(raw, 'data', 'version', 'String')) as version, JSONExtract(raw, 'data', 'bids', 'Array(Tuple(Float64, Float64))') as bids, JSONExtract(raw, 'data', 'asks', 'Array(Tuple(Float64, Float64))') as asks"
    }

    for table_name in schemas.keys():
        source_table = f"{table_name}_kafka"

        # Timestamp extraction logic varies by message type.
        if table_name in ['tickers', 'depth']:
            # These streams provide a top-level 'ts' timestamp in milliseconds.
            timestamp_extractor = "toDateTime64(toInt64(JSONExtract(raw, 'ts', 'String')) / 1000, 3, 'UTC')"
        elif table_name == 'klines':
            # Klines use a 't' timestamp in seconds within the 'data' object.
            timestamp_extractor = "toDateTime64(toInt64(JSONExtract(raw, 'data', 't', 'String')), 0, 'UTC')"
        else:  # deals
            # Deals use a 't' timestamp in milliseconds within the 'data' object.
            timestamp_extractor = "toDateTime64(toInt64(JSONExtract(raw, 'data', 't', 'String')) / 1000, 3, 'UTC')"

        column_selector = mv_column_selectors[table_name]
        
        # Extracts the symbol (e.g., 'ETH_USDT') from the channel name (e.g., 'push.deal.ETH_USDT').
        symbol_extractor = "JSONExtractString(raw, 'symbol')"

        # WHERE clauses are no longer needed as each stream has its own topic.
        mv_sql = f"""
        CREATE MATERIALIZED VIEW {table_name}_mv TO {table_name} AS
        SELECT
            {timestamp_extractor} AS timestamp,
            {symbol_extractor} AS symbol,
            {column_selector}
        FROM {table_name}_kafka
        WHERE JSONHas(raw, 'channel') AND JSONHas(raw, 'data') AND JSONHas(raw, 'symbol');
        """

        # This second materialized view acts as our dead-letter queue.
        # It catches any message that doesn't have the basic structure and inserts it for review.
        dl_mv_sql = f"""
        CREATE MATERIALIZED VIEW {table_name}_dl_mv TO dead_letter AS
        SELECT 
            '{topic}' AS topic,
            raw
        FROM {table_name}_kafka
        WHERE NOT (JSONHas(raw, 'channel') AND JSONHas(raw, 'data') AND JSONHas(raw, 'symbol'));
        """

        logging.info(f"Creating Materialized View for '{table_name}' sourcing from '{source_table}'.")
        client.execute(mv_sql)
        logging.info(f"Creating Dead-Letter Materialized View for '{table_name}'.")
        client.execute(dl_mv_sql)
        logging.info(f"Successfully created views for '{table_name}'.")

def main():
    logging.info("===== STARTING DATA PIPELINE SETUP =====")
    
    logging.info("--- Step 1: Pausing for 20 seconds to allow services to initialize ---")
    time.sleep(20)
    
    logging.info("--- Step 2: Setting up Kafka Topics ---")
    if not create_kafka_topics():
        logging.error("Setup aborted due to Kafka topic creation failure.")
        return

    logging.info("\n--- Step 3: Setting up ClickHouse Database ---")
    client = None
    for i in range(5):
        client = get_clickhouse_client()
        if client:
            break
        logging.info(f"Attempt {i+1}/5 failed. Retrying in 5 seconds...")
        time.sleep(5)
    
    if not client:
        logging.error("Could not establish connection to ClickHouse after several retries. Aborting setup.")
        return

    try:
        create_database(client)
        create_tables_and_views(client)
        logging.info("\n✅ Database and tables setup completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during ClickHouse setup: {e}", exc_info=True)
    finally:
        if client:
            client.disconnect()
            logging.info("Disconnected from ClickHouse.")
    
    logging.info("\n===== DATA PIPELINE SETUP FINISHED =====")

if __name__ == "__main__":
    main()
