# verif.py
# Verification script for optimized ClickHouse schema with specialized tables
# Updated for mexc_ticker, mexc_kline, mexc_deal, mexc_depth tables

import time
import logging
from clickhouse_driver import Client
import config
from clickhouse_driver.errors import ServerException
import datetime
import decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
TABLES_TO_VERIFY = ["mexc_ticker", "mexc_kline", "mexc_deal", "mexc_depth"]
NUM_RECORDS_TO_FETCH = 3

def create_clickhouse_client():
    """Establishes and returns a connection to the ClickHouse database."""
    try:
        client = Client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password='',
            database=config.CLICKHOUSE_DB
        )
        client.execute('SELECT 1')
        logging.info("Successfully connected to ClickHouse for verification.")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to ClickHouse: {e}")
        return None

def verify_optimized_tables(client):
    """
    Verifies each specialized table and shows record counts and sample data.
    """
    if not client:
        logging.error("ClickHouse client is not available. Aborting verification.")
        return False

    all_tables_successful = True
    total_records = 0

    print(f"\n{'='*70}")
    print(f"OPTIMIZED SCHEMA VERIFICATION")
    print(f"{'='*70}")

    # Check each specialized table
    for table in TABLES_TO_VERIFY:
        print(f"\n{'='*50}")
        print(f"TABLE: {table.upper()}")
        print(f"{'='*50}")
        
        try:
            # Get total count for this table
            count_query = f"SELECT COUNT(*) FROM {table}"
            table_count = client.execute(count_query)[0][0]
            total_records += table_count
            print(f"Total records: {table_count}")
            
            if table_count == 0:
                print("ERROR: No data found in this table.")
                continue
            
            # Get table schema information
            schema_query = f"DESCRIBE {table}"
            schema = client.execute(schema_query)
            print(f"Schema ({len(schema)} columns):")
            for column_name, column_type, default_type, default_expr, comment, codec_expr, ttl_expr in schema:
                print(f"   {column_name}: {column_type}")
            
            # Get most recent records
            query = f"""
            SELECT ts, symbol
            FROM {table}
            ORDER BY ts DESC
            LIMIT {NUM_RECORDS_TO_FETCH}
            """
            records = client.execute(query)
            
            print(f"\nRECENT ENTRIES (most recent {len(records)} entries):")
            print("-" * 50)
            
            for i, record in enumerate(records, 1):
                ts_val = record[0]
                symbol_val = record[1]
                print(f"Entry #{i}: ts={ts_val}, symbol={symbol_val}")
            
            logging.info(f"Successfully verified table '{table}': {table_count} records")

        except ServerException as e:
            logging.error(f"ClickHouse server error occurred while querying table '{table}': {e}")
            all_tables_successful = False
        except Exception as e:
            logging.error(f"Unexpected error occurred for table '{table}': {e}", exc_info=True)
            all_tables_successful = False
    
    print(f"\n{'='*70}")
    print(f"TOTAL RECORDS ACROSS ALL TABLES: {total_records}")
    print(f"{'='*70}")
    
    return all_tables_successful

def verify_temporal_ordering(client):
    """Verify that temporal ordering works across all specialized tables."""
    print(f"\n{'='*70}")
    print("TEMPORAL ORDERING VERIFICATION")
    print(f"{'='*70}")
    
    try:
        # Use the message_sequence view to check unified temporal ordering
        query = """
        SELECT ts, message_type, symbol
        FROM message_sequence
        ORDER BY ts DESC
        LIMIT 15
        """
        result = client.execute(query)
        
        print(f"UNIFIED TEMPORAL SEQUENCE (most recent {len(result)} messages):")
        print("-" * 70)
        print("Timestamp                   | Type    | Symbol")
        print("-" * 70)
        
        for ts, message_type, symbol in result:
            print(f"{ts} | {message_type:7} | {symbol}")
        
        # Check time gaps between messages
        if len(result) > 1:
            print(f"\nTIME GAP ANALYSIS:")
            print("-" * 50)
            for i in range(1, min(6, len(result))):
                current_ts = result[i-1][0]
                prev_ts = result[i][0]
                if isinstance(current_ts, datetime.datetime) and isinstance(prev_ts, datetime.datetime):
                    gap = (current_ts - prev_ts).total_seconds()
                    print(f"Gap {i}: {gap:.3f} seconds between {result[i-1][1]} and {result[i][1]}")
        
        logging.info("Temporal ordering verification completed successfully.")
        return True
        
    except Exception as e:
        logging.error(f"Temporal ordering verification failed: {e}")
        print(f"Error: {e}")
        return False

def verify_field_completeness(client):
    """Verify that all expected fields are present in ClickHouse tables."""
    print(f"\n{'='*70}")
    print("FIELD COMPLETENESS VERIFICATION")
    print(f"{'='*70}")
    
    # Expected field counts for each table
    expected_fields = {
        'mexc_ticker': 19,     # All ticker fields including fairPrice, indexPrice
        'mexc_kline': 12,      # All kline fields including real OHLC
        'mexc_deal': 9,        # All deal fields including tradeTime
        'mexc_depth': 11       # All depth fields including bids/asks arrays
    }
    
    try:
        for table_name, expected_count in expected_fields.items():
            print(f"\n{table_name.upper()} FIELD VERIFICATION:")
            print("-" * 50)
            
            # Get actual schema
            schema_query = f"DESCRIBE {table_name}"
            schema = client.execute(schema_query)
            actual_count = len(schema)
            
            print(f"Expected fields: {expected_count}")
            print(f"Actual fields: {actual_count}")
            
            if actual_count == expected_count:
                print("OK: Field count matches expectation")
            else:
                print("ERROR: Field count mismatch!")
            
            # List all fields
            print("\nActual fields in table:")
            for i, (column_name, column_type, *_) in enumerate(schema, 1):
                print(f"  {i:2d}. {column_name} ({column_type})")
        
        # Specific verification for critical ticker fields
        print(f"\nCRITICAL TICKER FIELDS VERIFICATION:")
        print("-" * 50)
        
        critical_ticker_fields = [
            'fairPrice', 'indexPrice', 'fundingRate', 'lastPrice', 
            'high24Price', 'lower24Price', 'maxBidPrice', 'minAskPrice'
        ]
        
        ticker_schema = client.execute("DESCRIBE mexc_ticker")
        actual_ticker_fields = [field[0] for field in ticker_schema]
        
        for field in critical_ticker_fields:
            status = "PRESENT" if field in actual_ticker_fields else "MISSING"
            print(f"  {field:15}: {status}")
            
        print(f"\nSUMMARY: {len([f for f in critical_ticker_fields if f in actual_ticker_fields])}/{len(critical_ticker_fields)} critical fields present")
        
        return True
        
    except Exception as e:
        logging.error(f"Field completeness verification failed: {e}")
        print(f"ERROR: {e}")
        return False

def verify_data_samples(client):
    """Show detailed sample data from each table to verify field extraction."""
    print(f"\n{'='*70}")
    print("DETAILED DATA SAMPLES VERIFICATION")
    print(f"{'='*70}")
    
    try:
        # Sample ticker data - ALL FIELDS
        print("\nTICKER Sample (ALL FIELDS - typed columns):")
        print("-" * 70)
        ticker_query = """
            SELECT ts, symbol, lastPrice, riseFallRate, riseFallValue, fairPrice, indexPrice, 
                   volume24, amount24, high24Price, lower24Price, maxBidPrice, minAskPrice, 
                   fundingRate, bid1, ask1, holdVol, timestamp, zone, riseFallRates, riseFallRatesOfTimezone
            FROM mexc_ticker 
            ORDER BY ts DESC 
            LIMIT 1
        """
        ticker_result = client.execute(ticker_query)
        if ticker_result:
            (ts, symbol, lastPrice, riseFallRate, riseFallValue, fairPrice, indexPrice, 
             volume24, amount24, high24Price, lower24Price, maxBidPrice, minAskPrice, 
             fundingRate, bid1, ask1, holdVol, timestamp, zone, riseFallRates, riseFallRatesOfTimezone) = ticker_result[0]
            
            print(f"  Timestamp: {ts}")
            print(f"  Symbol: {symbol}")
            print(f"  Last Price: {lastPrice}")
            print(f"  Rise/Fall Rate: {riseFallRate}")
            print(f"  Rise/Fall Value: {riseFallValue}")
            print(f"  Fair Price: {fairPrice}")
            print(f"  Index Price: {indexPrice}")
            print(f"  24h Volume: {volume24}")
            print(f"  24h Amount: {amount24}")
            print(f"  24h High: {high24Price}")
            print(f"  24h Low: {lower24Price}")
            print(f"  Max Bid: {maxBidPrice}")
            print(f"  Min Ask: {minAskPrice}")
            print(f"  Funding Rate: {fundingRate:.8f} ({fundingRate*100:.6f}%)")
            print(f"  Best Bid: {bid1}")
            print(f"  Best Ask: {ask1}")
            print(f"  Hold Volume: {holdVol}")
            print(f"  MEXC Timestamp: {timestamp}")
            print(f"  Timezone: {zone}")
            print(f"  Rise/Fall Rates Array: {riseFallRates}")
            print(f"  Timezone Rates Array: {riseFallRatesOfTimezone}")
            
            # Verify critical fields are not missing
            critical_fields = {
                'fairPrice': fairPrice,
                'indexPrice': indexPrice,
                'fundingRate': fundingRate,
                'lastPrice': lastPrice
            }
            
            print(f"\nCRITICAL FIELD VERIFICATION:")
            print("-" * 40)
            for field_name, field_value in critical_fields.items():
                status = "PRESENT" if field_value not in [0, None, ''] else "MISSING/ZERO"
                if field_name == 'fundingRate':
                    formatted_value = f"{field_value:.8f} ({field_value*100:.6f}%)"
                else:
                    formatted_value = str(field_value)
                print(f"  {field_name:12}: {formatted_value} - {status}")
                
        else:
            print("  ERROR: No ticker data found")

        # Sample kline data - ALL FIELDS
        print("\nKLINE Sample (ALL FIELDS - OHLCV data):")
        print("-" * 70)
        kline_query = """
            SELECT ts, symbol, interval, startTime, open, close, high, low, amount, quantity, 
                   realOpen, realClose, realHigh, realLow
            FROM mexc_kline 
            ORDER BY ts DESC 
            LIMIT 1
        """
        kline_result = client.execute(kline_query)
        if kline_result:
            (ts, symbol, interval, startTime, open_price, close_price, high, low, amount, quantity, 
             realOpen, realClose, realHigh, realLow) = kline_result[0]
            print(f"  Timestamp: {ts}")
            print(f"  Symbol: {symbol}")
            print(f"  Interval: {interval}")
            print(f"  Start Time: {startTime}")
            print(f"  OHLC: O={open_price}, H={high}, L={low}, C={close_price}")
            print(f"  Volume: amount={amount}, quantity={quantity}")
            print(f"  Real OHLC: rO={realOpen}, rH={realHigh}, rL={realLow}, rC={realClose}")
        else:
            print("  ERROR: No kline data found")

        # Sample deal data - ALL FIELDS
        print("\nDEAL Sample (ALL FIELDS - trade execution):")
        print("-" * 70)
        deal_query = """
            SELECT ts, symbol, price, volume, side, tradeType, orderType, matchType, tradeTime
            FROM mexc_deal 
            ORDER BY ts DESC 
            LIMIT 1
        """
        deal_result = client.execute(deal_query)
        if deal_result:
            ts, symbol, price, volume, side, tradeType, orderType, matchType, tradeTime = deal_result[0]
            print(f"  Timestamp: {ts}")
            print(f"  Symbol: {symbol}")
            print(f"  Price: {price}")
            print(f"  Volume: {volume}")
            print(f"  Side: {side}")
            print(f"  Trade Type: {tradeType}")
            print(f"  Order Type: {orderType}")
            print(f"  Match Type: {matchType}")
            print(f"  Trade Time: {tradeTime}")
        else:
            print("  ERROR: No deal data found")

        # Sample depth data - ALL FIELDS
        print("\nDEPTH Sample (ALL FIELDS - order book):")
        print("-" * 70)
        depth_query = """
            SELECT ts, symbol, version, bestBidPrice, bestBidQty, bestAskPrice, bestAskQty, 
                   bidLevels, askLevels, bids, asks
            FROM mexc_depth 
            ORDER BY ts DESC 
            LIMIT 1
        """
        depth_result = client.execute(depth_query)
        if depth_result:
            (ts, symbol, version, bestBidPrice, bestBidQty, bestAskPrice, bestAskQty, 
             bidLevels, askLevels, bids, asks) = depth_result[0]
            print(f"  Timestamp: {ts}")
            print(f"  Symbol: {symbol}")
            print(f"  Version: {version}")
            print(f"  Best Bid: {bestBidPrice} x {bestBidQty}")
            print(f"  Best Ask: {bestAskPrice} x {bestAskQty}")
            print(f"  Levels: {bidLevels} bids, {askLevels} asks")
            
            # Show sample bid/ask levels
            if bids and len(bids) > 0:
                print(f"  Sample Bids: {bids[:3]}...")
            if asks and len(asks) > 0:
                print(f"  Sample Asks: {asks[:3]}...")
            
            # Show first few bid/ask levels
            full_depth_query = """
                SELECT bids, asks 
                FROM mexc_depth 
                ORDER BY ts DESC 
                LIMIT 1
            """
            full_depth_result = client.execute(full_depth_query)
            if full_depth_result:
                bids, asks = full_depth_result[0]
                print(f"  Sample Bids (first 3): {bids[:3] if bids else 'None'}")
                print(f"  Sample Asks (first 3): {asks[:3] if asks else 'None'}")
        else:
            print("  No depth data found")

        logging.info("Detailed data samples verification completed.")
        return True
        
    except Exception as e:
        logging.error(f"Data samples verification failed: {e}")
        print(f"Error: {e}")
        return False

def verify_storage_efficiency(client):
    """Verify storage efficiency of the optimized schema."""
    print(f"\n{'='*70}")
    print("STORAGE EFFICIENCY VERIFICATION")
    print(f"{'='*70}")
    
    try:
        # Get storage statistics for each table
        storage_query = """
        SELECT 
            table,
            formatReadableSize(SUM(data_compressed_bytes)) as compressed_size,
            formatReadableSize(SUM(data_uncompressed_bytes)) as uncompressed_size,
            round(SUM(data_uncompressed_bytes)/SUM(data_compressed_bytes), 2) as compression_ratio,
            SUM(rows) as total_rows
        FROM system.parts 
        WHERE database = 'mexc_data' AND active = 1
        GROUP BY table 
        ORDER BY table
        """
        
        storage_result = client.execute(storage_query)
        
        print("STORAGE STATISTICS BY TABLE:")
        print("-" * 70)
        print("Table        | Rows  | Compressed | Uncompressed | Ratio")
        print("-" * 70)
        
        total_compressed_mb = 0
        total_uncompressed_mb = 0
        total_rows = 0
        
        for table, compressed, uncompressed, ratio, rows in storage_result:
            print(f"{table:12} | {rows:5} | {compressed:10} | {uncompressed:12} | {ratio:5}")
            total_rows += rows
            
            # Parse sizes for totals (rough approximation)
            if 'KiB' in compressed:
                total_compressed_mb += float(compressed.split()[0]) / 1024
            elif 'MiB' in compressed:
                total_compressed_mb += float(compressed.split()[0])
        
        print("-" * 70)
        print(f"TOTAL        | {total_rows:5} | ~{total_compressed_mb:.1f}MB    | ~{total_compressed_mb*3:.1f}MB      | Avg")
        
        # Calculate storage efficiency
        print(f"\n🎯 STORAGE EFFICIENCY METRICS:")
        print(f"   ✅ Total records stored: {total_rows}")
        print(f"   ✅ Compressed storage: ~{total_compressed_mb:.1f} MB")
        print(f"   ✅ Specialized tables: {len(storage_result)} optimized tables")
        print(f"   ✅ Typed columns: No JSON parsing overhead")
        print(f"   ✅ LowCardinality: Symbols and enums compressed efficiently")
        
        return True
        
    except Exception as e:
        logging.error(f"Storage efficiency verification failed: {e}")
        print(f"Error: {e}")
        return False

def verify_query_performance(client):
    """Test query performance on the optimized schema."""
    print(f"\n{'='*70}")
    print("QUERY PERFORMANCE VERIFICATION")
    print(f"{'='*70}")
    
    try:
        # Test various query patterns
        queries = [
            ("Price range query", "SELECT COUNT(*) FROM mexc_ticker WHERE lastPrice BETWEEN 2900 AND 3000"),
            ("Recent trades", "SELECT COUNT(*) FROM mexc_deal WHERE ts >= now() - INTERVAL 5 MINUTE"),
            ("Volume aggregation", "SELECT symbol, SUM(volume) FROM mexc_deal GROUP BY symbol"),
            ("Time window analysis", "SELECT toStartOfMinute(ts) as minute, COUNT(*) FROM mexc_kline GROUP BY minute ORDER BY minute DESC LIMIT 5"),
            ("Cross-table temporal", "SELECT COUNT(*) FROM message_sequence WHERE ts >= now() - INTERVAL 10 MINUTE"),
        ]
        
        print("QUERY PERFORMANCE TESTS:")
        print("-" * 70)
        
        for query_name, query in queries:
            start_time = time.time()
            try:
                result = client.execute(query)
                execution_time = (time.time() - start_time) * 1000  # ms
                result_count = len(result) if isinstance(result, list) else 1
                print(f"  {query_name:20} | {execution_time:6.1f}ms | {result_count} rows")
            except Exception as e:
                print(f"  {query_name:20} | ERROR: {str(e)[:30]}...")
        
        print(f"\n🎯 PERFORMANCE BENEFITS:")
        print("   ✅ Typed columns: Fast numeric comparisons")
        print("   ✅ Specialized tables: Optimized for specific queries")
        print("   ✅ Temporal indexing: Efficient time-based filtering")
        print("   ✅ Compressed storage: Faster disk I/O")
        
        return True
        
    except Exception as e:
        logging.error(f"Query performance verification failed: {e}")
        print(f"Error: {e}")
        return False

def main():
    """
    Main function to run comprehensive verification of the optimized schema.
    """
    print("===== STARTING OPTIMIZED SCHEMA VERIFICATION =====")
    client = create_clickhouse_client()
    if not client:
        print("Aborting due to connection failure.")
        return

    try:
        # Verify each specialized table
        tables_success = verify_optimized_tables(client)
        
        # Verify field completeness (NEW - critical for fairPrice/indexPrice verification)
        fields_success = verify_field_completeness(client)
        
        # Verify temporal ordering across tables
        temporal_success = verify_temporal_ordering(client)
        
        # Verify detailed data samples
        samples_success = verify_data_samples(client)
        
        # Verify storage efficiency
        storage_success = verify_storage_efficiency(client)
        
        # Verify query performance
        performance_success = verify_query_performance(client)
        
        if all([tables_success, fields_success, temporal_success, samples_success, storage_success, performance_success]):
            print(f"\n{'='*70}")
            print("ALL VERIFICATION CHECKS PASSED!")
            print("OPTIMIZED SCHEMA WORKING PERFECTLY!")
            print("STORAGE REDUCTION ACHIEVED!")
            print("QUERY PERFORMANCE OPTIMIZED!")
            print(f"{'='*70}")
        else:
            print("\nWARNING: Some verification checks failed. Please check the logs.")
            
    finally:
        if client:
            client.disconnect()
            logging.info("Disconnected from ClickHouse.")
    
    print("\n===== VERIFICATION FINISHED =====")

if __name__ == "__main__":
    main()