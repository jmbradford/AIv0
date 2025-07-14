#!/usr/bin/env python3
import sys
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE, CLICKHOUSE_BUFFER_TABLE
)

def connect_with_retry(max_retries=3):
    """Connect to ClickHouse with retry logic."""
    for attempt in range(max_retries):
        try:
            client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            
            # Test connection by checking if database exists
            client.execute("SELECT 1")
            print(f"✅ Connected to ClickHouse successfully (attempt {attempt + 1})")
            return client
            
        except Exception as e:
            print(f"❌ Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(2)
            else:
                print(f"❌ Failed to connect after {max_retries} attempts")
                return None

def verify_tables_exist(client):
    """Verify required symbol-specific tables exist."""
    try:
        btc_exists = client.execute("EXISTS TABLE btc_current")[0][0]
        eth_exists = client.execute("EXISTS TABLE eth_current")[0][0]
        sol_exists = client.execute("EXISTS TABLE sol_current")[0][0]
        export_log_exists = client.execute("EXISTS TABLE export_log")[0][0]
        
        if not (btc_exists and eth_exists and sol_exists):
            print(f"❌ Current symbol tables missing - run setup_database.py first")
            return False
            
        if not export_log_exists:
            print(f"⚠️  Export log table missing - hourly exports may not work")
            
        print(f"✅ Current symbol tables exist: btc_current, eth_current, sol_current")
        if export_log_exists:
            print(f"✅ Export log table exists: export_log")
        return True
        
    except Exception as e:
        print(f"❌ Error checking tables: {e}")
        return False

def verify_data():
    """Verify data in ClickHouse by showing last 3 entries of each type."""
    
    # Connect with retry logic
    client = connect_with_retry()
    if not client:
        print("❌ Cannot establish ClickHouse connection - aborting verification")
        sys.exit(1)
        
    # Verify tables exist
    if not verify_tables_exist(client):
        print("❌ Required tables missing - run setup_database.py first")
        sys.exit(1)
    
    try:
        print("\n" + "="*80)
        print("DATA VERIFICATION REPORT")
        print("="*80)
        
        # Check symbol-specific file sizes
        print("💾 Symbol-specific storage status:")
        
        # Get counts from each current symbol table
        btc_count = client.execute("SELECT COUNT(*) FROM btc_current")[0][0]
        eth_count = client.execute("SELECT COUNT(*) FROM eth_current")[0][0] 
        sol_count = client.execute("SELECT COUNT(*) FROM sol_current")[0][0]
        total_count = btc_count + eth_count + sol_count
        
        print(f"\nTotal records in current tables: {total_count}")
        print(f"  btc_current: {btc_count} records")
        print(f"  eth_current: {eth_count} records")
        print(f"  sol_current: {sol_count} records")
        
        # Get counts by message type for each symbol
        print("\nRecords by symbol and message type:")
        for symbol_table in ['btc_current', 'eth_current', 'sol_current']:
            try:
                type_counts = client.execute(f"""
                    SELECT mt, COUNT(*) as count 
                    FROM {symbol_table}
                    GROUP BY mt 
                    ORDER BY mt
                """)
                
                symbol_total = sum([count for _, count in type_counts])
                symbol_name = symbol_table.replace('_current', '').upper()
                print(f"  {symbol_name}: {symbol_total} total")
                for msg_type, count in type_counts:
                    print(f"    {msg_type}: {count}")
            except Exception as e:
                symbol_name = symbol_table.replace('_current', '').upper()
                print(f"  {symbol_name}: No data yet")
        
        # Show last 3 ticker messages from each symbol
        print("\n" + "-"*80)
        print("LAST 3 TICKER MESSAGES (BY SYMBOL)")
        print("-"*80)
        
        for symbol_table in ['btc_current', 'eth_current', 'sol_current']:
            symbol_name = symbol_table.replace('_current', '').upper()
            print(f"\n{symbol_name} Ticker Messages:")
            try:
                ticker_data = client.execute(f"""
                    SELECT ts, m
                    FROM {symbol_table}
                    WHERE mt = 't'
                    ORDER BY ts DESC 
                    LIMIT 3
                """)
                
                if ticker_data:
                    print("  Timestamp            | Message (lastPrice|fairPrice|indexPrice|holdVol|fundingRate)")
                    print("  " + "-"*90)
                    for row in reversed(ticker_data):
                        print(f"  {row[0]} | {row[1]}")
                else:
                    print("  No ticker data found")
            except Exception as e:
                print(f"  Error: {e}")
        
        # Show last 3 deal messages from each symbol
        print("\n" + "-"*80)
        print("LAST 3 DEAL MESSAGES (BY SYMBOL)")
        print("-"*80)
        
        for symbol_table in ['btc_current', 'eth_current', 'sol_current']:
            symbol_name = symbol_table.replace('_current', '').upper()
            print(f"\n{symbol_name} Deal Messages:")
            try:
                deal_data = client.execute(f"""
                    SELECT ts, m
                    FROM {symbol_table}
                    WHERE mt = 'd'
                    ORDER BY ts DESC 
                    LIMIT 3
                """)
                
                if deal_data:
                    print("  Timestamp            | Message (price|volume|direction)")
                    print("  " + "-"*65)
                    for row in reversed(deal_data):
                        print(f"  {row[0]} | {row[1]}")
                else:
                    print("  No deal data found")
            except Exception as e:
                print(f"  Error: {e}")
        
        # Show last 3 depth messages from each symbol (simplified)
        print("\n" + "-"*80)
        print("LAST 3 DEPTH MESSAGES (BY SYMBOL)")
        print("-"*80)
        
        for symbol_table in ['btc_current', 'eth_current', 'sol_current']:
            symbol_name = symbol_table.replace('_current', '').upper()
            print(f"\n{symbol_name} Depth Messages:")
            try:
                depth_data = client.execute(f"""
                    SELECT ts, m
                    FROM {symbol_table}
                    WHERE mt = 'dp'
                    ORDER BY ts DESC 
                    LIMIT 3
                """)
                
                if depth_data:
                    print("  Depth data (truncated for display):")
                    for row in reversed(depth_data):
                        ts, message = row
                        # Split bids and asks 
                        parts = message.split('|')
                        bids_display = parts[0][:50] + "..." if len(parts[0]) > 50 else parts[0]
                        asks_display = parts[1][:50] + "..." if len(parts) > 1 and len(parts[1]) > 50 else (parts[1] if len(parts) > 1 else "")
                        print(f"  {ts}")
                        print(f"    Bids: {bids_display}")
                        print(f"    Asks: {asks_display}")
                else:
                    print("  No depth data found")
            except Exception as e:
                print(f"  Error: {e}")
        
        # Export log verification
        print("\n" + "-"*80)
        print("EXPORT LOG VERIFICATION")
        print("-"*80)
        
        try:
            export_stats = client.execute("""
                SELECT symbol, COUNT(*) as exported_hours, 
                       MIN(hour_start) as first_export,
                       MAX(hour_start) as last_export
                FROM export_log 
                GROUP BY symbol
                ORDER BY symbol
            """)
            
            if export_stats:
                print("  Hourly exports completed:")
                for symbol, count, first, last in export_stats:
                    print(f"    {symbol.upper()}: {count} hours exported (first: {first}, last: {last})")
            else:
                print("  No exports completed yet")
        except Exception as e:
            print(f"  Export log check failed: {e}")
        
        # Storage statistics - symbol-specific files
        print("\n" + "-"*80)
        print("STORAGE STATISTICS")
        print("-"*80)
        
        print("  Symbol-specific append-only architecture: StripeLog tables")
        print("  Files grow continuously with no parts or merging")
        print("  Schema: ts (timestamp), mt (message type), m (message data)")
        print("  Tables: btc_current, eth_current, sol_current (active data)")
        print("  Export: export_log (tracks hourly parquet exports)")
        
        # Check Docker volume size growth
        try:
            import subprocess
            result = subprocess.run(['docker', 'exec', 'mexc-clickhouse', 'du', '-sh', '/var/lib/clickhouse/'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                volume_size = result.stdout.strip().split('\t')[0]
                print(f"  Total ClickHouse volume size: {volume_size}")
        except Exception:
            print("  Volume size: Unable to check")
        
        # Show individual symbol file info
        try:
            for symbol in ['btc_current', 'eth_current', 'sol_current']:
                file_result = subprocess.run(['docker', 'exec', 'mexc-clickhouse', 'find', 
                                            f'/var/lib/clickhouse/data/{CLICKHOUSE_DATABASE}/{symbol}/', 
                                            '-name', '*.bin', '-exec', 'du', '-h', '{}', ';'], 
                                           capture_output=True, text=True)
                if file_result.returncode == 0 and file_result.stdout.strip():
                    lines = file_result.stdout.strip().split('\n')
                    for line in lines:
                        if 'data.bin' in line:
                            size = line.split('\t')[0]
                            symbol_name = symbol.replace('_current', '')
                            print(f"  {symbol_name}.bin: {size}")
                else:
                    symbol_name = symbol.replace('_current', '')
                    print(f"  {symbol_name}.bin: File not found yet")
        except Exception:
            print("  Individual file sizes: Unable to check")
        
        # Show export directory if it exists
        try:
            export_result = subprocess.run(['ls', '-la', 'exports/'], 
                                          capture_output=True, text=True)
            if export_result.returncode == 0:
                print("\n  Export directory contents:")
                lines = export_result.stdout.strip().split('\n')
                parquet_files = [line for line in lines if '.parquet' in line]
                if parquet_files:
                    print(f"    {len(parquet_files)} parquet files found")
                    # Show last few exports
                    for line in parquet_files[-3:]:
                        parts = line.split()
                        if len(parts) >= 9:
                            filename = parts[-1]
                            size = parts[4]
                            print(f"      {filename} ({size} bytes)")
                else:
                    print("    No parquet files found")
        except Exception:
            print("  Export directory: Unable to check")
        
        print("\n" + "="*80)
        print("Verification completed successfully!")
        print("\nNote: This verification checks the new table structure:")
        print("  - btc_current, eth_current, sol_current (active data)")
        print("  - export_log (tracks hourly parquet exports)")
        print("  - Hourly rotation exports data to parquet files")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        if "Connection refused" in str(e):
            print("💡 Hint: Is ClickHouse container running? Try: docker-compose up -d")
        elif "doesn't exist" in str(e):
            print("💡 Hint: Tables missing? Try: python setup_database.py")
        sys.exit(1)
    finally:
        if client:
            client.disconnect()

if __name__ == "__main__":
    verify_data()