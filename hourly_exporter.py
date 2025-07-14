#!/usr/bin/env python3
"""
Hourly Data Exporter with Container Uptime Monitoring

This service runs every hour and:
1. Checks if client containers ran continuously for the previous hour
2. Exports complete hourly data to Parquet files per asset
3. Verifies the export integrity
4. Cleans up the exported data from ClickHouse
"""

import os
import sys
import time
import docker
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
)

# Export configuration
EXPORT_DIR = "/exports"  # Will be mounted as volume
SYMBOLS = ["btc", "eth", "sol"]
CONTAINER_NAMES = {
    "btc": "mexc-btc-client",
    "eth": "mexc-eth-client", 
    "sol": "mexc-sol-client"
}

class HourlyExporter:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.ch_client = None
        self.connect_clickhouse()
        
    def connect_clickhouse(self):
        """Connect to ClickHouse database."""
        try:
            self.ch_client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            print("✅ Connected to ClickHouse")
        except Exception as e:
            print(f"❌ ClickHouse connection failed: {e}")
            sys.exit(1)
    
    def get_container_uptime(self, container_name, hour_start, hour_end):
        """Check if container was running continuously during the specified hour."""
        try:
            container = self.docker_client.containers.get(container_name)
            
            # Get container state
            state = container.attrs['State']
            if not state['Running']:
                print(f"❌ {container_name}: Not currently running")
                return False
            
            # Parse container start time
            started_at = datetime.fromisoformat(state['StartedAt'].replace('Z', '+00:00'))
            started_at = started_at.replace(tzinfo=None)  # Remove timezone for comparison
            
            # Check if container started before hour_start
            if started_at > hour_start:
                print(f"❌ {container_name}: Started at {started_at}, after hour start {hour_start}")
                return False
                
            print(f"✅ {container_name}: Running continuously since {started_at}")
            return True
            
        except docker.errors.NotFound:
            print(f"❌ {container_name}: Container not found")
            return False
        except Exception as e:
            print(f"❌ {container_name}: Error checking uptime: {e}")
            return False
    
    def get_hourly_data(self, symbol, hour_start, hour_end):
        """Fetch data for specific hour from ClickHouse."""
        query = f"""
        SELECT 
            ts,
            mt,
            m
        FROM {symbol}
        WHERE ts >= '{hour_start.strftime('%Y-%m-%d %H:%M:%S')}'
          AND ts < '{hour_end.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY ts
        """
        
        try:
            result = self.ch_client.execute(query)
            return result
        except Exception as e:
            print(f"❌ Error fetching {symbol} data: {e}")
            return []
    
    def export_to_parquet(self, symbol, data, hour_start):
        """Export data to Parquet file."""
        if not data:
            print(f"⚠️  No data to export for {symbol}")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
        
        # Convert mt enum to string for better compatibility
        mt_map = {1: 't', 2: 'd', 3: 'dp', 4: 'dl'}
        df['mt'] = df['mt'].map(mt_map)
        
        # Create filename: symbol_YYYYMMDD_HH00.parquet
        filename = f"{symbol}_{hour_start.strftime('%Y%m%d_%H')}00.parquet"
        filepath = os.path.join(EXPORT_DIR, filename)
        
        # Ensure export directory exists
        os.makedirs(EXPORT_DIR, exist_ok=True)
        
        # Write Parquet file with compression
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, filepath, compression='snappy')
            
            # Verify file was created and has data
            file_size = os.path.getsize(filepath)
            read_table = pq.read_table(filepath)
            row_count = read_table.num_rows
            
            print(f"✅ Exported {symbol}: {filename} ({row_count} rows, {file_size:,} bytes)")
            return filepath
            
        except Exception as e:
            print(f"❌ Failed to export {symbol} to Parquet: {e}")
            return None
    
    def verify_export(self, filepath, original_count):
        """Verify the exported Parquet file contains the expected data."""
        try:
            table = pq.read_table(filepath)
            exported_count = table.num_rows
            
            if exported_count == original_count:
                print(f"✅ Verification passed: {exported_count} rows in Parquet")
                return True
            else:
                print(f"❌ Verification failed: Expected {original_count} rows, found {exported_count}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to verify export: {e}")
            return False
    
    def delete_exported_data(self, symbol, hour_start, hour_end):
        """Mark exported data as processed (StripeLog doesn't support DELETE)."""
        # StripeLog is append-only and doesn't support DELETE operations
        # Instead, we'll track exported hours in the export_log table
        # Future enhancement: Create new tables periodically and drop old ones
        
        try:
            # For now, just verify the data was exported
            count_query = f"""
            SELECT count(*) FROM {symbol}
            WHERE ts >= '{hour_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND ts < '{hour_end.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            exported_count = self.ch_client.execute(count_query)[0][0]
            
            print(f"ℹ️  {symbol}: {exported_count} rows exported (StripeLog tables are append-only)")
            print(f"ℹ️  Data remains in ClickHouse but is marked as exported in export_log")
            return True
                
        except Exception as e:
            print(f"❌ Failed to verify {symbol} export: {e}")
            return False
    
    def is_hour_exported(self, symbol, hour_start):
        """Check if this hour has already been exported."""
        try:
            result = self.ch_client.execute(f"""
            SELECT count(*) FROM export_log
            WHERE symbol = '{symbol}'
              AND hour_start = '{hour_start.strftime('%Y-%m-%d %H:%M:%S')}'
            """)
            return result[0][0] > 0
        except Exception as e:
            print(f"⚠️  Failed to check export status: {e}")
            return False
    
    def record_export(self, symbol, hour_start, filepath, row_count):
        """Record successful export in tracking table."""
        try:
            self.ch_client.execute("""
            INSERT INTO export_log (symbol, hour_start, export_time, filepath, row_count)
            VALUES
            """, [(symbol, hour_start, datetime.now(), filepath, row_count)])
            print(f"✅ Recorded export in tracking table")
        except Exception as e:
            print(f"⚠️  Failed to record export: {e}")
    
    def process_hour(self, hour_start, hour_end):
        """Process exports for all symbols for a specific hour."""
        print(f"\n{'='*60}")
        print(f"Processing hour: {hour_start.strftime('%Y-%m-%d %H:00')} to {hour_end.strftime('%Y-%m-%d %H:00')}")
        print(f"{'='*60}")
        
        for symbol in SYMBOLS:
            container_name = CONTAINER_NAMES[symbol]
            print(f"\n📊 Processing {symbol.upper()} ({container_name})")
            
            # Check if already exported
            if self.is_hour_exported(symbol, hour_start):
                print(f"⏭️  Skipping {symbol} - hour already exported")
                continue
            
            # Check container uptime
            if not self.get_container_uptime(container_name, hour_start, hour_end):
                print(f"⏭️  Skipping {symbol} - container didn't run full hour")
                continue
            
            # Get data for the hour
            data = self.get_hourly_data(symbol, hour_start, hour_end)
            if not data:
                print(f"⏭️  Skipping {symbol} - no data for this hour")
                continue
            
            original_count = len(data)
            print(f"📥 Found {original_count} records to export")
            
            # Export to Parquet
            filepath = self.export_to_parquet(symbol, data, hour_start)
            if not filepath:
                print(f"❌ Export failed for {symbol}")
                continue
            
            # Verify export
            if not self.verify_export(filepath, original_count):
                print(f"❌ Verification failed for {symbol} - skipping deletion")
                continue
            
            # Mark as exported (StripeLog doesn't support deletion)
            if self.delete_exported_data(symbol, hour_start, hour_end):
                # Record successful export
                self.record_export(symbol, hour_start, filepath, original_count)
                print(f"✅ Successfully exported {symbol} for hour {hour_start.strftime('%Y-%m-%d %H:00')}")
            else:
                print(f"⚠️  Export completed but verification failed for {symbol}")
    
    def get_last_processed_hour(self):
        """Get the last successfully processed hour from export log."""
        try:
            result = self.ch_client.execute("""
            SELECT max(hour_start) FROM export_log
            """)
            if result and result[0][0]:
                return result[0][0]
        except:
            pass
        return None
    
    def run_once(self):
        """Run export for the previous complete hour."""
        # Calculate previous hour boundaries
        now = datetime.now()
        hour_end = now.replace(minute=0, second=0, microsecond=0)
        hour_start = hour_end - timedelta(hours=1)
        
        print(f"Current time: {now}")
        print(f"Processing data from {hour_start} to {hour_end}")
        
        self.process_hour(hour_start, hour_end)
    
    def run_continuous(self):
        """Run continuously, processing each hour after it completes."""
        print("🚀 Starting hourly exporter service")
        
        while True:
            now = datetime.now()
            
            # Calculate next hour boundary
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            
            # Wait until 1 minute after the hour to ensure data completeness
            wait_until = next_hour + timedelta(minutes=1)
            wait_seconds = (wait_until - now).total_seconds()
            
            if wait_seconds > 0:
                print(f"\n⏰ Waiting {wait_seconds:.0f} seconds until {wait_until} for next export")
                time.sleep(wait_seconds)
            
            # Process the previous hour
            self.run_once()
            
            # Short sleep to prevent tight loop
            time.sleep(10)

def main():
    """Main entry point."""
    exporter = HourlyExporter()
    
    # Check if this is a one-time run or continuous
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        print("Running one-time export for previous hour")
        exporter.run_once()
    else:
        print("Starting continuous hourly export service")
        exporter.run_continuous()

if __name__ == "__main__":
    main()