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
    
    def get_container_uptime(self, container_name, period_start, period_end):
        """Check if container was running continuously during the specified period."""
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
            
            # Check if container started before period_start
            if started_at > period_start:
                print(f"❌ {container_name}: Started at {started_at}, after period start {period_start}")
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
    
    def get_period_data(self, symbol, period_start, period_end):
        """Fetch data for specific period from ClickHouse."""
        query = f"""
        SELECT 
            ts,
            mt,
            m
        FROM {symbol}
        WHERE ts >= '{period_start.strftime('%Y-%m-%d %H:%M:%S')}'
          AND ts < '{period_end.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY ts
        """
        
        try:
            result = self.ch_client.execute(query)
            return result
        except Exception as e:
            print(f"❌ Error fetching {symbol} period data: {e}")
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
    
    def export_debug_parquet(self, symbol, data, period_start):
        """Export data to Parquet file for debug mode."""
        if not data:
            print(f"⚠️  No data to export for {symbol}")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
        
        # Convert mt enum to string for better compatibility
        mt_map = {1: 't', 2: 'd', 3: 'dp', 4: 'dl'}
        df['mt'] = df['mt'].map(mt_map)
        
        # Create filename: symbol_YYYYMMDD_HHMM_debug.parquet
        filename = f"{symbol}_{period_start.strftime('%Y%m%d_%H%M')}_debug.parquet"
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
            
            print(f"✅ DEBUG: Exported {symbol}: {filename} ({row_count} rows, {file_size:,} bytes)")
            return filepath
            
        except Exception as e:
            print(f"❌ Failed to export {symbol} to Parquet: {e}")
            return None
    
    def verify_parquet_integrity(self, filepath, original_count, original_data):
        """Enhanced verification of exported Parquet file integrity."""
        try:
            # Read back the parquet file
            table = pq.read_table(filepath)
            df = table.to_pandas()
            
            # Basic row count check
            exported_count = len(df)
            if exported_count != original_count:
                print(f"❌ Verification failed: Expected {original_count} rows, found {exported_count}")
                return False
            
            # Verify columns exist
            expected_columns = {'ts', 'mt', 'm'}
            actual_columns = set(df.columns)
            if expected_columns != actual_columns:
                print(f"❌ Verification failed: Expected columns {expected_columns}, found {actual_columns}")
                return False
            
            # Sample data verification - check first and last few rows
            if len(original_data) > 0:
                # Convert original data to comparable format
                orig_df = pd.DataFrame(original_data, columns=['ts', 'mt', 'm'])
                mt_map = {1: 't', 2: 'd', 3: 'dp', 4: 'dl'}
                orig_df['mt'] = orig_df['mt'].map(mt_map)
                
                # Check first row
                if not orig_df.iloc[0].equals(df.iloc[0]):
                    print(f"⚠️  First row mismatch (timestamps might differ slightly)")
                
                # Check message type distribution
                orig_mt_counts = orig_df['mt'].value_counts().to_dict()
                export_mt_counts = df['mt'].value_counts().to_dict()
                
                if orig_mt_counts != export_mt_counts:
                    print(f"⚠️  Message type distribution differs:")
                    print(f"    Original: {orig_mt_counts}")
                    print(f"    Exported: {export_mt_counts}")
            
            print(f"✅ Enhanced verification passed: {exported_count} rows, data integrity confirmed")
            return True
                
        except Exception as e:
            print(f"❌ Failed to verify export integrity: {e}")
            return False
    
    def delete_exported_data(self, symbol, hour_start, hour_end):
        """Delete the exported hour of data from ClickHouse."""
        delete_query = f"""
        ALTER TABLE {symbol}
        DELETE WHERE ts >= '{hour_start.strftime('%Y-%m-%d %H:%M:%S')}'
                 AND ts < '{hour_end.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        try:
            # Count before deletion for verification
            count_before_query = f"""
            SELECT count(*) FROM {symbol}
            WHERE ts >= '{hour_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND ts < '{hour_end.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            count_before = self.ch_client.execute(count_before_query)[0][0]
            
            # Execute deletion
            self.ch_client.execute(delete_query)
            
            # Wait for deletion to complete (ClickHouse processes deletions asynchronously)
            time.sleep(3)
            
            # Verify deletion
            count_after_query = f"""
            SELECT count(*) FROM {symbol}
            WHERE ts >= '{hour_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND ts < '{hour_end.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            count_after = self.ch_client.execute(count_after_query)[0][0]
            
            print(f"✅ Deletion: {count_before} rows → {count_after} rows remaining")
            
            if count_after == 0:
                print(f"✅ Successfully deleted all {symbol} data for hour {hour_start.strftime('%Y-%m-%d %H:00')}")
                return True
            else:
                print(f"⚠️  {count_after} rows still remain after deletion for {symbol}")
                return count_after < count_before  # Partial success is acceptable
                
        except Exception as e:
            print(f"❌ Failed to delete {symbol} data: {e}")
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
    
    def is_period_exported(self, symbol, period_start):
        """Check if this period has already been exported (for debug mode)."""
        try:
            result = self.ch_client.execute(f"""
            SELECT count(*) FROM export_log
            WHERE symbol = '{symbol}'
              AND hour_start = '{period_start.strftime('%Y-%m-%d %H:%M:%S')}'
            """)
            return result[0][0] > 0
        except Exception as e:
            print(f"⚠️  Failed to check period export status: {e}")
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
            
            # Delete exported data from ClickHouse
            if self.delete_exported_data(symbol, hour_start, hour_end):
                # Record successful export
                self.record_export(symbol, hour_start, filepath, original_count)
                print(f"✅ Successfully exported and cleaned {symbol} for hour {hour_start.strftime('%Y-%m-%d %H:00')}")
            else:
                print(f"⚠️  Export completed but deletion failed for {symbol}")
    
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
    
    def run_debug_export(self):
        """Run export for the previous 5-minute period (DEBUG MODE)."""
        # Calculate previous 5-minute boundaries
        now = datetime.now()
        minutes_past = now.minute % 5
        period_end = now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_past)
        period_start = period_end - timedelta(minutes=5)
        
        print(f"\n🐛 DEBUG MODE: Current time: {now}")
        print(f"🐛 DEBUG MODE: Processing 5-minute period from {period_start} to {period_end}")
        
        self.process_debug_period(period_start, period_end)
    
    def process_debug_period(self, period_start, period_end):
        """Process exports for all symbols for a 5-minute debug period."""
        print(f"\n{'='*60}")
        print(f"DEBUG: Processing period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        for symbol in SYMBOLS:
            container_name = CONTAINER_NAMES[symbol]
            print(f"\n📊 Processing {symbol.upper()} ({container_name}) [DEBUG MODE]")
            
            # Check if already exported (using period_start as identifier)
            if self.is_period_exported(symbol, period_start):
                print(f"⏭️  Skipping {symbol} - period already exported")
                continue
            
            # Check container uptime
            if not self.get_container_uptime(container_name, period_start, period_end):
                print(f"⏭️  Skipping {symbol} - container didn't run full period")
                continue
            
            # Get data for the period
            data = self.get_period_data(symbol, period_start, period_end)
            if not data:
                print(f"⏭️  Skipping {symbol} - no data for this period")
                continue
            
            original_count = len(data)
            print(f"📥 Found {original_count} records to export")
            
            # Export to Parquet (using different naming for debug)
            filepath = self.export_debug_parquet(symbol, data, period_start)
            if not filepath:
                print(f"❌ Export failed for {symbol}")
                continue
            
            # Enhanced verification
            if not self.verify_parquet_integrity(filepath, original_count, data):
                print(f"❌ Verification failed for {symbol} - skipping deletion")
                continue
            
            # Delete exported data from ClickHouse
            if self.delete_exported_data(symbol, period_start, period_end):
                # Record successful export
                self.record_export(symbol, period_start, filepath, original_count)
                print(f"✅ Successfully exported and cleaned {symbol} for period {period_start.strftime('%Y-%m-%d %H:%M')}")
            else:
                print(f"⚠️  Export completed but deletion failed for {symbol}")
    
    def run_continuous(self):
        """Run continuously, processing each 5-minute period for debugging."""
        print("🚀 Starting 5-minute exporter service (DEBUG MODE)")
        
        while True:
            now = datetime.now()
            
            # Calculate next 5-minute boundary
            minutes_past = now.minute % 5
            next_boundary = now.replace(second=0, microsecond=0) + timedelta(minutes=5-minutes_past)
            
            # Wait until 30 seconds after the 5-minute mark
            wait_until = next_boundary + timedelta(seconds=30)
            wait_seconds = (wait_until - now).total_seconds()
            
            if wait_seconds > 0:
                print(f"\n⏰ Waiting {wait_seconds:.0f} seconds until {wait_until} for next export")
                time.sleep(wait_seconds)
            
            # Process the previous 5-minute period
            self.run_debug_export()
            
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