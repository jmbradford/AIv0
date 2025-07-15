#!/usr/bin/env python3
"""
Hourly Table Rotation and Export System

This service rotates StripeLog tables hourly and:
1. Signals clients to activate memory buffers  
2. Renames current tables to previous, creates new current tables
3. Clients reconnect and flush buffers to new tables
4. Exports previous tables to Parquet files
5. Drops previous tables entirely (deletes UUID directories)
6. Provides zero data loss through memory buffer coordination
"""

import os
import sys
import time
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

class HourlyTableRotator:
    def __init__(self):
        self.ch_client = None
        self.debug_mode = os.getenv('EXPORT_DEBUG_MODE', 'false').lower() == 'true'
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
    
    def check_container_status(self, container_name, period_start, period_end):
        """Check if container is likely to have been running during the period."""
        try:
            # Simple alternative: check if the table has recent data
            symbol = container_name.replace('mexc-', '').replace('-client', '')
            current_table = f"{symbol}_current"
            
            # Check if there's data in the current table (indicates container is working)
            recent_count = self.ch_client.execute(f"""
                SELECT count(*) FROM {current_table} 
                WHERE ts >= now() - INTERVAL 5 MINUTE
            """)[0][0]
            
            if recent_count > 0:
                print(f"✅ {container_name}: Active (recent data detected)")
                return True
            else:
                print(f"⚠️  {container_name}: No recent data (last 5 minutes)")
                return False
                
        except Exception as e:
            print(f"❌ {container_name}: Error checking status: {e}")
            return False
    
    def get_buffer_analysis(self, symbol):
        """Analyze buffer activity by checking data patterns."""
        try:
            current_table = f"{symbol}_current"
            
            # Get recent data patterns to understand buffer activity
            recent_data = self.ch_client.execute(f"""
                SELECT 
                    count(*) as total_messages,
                    min(ts) as earliest_message,
                    max(ts) as latest_message,
                    count(DISTINCT mt) as message_types
                FROM {current_table} 
                WHERE ts >= now() - INTERVAL 10 MINUTE
            """)
            
            if recent_data and recent_data[0][0] > 0:
                total, earliest, latest, types = recent_data[0]
                duration = (latest - earliest).total_seconds() if latest and earliest else 0
                rate = total / duration if duration > 0 else 0
                
                print(f"📊 {symbol.upper()} Buffer Analysis:")
                print(f"    Recent messages (10min): {total}")
                print(f"    Message rate: {rate:.2f} msg/sec")
                print(f"    Message types active: {types}")
                return {'status': 'active', 'rate': rate, 'total': total}
            else:
                print(f"⚠️  {symbol.upper()} Buffer Analysis: No recent activity")
                return {'status': 'inactive', 'rate': 0, 'total': 0}
                
        except Exception as e:
            print(f"❌ Failed to analyze {symbol} buffer: {e}")
            return {'status': 'error', 'rate': 0, 'total': 0}
    
    def signal_rotation_start(self, symbol):
        """Signal client to activate memory buffer."""
        flag_file = f"/tmp/{symbol}_rotate"
        try:
            with open(flag_file, 'w') as f:
                f.write(str(datetime.now()))
            print(f"🚨 Rotation signal sent to {symbol} client")
            return True
        except Exception as e:
            print(f"❌ Failed to signal {symbol} rotation: {e}")
            return False
    
    def signal_rotation_complete(self, symbol):
        """Remove rotation signal."""
        flag_file = f"/tmp/{symbol}_rotate"
        try:
            if os.path.exists(flag_file):
                os.remove(flag_file)
            print(f"✅ Rotation signal cleared for {symbol}")
        except Exception as e:
            print(f"⚠️  Failed to clear rotation signal for {symbol}: {e}")
    
    def rotate_table(self, symbol):
        """Rotate current table to previous and create new current table."""
        current_table = f"{symbol}_current"
        previous_table = f"{symbol}_previous"
        
        try:
            # Drop any existing previous table
            self.ch_client.execute(f"DROP TABLE IF EXISTS {previous_table}")
            
            # Rename current to previous (atomic operation)
            self.ch_client.execute(f"RENAME TABLE {current_table} TO {previous_table}")
            print(f"📋 Renamed {current_table} → {previous_table}")
            
            # Create new current table (gets new UUID directory)
            self.ch_client.execute(f"""
            CREATE TABLE {current_table}
            (
                ts DateTime64(3),
                mt Enum8('t' = 1, 'd' = 2, 'dp' = 3, 'dl' = 4),
                m String
            )
            ENGINE = StripeLog
            """)
            print(f"🆕 Created new {current_table} table")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to rotate {symbol} table: {e}")
            return False
    
    def get_table_data(self, table_name):
        """Fetch all data from specified table."""
        query = f"""
        SELECT 
            ts,
            mt,
            m
        FROM {table_name}
        ORDER BY ts
        """
        
        try:
            result = self.ch_client.execute(query)
            return result
        except Exception as e:
            print(f"❌ Error fetching {table_name} data: {e}")
            return []
    
    def export_to_parquet(self, symbol, data, period_start):
        """Export data to Parquet file."""
        if not data:
            print(f"⚠️  No data to export for {symbol}")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
        
        # Convert mt enum to string for better compatibility
        mt_map = {1: 't', 2: 'd', 3: 'dp', 4: 'dl'}
        df['mt'] = df['mt'].map(mt_map)
        
        # Create filename based on mode
        if self.debug_mode:
            filename = f"{symbol}_{period_start.strftime('%Y%m%d_%H%M')}_debug.parquet"
        else:
            filename = f"{symbol}_{period_start.strftime('%Y%m%d_%H')}00.parquet"
        
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
    
    def analyze_exported_data(self, symbol, data, total_count):
        """Analyze the composition of exported data to show buffer effectiveness."""
        try:
            print(f"\n📊 DATA COMPOSITION ANALYSIS for {symbol.upper()}:")
            print(f"    Total records exported: {total_count}")
            
            # Count by message type
            if data and len(data) > 0:
                # Convert to DataFrame for analysis
                df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
                
                # Message type breakdown
                msg_type_counts = df['mt'].value_counts().sort_index()
                print(f"    Message type breakdown:")
                for msg_type, count in msg_type_counts.items():
                    msg_name = {'t': 'ticker', 'd': 'deal', 'dp': 'depth', 'dl': 'deadletter'}.get(msg_type, msg_type)
                    percentage = (count / total_count) * 100
                    print(f"      {msg_type} ({msg_name}): {count:,} records ({percentage:.1f}%)")
                
                # Time analysis
                df['ts'] = pd.to_datetime(df['ts'])
                time_span = df['ts'].max() - df['ts'].min()
                rate = total_count / time_span.total_seconds() if time_span.total_seconds() > 0 else 0
                
                print(f"    Time span: {time_span}")
                print(f"    Collection rate: {rate:.2f} messages/second")
                
                # Check for gaps (potential buffer periods)
                df_sorted = df.sort_values('ts')
                time_diffs = df_sorted['ts'].diff().dropna()
                large_gaps = time_diffs[time_diffs > pd.Timedelta(seconds=5)]
                
                if len(large_gaps) > 0:
                    print(f"    ⚠️  Found {len(large_gaps)} time gaps >5s (potential buffer periods)")
                    print(f"    Largest gap: {large_gaps.max()}")
                else:
                    print(f"    ✅ No significant time gaps detected")
                
                # Show recent vs older data distribution
                median_time = df_sorted['ts'].median()
                recent_count = len(df_sorted[df_sorted['ts'] >= median_time])
                older_count = len(df_sorted[df_sorted['ts'] < median_time])
                print(f"    Data distribution: {older_count} older + {recent_count} recent messages")
                
            else:
                print(f"    ⚠️  No data to analyze")
                
        except Exception as e:
            print(f"    ❌ Analysis failed: {e}")
    
    def delete_previous_table(self, symbol):
        """Delete the previous table completely (removes UUID directory)."""
        previous_table = f"{symbol}_previous"
        
        try:
            # Count rows before deletion for reporting
            count_query = f"SELECT count(*) FROM {previous_table}"
            row_count = self.ch_client.execute(count_query)[0][0]
            
            # Drop table completely (removes UUID directory)
            self.ch_client.execute(f"DROP TABLE {previous_table}")
            
            print(f"🗑️  Deleted {previous_table} table and UUID directory ({row_count} rows)")
            return True
                
        except Exception as e:
            print(f"❌ Failed to delete {previous_table}: {e}")
            return False
    
    def record_export(self, symbol, period_start, filepath, row_count):
        """Record successful export in tracking table."""
        try:
            self.ch_client.execute("""
            INSERT INTO export_log (symbol, hour_start, export_time, filepath, row_count)
            VALUES
            """, [(symbol, period_start, datetime.now(), filepath, row_count)])
            print(f"✅ Recorded export in tracking table")
        except Exception as e:
            print(f"⚠️  Failed to record export: {e}")
    
    def process_symbol_rotation(self, symbol, period_start, period_end, force_rotation=False):
        """Process complete rotation and export for a single symbol."""
        container_name = CONTAINER_NAMES[symbol]
        print(f"\\n📊 Processing {symbol.upper()} rotation ({container_name})")
        
        # Check container status (skip in debug mode or when forced)
        if not self.debug_mode and not force_rotation:
            if not self.check_container_status(container_name, period_start, period_end):
                print(f"⏭️  Skipping {symbol} - container not active during period")
                return False
        elif self.debug_mode:
            print(f"🐛 DEBUG MODE: Skipping status check for {symbol}")
        elif force_rotation:
            print(f"🔧 FORCED MODE: Skipping status check for {symbol}")
        
        # Step 1: Get pre-rotation buffer analysis
        pre_rotation_stats = self.get_buffer_analysis(symbol)
        
        # Step 2: Signal client to activate buffer
        if not self.signal_rotation_start(symbol):
            return False
        
        # Step 3: Wait for buffer activation and monitor
        print(f"⏳ Waiting 2 seconds for {symbol} buffer activation...")
        time.sleep(2)
        
        # Get post-signal stats to see buffer activation
        print(f"🔄 Monitoring {symbol} memory buffer activation...")
        
        # Step 4: Rotate tables
        if not self.rotate_table(symbol):
            self.signal_rotation_complete(symbol)
            return False
        
        # Step 5: Wait for client reconnection and monitor buffer flush
        print(f"⏳ Waiting 3 seconds for {symbol} client reconnection and buffer flush...")
        time.sleep(1)
        
        # Monitor during reconnection
        print(f"🔄 Monitoring {symbol} buffer flush process...")
        time.sleep(2)
        
        # Get post-rotation stats to see buffer flush to new table
        post_rotation_stats = self.get_buffer_analysis(symbol)
        print(f"✅ Post-rotation {symbol.upper()} buffer: {post_rotation_stats['status']}")
        
        # Show buffer effectiveness
        if post_rotation_stats['total'] > 0:
            print(f"🎯 Buffer effectiveness: {post_rotation_stats['total']} messages captured during rotation")
        
        # Step 6: Get data from previous table
        previous_table = f"{symbol}_previous"
        data = self.get_table_data(previous_table)
        if not data:
            print(f"⏭️  No data in {previous_table} to export")
            self.signal_rotation_complete(symbol)
            return False
        
        original_count = len(data)
        print(f"📥 Found {original_count} records to export from {previous_table}")
        
        # Step 6: Analyze data composition
        self.analyze_exported_data(symbol, data, original_count)
        
        # Step 7: Export to Parquet
        filepath = self.export_to_parquet(symbol, data, period_start)
        if not filepath:
            print(f"❌ Export failed for {symbol}")
            self.signal_rotation_complete(symbol)
            return False
        
        # Step 7: Verify export
        if not self.verify_export(filepath, original_count):
            print(f"❌ Verification failed for {symbol} - keeping table")
            self.signal_rotation_complete(symbol)
            return False
        
        # Step 8: Delete previous table and UUID directory
        if self.delete_previous_table(symbol):
            # Record successful export
            self.record_export(symbol, period_start, filepath, original_count)
            print(f"✅ Successfully rotated, exported, and cleaned {symbol}")
            success = True
        else:
            print(f"⚠️  Export completed but table deletion failed for {symbol}")
            success = False
        
        # Step 9: Clear rotation signal
        self.signal_rotation_complete(symbol)
        
        return success
    
    def run_rotation_cycle(self, period_start, period_end, force_rotation=False):
        """Run complete rotation cycle for all symbols."""
        print(f"\\n{'='*70}")
        if force_rotation:
            print(f"🔧 FORCED MODE: Processing immediate rotation")
        elif self.debug_mode:
            print(f"🐛 DEBUG MODE: Processing period: {period_start.strftime('%Y-%m-%d %H:%M')} to {period_end.strftime('%Y-%m-%d %H:%M')}")
        else:
            print(f"⏰ PRODUCTION: Processing hour: {period_start.strftime('%Y-%m-%d %H:00')} to {period_end.strftime('%Y-%m-%d %H:00')}")
        print(f"{'='*70}")
        
        success_count = 0
        for symbol in SYMBOLS:
            if self.process_symbol_rotation(symbol, period_start, period_end, force_rotation):
                success_count += 1
        
        print(f"\\n🎯 Rotation complete: {success_count}/{len(SYMBOLS)} symbols processed successfully")
        return success_count == len(SYMBOLS)
    
    def run_once(self, force_rotation=True):
        """Run export for the previous complete period - FORCED mode ignores uptime checks."""
        now = datetime.now()
        
        if self.debug_mode:
            # Debug: previous 5-minute period
            minutes_past = now.minute % 5
            period_end = now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_past)
            period_start = period_end - timedelta(minutes=5)
        else:
            # Production: previous hour (but forced)
            period_end = now.replace(minute=0, second=0, microsecond=0)
            period_start = period_end - timedelta(hours=1)
        
        print(f"Current time: {now}")
        print(f"Processing data from {period_start} to {period_end}")
        
        return self.run_rotation_cycle(period_start, period_end, force_rotation)
    
    def run_continuous(self):
        """Run continuously, processing at configured intervals."""
        if self.debug_mode:
            print("🚀 Starting 5-minute table rotator service (DEBUG MODE)")
            self._run_debug_continuous()
        else:
            print("🚀 Starting hourly table rotator service (PRODUCTION MODE)")
            self._run_production_continuous()
    
    def _run_debug_continuous(self):
        """Run debug mode with 5-minute intervals."""
        while True:
            now = datetime.now()
            
            # Calculate next 5-minute boundary
            minutes_past = now.minute % 5
            next_boundary = now.replace(second=0, microsecond=0) + timedelta(minutes=5-minutes_past)
            
            # Wait until 30 seconds after the 5-minute mark
            wait_until = next_boundary + timedelta(seconds=30)
            wait_seconds = (wait_until - now).total_seconds()
            
            if wait_seconds > 0:
                print(f"\\n⏰ [DEBUG] Waiting {wait_seconds:.0f} seconds until {wait_until} for next rotation")
                time.sleep(wait_seconds)
            
            # Process the previous 5-minute period
            self.run_once()
            
            # Short sleep to prevent tight loop
            time.sleep(10)
    
    def _run_production_continuous(self):
        """Run production mode with hourly intervals."""
        while True:
            now = datetime.now()
            
            # Calculate next hour boundary
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            
            # Wait until 1 minute after the hour to ensure data completeness
            wait_until = next_hour + timedelta(minutes=1)
            wait_seconds = (wait_until - now).total_seconds()
            
            if wait_seconds > 0:
                print(f"\\n⏰ [PRODUCTION] Waiting {wait_seconds:.0f} seconds until {wait_until} for next rotation")
                time.sleep(wait_seconds)
            
            # Process the previous hour
            self.run_once()
            
            # Short sleep to prevent tight loop
            time.sleep(10)

def main():
    """Main entry point."""
    rotator = HourlyTableRotator()
    
    # Check if this is a one-time run or continuous
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        print("Running one-time table rotation and export")
        success = rotator.run_once()
        sys.exit(0 if success else 1)
    else:
        mode = "DEBUG" if rotator.debug_mode else "PRODUCTION"
        print(f"Starting continuous table rotation service in {mode} mode")
        rotator.run_continuous()

if __name__ == "__main__":
    main()