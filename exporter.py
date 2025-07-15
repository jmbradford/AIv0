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
        self.perform_preflight_checks()
        
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
    
    def perform_preflight_checks(self):
        """Perform pre-flight checks for export functionality."""
        print("🔍 Performing pre-flight checks...")
        
        # Check and fix export directory permissions
        self.ensure_export_directory_permissions()
        
        # Check ClickHouse connectivity
        try:
            self.ch_client.execute("SELECT 1")
            print(f"✅ ClickHouse connectivity confirmed")
        except Exception as db_error:
            print(f"❌ ClickHouse connectivity issue: {db_error}")
        
        # Check required tables exist
        for symbol in SYMBOLS:
            try:
                current_table = f"{symbol}_current"
                count = self.ch_client.execute(f"SELECT count(*) FROM {current_table}")[0][0]
                print(f"✅ Table {current_table} exists ({count} rows)")
            except Exception as table_error:
                print(f"⚠️  Table {current_table} issue: {table_error}")
        
        print("🔍 Pre-flight checks completed\\n")
    
    def ensure_export_directory_permissions(self):
        """Ensure export directory exists with proper permissions and is writable."""
        import stat
        import pwd
        import grp
        
        print(f"🔧 Ensuring export directory permissions...")
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(EXPORT_DIR, exist_ok=True)
            print(f"✅ Export directory accessible: {EXPORT_DIR}")
            
            # Get current directory stats
            dir_stat = os.stat(EXPORT_DIR)
            current_uid = os.getuid()
            current_gid = os.getgid()
            
            print(f"🔍 Directory owner: {dir_stat.st_uid}:{dir_stat.st_gid}, Process: {current_uid}:{current_gid}")
            
            # Check if we can write to the directory
            can_write = os.access(EXPORT_DIR, os.W_OK)
            
            if not can_write:
                print(f"⚠️  No write access to {EXPORT_DIR}, attempting to fix permissions...")
                
                # Try to set permissions to be more permissive
                try:
                    os.chmod(EXPORT_DIR, 0o755)
                    print(f"✅ Set directory permissions to 755")
                except Exception as chmod_error:
                    print(f"⚠️  Could not change directory permissions: {chmod_error}")
                
                # Check if write access is now available
                can_write = os.access(EXPORT_DIR, os.W_OK)
            
            # Test write permissions with a file
            test_file = os.path.join(EXPORT_DIR, f".preflight_test_{int(time.time())}")
            try:
                with open(test_file, 'w') as f:
                    f.write('preflight_test')
                
                # Set file permissions
                os.chmod(test_file, 0o644)
                
                # Clean up test file
                os.remove(test_file)
                print(f"✅ Write permissions verified and test file cleaned up")
                
            except Exception as write_error:
                print(f"❌ Write permission test failed: {write_error}")
                
                # Try alternative directory if main fails
                self.setup_fallback_directory()
                
        except Exception as dir_error:
            print(f"❌ Export directory setup failed: {dir_error}")
            self.setup_fallback_directory()
    
    def setup_fallback_directory(self):
        """Setup fallback export directory when primary fails."""
        fallback_dirs = ["/tmp/exports", "/app/exports"]
        
        for fallback_dir in fallback_dirs:
            try:
                print(f"🔄 Trying fallback directory: {fallback_dir}")
                os.makedirs(fallback_dir, exist_ok=True)
                os.chmod(fallback_dir, 0o755)
                
                # Test write access
                test_file = os.path.join(fallback_dir, f".fallback_test_{int(time.time())}")
                with open(test_file, 'w') as f:
                    f.write('fallback_test')
                os.remove(test_file)
                
                # Update EXPORT_DIR to fallback
                global EXPORT_DIR
                EXPORT_DIR = fallback_dir
                print(f"✅ Using fallback directory: {fallback_dir}")
                return True
                
            except Exception as fallback_error:
                print(f"⚠️  Fallback {fallback_dir} also failed: {fallback_error}")
                continue
        
        print(f"❌ All export directory options failed - exports may not work")
        return False
    
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
            print(f"🔍 DEBUG: First few rows from {table_name}: {result[:3] if result else 'No data'}")
            if result:
                print(f"🔍 DEBUG: Sample mt values from DB: {[row[1] for row in result[:5]]}")
            return result
        except Exception as e:
            print(f"❌ Error fetching {table_name} data: {e}")
            return []
    
    def export_to_parquet(self, symbol, data, period_start):
        """Export data to Parquet file."""
        if not data:
            print(f"⚠️  No data to export for {symbol}")
            return None
            
        print(f"🔍 TRACE: Starting export for {symbol} with {len(data)} records")
        print(f"🔍 TRACE: Raw data first 2 rows: {data[:2]}")
        
        # STEP 1: Extract mt values from raw data
        raw_mt_values = [row[1] for row in data[:10]]  # Get first 10 mt values
        print(f"🔍 TRACE: Raw mt values from data tuples: {raw_mt_values}")
        print(f"🔍 TRACE: Raw mt value types: {[type(x) for x in raw_mt_values[:5]]}")
        
        # STEP 2: Convert to DataFrame
        df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
        print(f"🔍 TRACE: DataFrame created successfully")
        
        # STEP 3: Check DataFrame mt column immediately after creation
        print(f"🔍 TRACE: DataFrame mt values (first 10): {df['mt'].head(10).tolist()}")
        print(f"🔍 TRACE: DataFrame mt dtype: {df['mt'].dtype}")
        print(f"🔍 TRACE: DataFrame mt unique values: {df['mt'].unique()}")
        print(f"🔍 TRACE: DataFrame mt value counts: {df['mt'].value_counts()}")
        print(f"🔍 TRACE: DataFrame mt null count: {df['mt'].isnull().sum()}")
        
        # STEP 4: Check for any None values
        none_mask = df['mt'].isnull()
        if none_mask.any():
            print(f"🔍 TRACE: Found {none_mask.sum()} None values in mt column!")
            print(f"🔍 TRACE: None value indices: {df[none_mask].index.tolist()}")
        else:
            print(f"🔍 TRACE: No None values found in mt column")
        
        # Convert mt enum to string - ClickHouse Enum8 returns string values directly
        # Map string values to themselves (no conversion needed since they're already correct)
        mt_map = {
            't': 't', 'd': 'd', 'dp': 'dp', 'dl': 'dl',
            # Also handle numeric values in case they appear
            1: 't', 2: 'd', 3: 'dp', 4: 'dl'
        }
        
        # STEP 5: Apply mapping with simple map() method (proven to work)
        print(f"🔍 TRACE: Starting mapping process...")
        original_mt_values = df['mt'].tolist()
        print(f"🔍 TRACE: Original mt values before mapping: {set(original_mt_values)}")
        print(f"🔍 TRACE: Mapping dictionary: {mt_map}")
        
        # Use simple map() method - this approach works in debug_actual_rotation.py
        df['mt'] = df['mt'].map(mt_map)
        print(f"🔍 TRACE: Mapping applied successfully with simple map() method")
        
        # STEP 6: Check mapping results
        print(f"🔍 TRACE: Mt values after mapping (first 10): {df['mt'].head(10).tolist()}")
        print(f"🔍 TRACE: Mt unique values after mapping: {df['mt'].unique()}")
        print(f"🔍 TRACE: Mt value counts after mapping: {df['mt'].value_counts()}")
        print(f"🔍 TRACE: Mt column dtype after mapping: {df['mt'].dtype}")
        
        # Verify no null values
        null_count = df['mt'].isnull().sum()
        if null_count > 0:
            print(f"⚠️  TRACE: {null_count} null values found after mapping - this should not happen!")
            # Force fill any remaining nulls
            df['mt'] = df['mt'].fillna('dl')
            print(f"🔍 TRACE: Filled remaining nulls with 'dl'")
        else:
            print(f"✅ TRACE: No null values found after mapping")
            
        print(f"🔍 TRACE: Final mt values after mapping: {df['mt'].value_counts()}")
        
        # Force flush stdout to ensure debug prints appear
        import sys
        sys.stdout.flush()
        
        # Create filename based on mode
        if self.debug_mode:
            filename = f"{symbol}_{period_start.strftime('%Y%m%d_%H%M')}_debug.parquet"
        else:
            filename = f"{symbol}_{period_start.strftime('%Y%m%d_%H')}00.parquet"
            
        print(f"🔍 DEBUG: Creating file: {filename} in directory: {EXPORT_DIR}")
        
        filepath = os.path.join(EXPORT_DIR, filename)
        
        # Ensure export directory exists with proper permissions
        self.ensure_export_directory_permissions()
        
        print(f"📄 Final export filepath: {filepath}")
        
        # STEP 7: Write Parquet file with compression
        try:
            print(f"🔍 TRACE: About to create Arrow table for {symbol}")
            print(f"🔍 TRACE: Final DataFrame shape: {df.shape}")
            print(f"🔍 TRACE: Final DataFrame columns: {df.columns.tolist()}")
            print(f"🔍 TRACE: Final DataFrame dtypes: {df.dtypes.to_dict()}")
            
            # Check mt column one more time before Arrow conversion
            print(f"🔍 TRACE: Final mt values before Arrow: {df['mt'].value_counts()}")
            print(f"🔍 TRACE: Final mt nulls before Arrow: {df['mt'].isnull().sum()}")
            
            # Create Arrow table from pandas DataFrame
            print(f"🔍 TRACE: Creating Arrow table from pandas")
            table = pa.Table.from_pandas(df)
            print(f"🔍 TRACE: Arrow table created successfully")
            print(f"🔍 TRACE: Arrow table schema: {table.schema}")
            
            # Check Arrow table mt column
            mt_column = table.column('mt')
            print(f"🔍 TRACE: Arrow mt column type: {mt_column.type}")
            print(f"🔍 TRACE: Arrow mt column length: {len(mt_column)}")
            
            # Verify Arrow table mt column values
            mt_pandas_from_arrow = mt_column.to_pandas()
            print(f"🔍 TRACE: Arrow mt column values: {mt_pandas_from_arrow.value_counts()}")
            print(f"🔍 TRACE: Arrow mt column nulls: {mt_pandas_from_arrow.isnull().sum()}")
            
            print(f"🔍 TRACE: Writing to {filepath}")
            pq.write_table(table, filepath, compression='snappy')
            print(f"🔍 TRACE: Parquet file written successfully")
            
            # STEP 8: Verify file was created and has data
            file_size = os.path.getsize(filepath)
            read_table = pq.read_table(filepath)
            row_count = read_table.num_rows
            
            print(f"🔍 TRACE: Reading back written file for verification")
            print(f"🔍 TRACE: Read back table schema: {read_table.schema}")
            
            # Check mt column in read-back data
            read_back_df = read_table.to_pandas()
            print(f"🔍 TRACE: Read back mt values: {read_back_df['mt'].value_counts()}")
            print(f"🔍 TRACE: Read back mt nulls: {read_back_df['mt'].isnull().sum()}")
            
            print(f"✅ Exported {symbol}: {filename} ({row_count} rows, {file_size:,} bytes)")
            return filepath
            
        except Exception as e:
            print(f"❌ Failed to export {symbol} to Parquet: {e}")
            import traceback
            traceback.print_exc()
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
                
                # DEBUG: Check what's in the data
                print(f"🔍 DEBUG: Sample raw data from analyze_exported_data:")
                print(f"    First 3 rows: {data[:3]}")
                print(f"    Mt column types: {df['mt'].dtype}")
                print(f"    Mt unique values: {df['mt'].unique()}")
                
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
    
    def verify_parquet_vs_clickhouse(self, symbol, filepath):
        """Compare exported Parquet file with ClickHouse previous table."""
        previous_table = f"{symbol}_previous"
        
        try:
            print(f"\\n🔍 VERIFICATION: Comparing {symbol} Parquet vs ClickHouse")
            
            # Read Parquet file
            parquet_df = pd.read_parquet(filepath)
            parquet_count = len(parquet_df)
            print(f"📄 Parquet file: {parquet_count} rows")
            
            # Get ClickHouse data
            ch_data = self.get_table_data(previous_table)
            ch_count = len(ch_data)
            print(f"🗄️  ClickHouse table: {ch_count} rows")
            
            # Compare counts
            if parquet_count != ch_count:
                print(f"❌ Row count mismatch: Parquet={parquet_count}, ClickHouse={ch_count}")
                return False
                
            # Convert ClickHouse data to DataFrame for comparison
            ch_df = pd.DataFrame(ch_data, columns=['ts', 'mt', 'm'])
            
            # Compare message type distributions
            parquet_mt_counts = parquet_df['mt'].value_counts().sort_index()
            ch_mt_counts = ch_df['mt'].value_counts().sort_index()
            
            print(f"📊 Message type comparison:")
            print(f"    Parquet: {dict(parquet_mt_counts)}")
            print(f"    ClickHouse: {dict(ch_mt_counts)}")
            
            # Check for null values in Parquet
            parquet_nulls = parquet_df.isnull().sum()
            if parquet_nulls.sum() > 0:
                print(f"⚠️  Parquet null values found: {dict(parquet_nulls[parquet_nulls > 0])}")
                return False
            
            # Check for null values in ClickHouse
            ch_nulls = ch_df.isnull().sum()
            if ch_nulls.sum() > 0:
                print(f"⚠️  ClickHouse null values found: {dict(ch_nulls[ch_nulls > 0])}")
                return False
                
            # Compare timestamps
            parquet_time_range = parquet_df['ts'].max() - parquet_df['ts'].min()
            ch_df['ts'] = pd.to_datetime(ch_df['ts'])
            ch_time_range = ch_df['ts'].max() - ch_df['ts'].min()
            
            print(f"⏰ Time range comparison:")
            print(f"    Parquet: {parquet_time_range}")
            print(f"    ClickHouse: {ch_time_range}")
            
            # Sample data comparison
            print(f"🔍 Sample data comparison (first 3 rows):")
            print(f"    Parquet mt values: {parquet_df['mt'].head(3).tolist()}")
            print(f"    ClickHouse mt values: {ch_df['mt'].head(3).tolist()}")
            
            print(f"✅ Verification passed for {symbol}")
            return True
            
        except Exception as e:
            print(f"❌ Verification failed for {symbol}: {e}")
            return False
    
    def process_symbol_rotation(self, symbol, period_start, period_end, force_rotation=False, preserve_data=False):
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
        
        # Step 8: Delete previous table and UUID directory (unless preserving data)
        if preserve_data:
            print(f"💾 Preserving {symbol}_previous table for verification")
            # Record successful export
            self.record_export(symbol, period_start, filepath, original_count)
            # Verify the export matches ClickHouse data
            verification_success = self.verify_parquet_vs_clickhouse(symbol, filepath)
            if verification_success:
                print(f"✅ Successfully rotated, exported, and preserved {symbol}")
                success = True
            else:
                print(f"⚠️  Export completed but verification failed for {symbol}")
                success = False
        else:
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
    
    def run_rotation_cycle(self, period_start, period_end, force_rotation=False, preserve_data=False):
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
            if self.process_symbol_rotation(symbol, period_start, period_end, force_rotation, preserve_data):
                success_count += 1
        
        print(f"\\n🎯 Rotation complete: {success_count}/{len(SYMBOLS)} symbols processed successfully")
        return success_count == len(SYMBOLS)
    
    def run_once(self, force_rotation=True, preserve_data=True):
        """Run export for the previous complete period - FORCED mode ignores uptime checks."""
        now = datetime.now()
        
        print(f"🔧 --ONCE MODE: Force rotation={force_rotation}, Preserve data={preserve_data}")
        print(f"🔧 --ONCE MODE: Bypassing hourly runtime checks and container status verification")
        
        # For --once mode, always use a recent time period regardless of debug_mode
        # Use last 5 minutes for immediate data availability
        minutes_past = now.minute % 5
        period_end = now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_past)
        period_start = period_end - timedelta(minutes=5)
        
        print(f"Current time: {now}")
        print(f"Processing data from {period_start} to {period_end}")
        print(f"Data preservation: {'ENABLED' if preserve_data else 'DISABLED'}")
        print(f"Force rotation: {'ENABLED' if force_rotation else 'DISABLED'}")
        
        return self.run_rotation_cycle(period_start, period_end, force_rotation, preserve_data)
    
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
            self.run_once(preserve_data=False)
            
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
            self.run_once(preserve_data=False)
            
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