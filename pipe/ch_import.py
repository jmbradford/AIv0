# ch_import.py
# Import Parquet files to ClickHouse with deduplication logic
# Only imports missing data, preserves newer records to avoid overwriting live data

import argparse
import logging
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from clickhouse_driver import Client
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickHouseImporter:
    def __init__(self):
        self.client = self.connect_to_clickhouse()
        self.table_schemas = self.get_table_schemas()
    
    def connect_to_clickhouse(self) -> Client:
        """Establish connection to ClickHouse"""
        try:
            client = Client(
                host=config.CLICKHOUSE_HOST,
                port=config.CLICKHOUSE_PORT,
                user=config.CLICKHOUSE_USER,
                password=config.CLICKHOUSE_PASSWORD,
                database=config.CLICKHOUSE_DB
            )
            client.execute('SELECT 1')
            logger.info("Connected to ClickHouse for import")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def get_table_schemas(self) -> Dict[str, List[str]]:
        """Get column definitions for all tables"""
        return {
            'deals': ['timestamp', 'symbol', 'price', 'volume', 'side'],
            'klines': ['timestamp', 'symbol', 'kline_type', 'open', 'close', 'high', 'low', 'volume'],
            'tickers': ['timestamp', 'symbol', 'last_price', 'bid1_price', 'ask1_price', 'volume_24h', 'hold_vol', 'fair_price_from_ticker', 'index_price_from_ticker', 'funding_rate_from_ticker'],
            'depth': ['timestamp', 'symbol', 'version', 'bids', 'asks']
        }
    
    def validate_table(self, table: str) -> bool:
        """Validate table exists and is importable"""
        return table in self.table_schemas
    
    def get_existing_data_range(self, table: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get existing data range for a table/symbol combination"""
        query = f"""
        SELECT 
            COUNT(*) as record_count,
            MIN(timestamp) as min_timestamp,
            MAX(timestamp) as max_timestamp
        FROM {table}
        """
        
        if symbol:
            query += f" WHERE symbol = '{symbol}'"
        
        result = self.client.execute(query)
        
        if result and result[0]:
            count, min_time, max_time = result[0]
            return {
                'table': table,
                'symbol': symbol,
                'record_count': count,
                'min_timestamp': min_time,
                'max_timestamp': max_time,
                'has_data': count > 0
            }
        return {'table': table, 'symbol': symbol, 'record_count': 0, 'has_data': False}
    
    def check_existing_records(self, table: str, df: pd.DataFrame, batch_size: int = 1000) -> pd.DataFrame:
        """Check which records already exist in the database"""
        if df.empty:
            return df
        
        # Create a mask for records that don't exist yet
        exists_mask = pd.Series([False] * len(df), index=df.index)
        
        # Process in batches to avoid large IN clauses
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Build query to check existing records
            conditions = []
            for _, row in batch.iterrows():
                timestamp_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Microseconds to milliseconds
                symbol = row['symbol']
                conditions.append(f"(timestamp = '{timestamp_str}' AND symbol = '{symbol}')")
            
            if conditions:
                query = f"""
                SELECT timestamp, symbol
                FROM {table}
                WHERE {' OR '.join(conditions)}
                """
                
                existing_records = self.client.execute(query)
                
                # Mark existing records in the mask
                for existing_ts, existing_symbol in existing_records:
                    # Find matching records in the batch
                    batch_mask = (
                        (batch['timestamp'] == existing_ts) & 
                        (batch['symbol'] == existing_symbol)
                    )
                    
                    # Update the main mask
                    batch_indices = batch.index[batch_mask]
                    exists_mask.loc[batch_indices] = True
        
        # Return only records that don't exist
        return df[~exists_mask]
    
    def calculate_sequential_cutoff(self, table: str, df: pd.DataFrame) -> Optional[datetime]:
        """Calculate optimal cutoff time based on exact data sequentiality"""
        if df.empty:
            return None
        
        # Get the maximum timestamp in the import file
        file_max_timestamp = df['timestamp'].max()
        
        # Find the first database record after the file's maximum timestamp
        query = f"""
        SELECT MIN(timestamp) as next_timestamp 
        FROM {table} 
        WHERE timestamp > '{file_max_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'
        """
        
        try:
            result = self.client.execute(query)
            next_db_timestamp = result[0][0] if result and result[0] and result[0][0] else None
            
            if next_db_timestamp:
                # Use the minimum of file max and next DB timestamp for perfect sequentiality
                cutoff = min(file_max_timestamp, next_db_timestamp)
                logger.info(f"Sequential cutoff calculated for {table}: {cutoff}")
                logger.info(f"  File max timestamp: {file_max_timestamp}")
                logger.info(f"  Next DB timestamp: {next_db_timestamp}")
                return cutoff
            else:
                # No records after file's max timestamp, use file's max timestamp
                logger.info(f"No records after file max timestamp, using file max: {file_max_timestamp}")
                return file_max_timestamp
        
        except Exception as e:
            logger.error(f"Failed to calculate sequential cutoff for {table}: {e}")
            return file_max_timestamp
    
    def filter_by_cutoff_time(self, df: pd.DataFrame, cutoff_time: Optional[datetime] = None) -> Tuple[pd.DataFrame, int]:
        """Filter out records newer than cutoff time to avoid overwriting live data"""
        if cutoff_time is None or df.empty:
            return df, 0
        
        # Filter out records newer than cutoff
        mask = df['timestamp'] <= cutoff_time
        filtered_df = df[mask]
        filtered_count = len(df) - len(filtered_df)
        
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} records newer than cutoff time {cutoff_time}")
        
        return filtered_df, filtered_count
    
    def prepare_data_for_insert(self, table: str, df: pd.DataFrame) -> List[tuple]:
        """Prepare DataFrame for ClickHouse insertion"""
        if df.empty:
            return []
        
        # Convert DataFrame to list of tuples
        records = []
        for _, row in df.iterrows():
            record = []
            for column in self.table_schemas[table]:
                value = row[column]
                
                # Handle special data types
                if pd.isna(value):
                    record.append(None)
                elif column == 'timestamp':
                    # Ensure timestamp is in correct format
                    if isinstance(value, str):
                        record.append(datetime.fromisoformat(value))
                    else:
                        record.append(value)
                elif column == 'funding_rate_from_ticker' and table == 'tickers':
                    # Handle decimal funding rate
                    if pd.isna(value):
                        record.append(None)
                    else:
                        record.append(float(value))
                else:
                    record.append(value)
            
            records.append(tuple(record))
        
        return records
    
    def insert_records(self, table: str, records: List[tuple]) -> int:
        """Insert records into ClickHouse table"""
        if not records:
            return 0
        
        columns = self.table_schemas[table]
        placeholders = ', '.join(['%s'] * len(columns))
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        
        try:
            self.client.execute(query, records)
            logger.info(f"Inserted {len(records)} records into {table}")
            return len(records)
        except Exception as e:
            logger.error(f"Failed to insert records into {table}: {e}")
            raise
    
    def import_parquet_file(
        self,
        file_path: str,
        table: str,
        cutoff_time: Optional[datetime] = None,
        batch_size: int = 10000,
        check_existing: bool = True,
        auto_cutoff: bool = True
    ) -> Dict[str, Any]:
        """Import Parquet file with deduplication logic"""
        
        if not self.validate_table(table):
            raise ValueError(f"Invalid table: {table}")
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")
        
        logger.info(f"Starting import of {file_path} to {table}")
        
        # Read Parquet file
        df = pd.read_parquet(file_path)
        
        if df.empty:
            logger.warning(f"Parquet file is empty: {file_path}")
            return {'imported_records': 0, 'skipped_records': 0, 'error': 'Empty file'}
        
        original_count = len(df)
        logger.info(f"Read {original_count} records from Parquet file")
        
        # Validate required columns
        required_columns = set(self.table_schemas[table])
        file_columns = set(df.columns)
        
        if not required_columns.issubset(file_columns):
            missing_columns = required_columns - file_columns
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Calculate automatic sequential cutoff if enabled
        if auto_cutoff and cutoff_time is None:
            cutoff_time = self.calculate_sequential_cutoff(table, df)
        
        # Filter by cutoff time if provided or calculated
        cutoff_filtered_count = 0
        if cutoff_time:
            df, cutoff_filtered_count = self.filter_by_cutoff_time(df, cutoff_time)
        
        # Check for existing records if enabled
        duplicate_filtered_count = 0
        if check_existing and not df.empty:
            logger.info("Checking for existing records...")
            original_after_cutoff = len(df)
            df = self.check_existing_records(table, df)
            duplicate_filtered_count = original_after_cutoff - len(df)
            logger.info(f"Found {duplicate_filtered_count} existing records, will skip them")
        
        # Prepare and insert records
        imported_records = 0
        if not df.empty:
            records = self.prepare_data_for_insert(table, df)
            imported_records = self.insert_records(table, records)
        
        total_skipped = cutoff_filtered_count + duplicate_filtered_count
        
        result = {
            'file_path': file_path,
            'table': table,
            'original_records': original_count,
            'imported_records': imported_records,
            'skipped_records': total_skipped,
            'skipped_cutoff': cutoff_filtered_count,
            'skipped_duplicates': duplicate_filtered_count,
            'cutoff_time': cutoff_time.isoformat() if cutoff_time else None
        }
        
        logger.info(f"Import completed: {imported_records} imported, {total_skipped} skipped")
        return result
    
    def import_directory(
        self,
        directory_path: str,
        cutoff_time: Optional[datetime] = None,
        batch_size: int = 10000,
        check_existing: bool = True,
        auto_cutoff: bool = True
    ) -> Dict[str, Any]:
        """Import all Parquet files from a directory"""
        
        directory = Path(directory_path)
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")
        
        parquet_files = list(directory.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No Parquet files found in {directory_path}")
            return {'imported_files': 0, 'total_imported': 0, 'total_skipped': 0}
        
        logger.info(f"Found {len(parquet_files)} Parquet files to import")
        
        results = {}
        total_imported = 0
        total_skipped = 0
        
        for file_path in parquet_files:
            try:
                # Extract table name from filename
                table = None
                for table_name in self.table_schemas.keys():
                    if file_path.name.startswith(table_name):
                        table = table_name
                        break
                
                if not table:
                    logger.warning(f"Could not determine table for file: {file_path.name}")
                    continue
                
                result = self.import_parquet_file(
                    file_path=str(file_path),
                    table=table,
                    cutoff_time=cutoff_time,
                    batch_size=batch_size,
                    check_existing=check_existing,
                    auto_cutoff=auto_cutoff
                )
                
                results[str(file_path)] = result
                total_imported += result['imported_records']
                total_skipped += result['skipped_records']
                
            except Exception as e:
                logger.error(f"Failed to import {file_path}: {e}")
                results[str(file_path)] = {'error': str(e)}
        
        summary = {
            'imported_files': len([r for r in results.values() if 'error' not in r]),
            'total_imported': total_imported,
            'total_skipped': total_skipped,
            'file_results': results
        }
        
        logger.info(f"Directory import completed: {total_imported} total imported, {total_skipped} total skipped")
        return summary

def main():
    parser = argparse.ArgumentParser(description='Import Parquet files to ClickHouse with deduplication')
    
    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--file', type=str, help='Parquet file to import')
    input_group.add_argument('--directory', type=str, help='Directory containing Parquet files to import')
    
    # Table specification (only needed for single file)
    parser.add_argument('--table', type=str, help='Table name (required if using --file)')
    
    # Cutoff time options (optional - auto-calculated by default)
    parser.add_argument('--manual-cutoff', type=str, help='Manual cutoff time override (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--no-auto-cutoff', action='store_true', help='Disable automatic sequential cutoff calculation')
    
    # Processing options
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for processing')
    parser.add_argument('--no-check-existing', action='store_true', help='Skip checking for existing records (faster but may create duplicates)')
    
    # Legacy options (for backward compatibility)
    parser.add_argument('--cutoff-time', type=str, help='Cutoff time - do not import records newer than this (YYYY-MM-DD HH:MM:SS) [DEPRECATED: use --manual-cutoff]')
    parser.add_argument('--cutoff-hours', type=int, help='Set cutoff time to N hours ago [DEPRECATED: use --manual-cutoff]')
    
    args = parser.parse_args()
    
    if not args.file and not args.directory:
        parser.error("Either --file or --directory must be specified")
    
    if args.file and not args.table:
        parser.error("--table is required when using --file")
    
    # Parse cutoff time with enhanced logic
    cutoff_time = None
    auto_cutoff = not args.no_auto_cutoff
    
    # Priority 1: Manual cutoff override
    if args.manual_cutoff:
        try:
            cutoff_time = datetime.strptime(args.manual_cutoff, '%Y-%m-%d %H:%M:%S')
            auto_cutoff = False  # Disable auto-cutoff when manual cutoff provided
            logger.info(f"Using manual cutoff time: {cutoff_time}")
        except ValueError as e:
            logger.error(f"Invalid manual cutoff time format: {e}")
            return
    
    # Legacy support
    elif args.cutoff_time:
        try:
            cutoff_time = datetime.strptime(args.cutoff_time, '%Y-%m-%d %H:%M:%S')
            auto_cutoff = False
            logger.warning("--cutoff-time is deprecated, use --manual-cutoff instead")
        except ValueError as e:
            logger.error(f"Invalid cutoff time format: {e}")
            return
    elif args.cutoff_hours:
        cutoff_time = datetime.now() - timedelta(hours=args.cutoff_hours)
        auto_cutoff = False
        logger.warning("--cutoff-hours is deprecated, use --manual-cutoff instead")
    
    # Auto-cutoff will be calculated per-table if enabled
    
    # Initialize importer
    importer = ClickHouseImporter()
    
    check_existing = not args.no_check_existing
    
    try:
        if args.file:
            # Import single file
            result = importer.import_parquet_file(
                file_path=args.file,
                table=args.table,
                cutoff_time=cutoff_time,
                batch_size=args.batch_size,
                check_existing=check_existing,
                auto_cutoff=auto_cutoff
            )
            
            logger.info(f"Import result: {result}")
        
        else:
            # Import directory
            result = importer.import_directory(
                directory_path=args.directory,
                cutoff_time=cutoff_time,
                batch_size=args.batch_size,
                check_existing=check_existing,
                auto_cutoff=auto_cutoff
            )
            
            logger.info(f"Directory import result: {result}")
    
    except Exception as e:
        logger.error(f"Import failed: {e}")

if __name__ == "__main__":
    main()