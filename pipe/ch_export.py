# ch_export.py
# Export ClickHouse data to Parquet files with time range filtering
# Supports efficient batch processing and maintains data integrity

import argparse
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from clickhouse_driver import Client
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickHouseExporter:
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
            logger.info("Connected to ClickHouse for export")
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
        """Validate table exists and is exportable"""
        return table in self.table_schemas
    
    def get_data_range(self, table: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get data range information for a table"""
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
                'max_timestamp': max_time
            }
        return {'table': table, 'symbol': symbol, 'record_count': 0}
    
    def export_table_to_parquet(
        self,
        table: str,
        start_time: datetime,
        end_time: datetime,
        output_path: str,
        symbol: Optional[str] = None,
        batch_size: int = 100000
    ) -> Dict[str, Any]:
        """Export table data to Parquet file"""
        
        if not self.validate_table(table):
            raise ValueError(f"Invalid table: {table}")
        
        logger.info(f"Starting export of {table} from {start_time} to {end_time}")
        
        # Build base query
        columns = self.table_schemas[table]
        query = f"""
        SELECT {', '.join(columns)}
        FROM {table}
        WHERE timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        if symbol:
            query += f" AND symbol = '{symbol}'"
        
        # Get total count
        count_query = query.replace(f"SELECT {', '.join(columns)}", "SELECT COUNT(*)")
        total_records = self.client.execute(count_query)[0][0]
        
        if total_records == 0:
            logger.warning(f"No records found for export query")
            return {'exported_records': 0, 'file_path': None}
        
        logger.info(f"Exporting {total_records} records from {table}")
        
        # Export in batches
        offset = 0
        exported_records = 0
        parquet_writer = None
        
        try:
            while offset < total_records:
                # Execute batch query
                batch_query = f"{query} ORDER BY timestamp LIMIT {batch_size} OFFSET {offset}"
                batch_result = self.client.execute(batch_query)
                
                if not batch_result:
                    break
                
                # Convert to DataFrame
                df = pd.DataFrame(batch_result, columns=columns)
                
                # Convert timestamp to proper datetime if needed
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Handle special data types
                if table == 'tickers':
                    # Convert funding_rate to float for Parquet compatibility
                    if 'funding_rate_from_ticker' in df.columns:
                        df['funding_rate_from_ticker'] = df['funding_rate_from_ticker'].astype('float64')
                
                # Initialize Parquet writer on first batch
                if parquet_writer is None:
                    parquet_schema = pa.Schema.from_pandas(df)
                    parquet_writer = pq.ParquetWriter(output_path, parquet_schema, compression='snappy')
                
                # Write batch to Parquet
                table_pa = pa.Table.from_pandas(df)
                parquet_writer.write_table(table_pa)
                
                exported_records += len(batch_result)
                offset += batch_size
                
                logger.info(f"Exported {exported_records}/{total_records} records")
        
        finally:
            if parquet_writer:
                parquet_writer.close()
        
        logger.info(f"Export completed: {exported_records} records saved to {output_path}")
        
        return {
            'exported_records': exported_records,
            'file_path': output_path,
            'table': table,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'symbol': symbol
        }
    
    def export_all_tables(
        self,
        start_time: datetime,
        end_time: datetime,
        output_dir: str,
        symbol: Optional[str] = None,
        batch_size: int = 100000
    ) -> Dict[str, Any]:
        """Export all tables to separate Parquet files"""
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        results = {}
        
        for table in self.table_schemas.keys():
            try:
                # Generate filename
                symbol_suffix = f"_{symbol}" if symbol else ""
                filename = f"{table}{symbol_suffix}_{start_time.strftime('%Y%m%d_%H%M%S')}_to_{end_time.strftime('%Y%m%d_%H%M%S')}.parquet"
                file_path = output_path / filename
                
                # Export table
                result = self.export_table_to_parquet(
                    table=table,
                    start_time=start_time,
                    end_time=end_time,
                    output_path=str(file_path),
                    symbol=symbol,
                    batch_size=batch_size
                )
                
                results[table] = result
                
            except Exception as e:
                logger.error(f"Failed to export table {table}: {e}")
                results[table] = {'error': str(e)}
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Export ClickHouse data to Parquet files')
    parser.add_argument('--table', type=str, help='Table to export (deals, klines, tickers, depth). If not specified, exports all tables.')
    
    # Time selection options (mutually exclusive groups)
    time_group = parser.add_mutually_exclusive_group()
    time_group.add_argument('--hours-back', type=int, help='Export data from last N hours')
    time_group.add_argument('--last-days', type=int, help='Export data from last N days')
    
    # Range selection (use together)
    parser.add_argument('--start-hours-back', type=int, help='Start time: N hours back from now')
    parser.add_argument('--end-hours-back', type=int, help='End time: N hours back from now')
    
    # Absolute time selection (fallback)
    parser.add_argument('--start-time', type=str, help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', type=str, help='End time (YYYY-MM-DD HH:MM:SS)')
    
    parser.add_argument('--symbol', type=str, help='Symbol filter (e.g., ETH_USDT)')
    parser.add_argument('--output-dir', type=str, default='./exports', help='Output directory for Parquet files')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch size for export')
    
    args = parser.parse_args()
    
    # Parse time arguments with enhanced logic
    start_time = None
    end_time = None
    
    # Priority 1: Relative time options
    if args.hours_back:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=args.hours_back)
        logger.info(f"Using relative time: last {args.hours_back} hours")
    
    elif args.last_days:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=args.last_days)
        logger.info(f"Using relative time: last {args.last_days} days")
    
    # Priority 2: Range selection
    elif args.start_hours_back and args.end_hours_back:
        now = datetime.now()
        start_time = now - timedelta(hours=args.start_hours_back)
        end_time = now - timedelta(hours=args.end_hours_back)
        logger.info(f"Using time range: {args.start_hours_back}h ago to {args.end_hours_back}h ago")
    
    # Priority 3: Absolute time selection
    elif args.start_time and args.end_time:
        try:
            start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S')
            logger.info(f"Using absolute time: {args.start_time} to {args.end_time}")
        except ValueError as e:
            logger.error(f"Invalid time format: {e}")
            return
    
    # Default: Last 24 hours
    else:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        logger.info("No time specified, defaulting to last 24 hours")
    
    # Validation
    if args.start_hours_back and not args.end_hours_back:
        logger.error("--start-hours-back requires --end-hours-back")
        return
    if args.end_hours_back and not args.start_hours_back:
        logger.error("--end-hours-back requires --start-hours-back")
        return
    
    if start_time >= end_time:
        logger.error("Start time must be before end time")
        return
    
    # Initialize exporter
    exporter = ClickHouseExporter()
    
    try:
        if args.table:
            # Export single table
            filename = f"{args.table}_{start_time.strftime('%Y%m%d_%H%M%S')}_to_{end_time.strftime('%Y%m%d_%H%M%S')}.parquet"
            if args.symbol:
                filename = f"{args.table}_{args.symbol}_{start_time.strftime('%Y%m%d_%H%M%S')}_to_{end_time.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            output_path = Path(args.output_dir) / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            result = exporter.export_table_to_parquet(
                table=args.table,
                start_time=start_time,
                end_time=end_time,
                output_path=str(output_path),
                symbol=args.symbol,
                batch_size=args.batch_size
            )
            
            logger.info(f"Export completed: {result}")
            
        else:
            # Export all tables
            results = exporter.export_all_tables(
                start_time=start_time,
                end_time=end_time,
                output_dir=args.output_dir,
                symbol=args.symbol,
                batch_size=args.batch_size
            )
            
            logger.info("All exports completed:")
            for table, result in results.items():
                if 'error' in result:
                    logger.error(f"  {table}: {result['error']}")
                else:
                    logger.info(f"  {table}: {result.get('exported_records', 0)} records")
    
    except Exception as e:
        logger.error(f"Export failed: {e}")

if __name__ == "__main__":
    main()