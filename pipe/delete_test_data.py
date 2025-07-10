# delete_test_data.py
# Surgical deletion script for testing gap-fill functionality

import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from clickhouse_driver import Client
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataDeleter:
    def __init__(self):
        self.client = self.connect_to_clickhouse()
        self.tables = ['deals', 'klines', 'tickers', 'depth']
    
    def connect_to_clickhouse(self) -> Client:
        """Connect to ClickHouse"""
        try:
            client = Client(
                host=config.CLICKHOUSE_HOST,
                port=config.CLICKHOUSE_PORT,
                user=config.CLICKHOUSE_USER,
                password=config.CLICKHOUSE_PASSWORD,
                database=config.CLICKHOUSE_DB
            )
            client.execute('SELECT 1')
            logger.info("Connected to ClickHouse for deletion")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def count_records_in_range(self, table: str, start_time: datetime, end_time: datetime, symbol: str = None) -> int:
        """Count records in a time range"""
        query = f"""
        SELECT COUNT(*) FROM {table}
        WHERE timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        if symbol:
            query += f" AND symbol = '{symbol}'"
        
        result = self.client.execute(query)
        return result[0][0] if result else 0
    
    def delete_records_in_range(self, table: str, start_time: datetime, end_time: datetime, symbol: str = None, dry_run: bool = False) -> Dict[str, Any]:
        """Delete records in a time range"""
        
        # Count records before deletion
        before_count = self.count_records_in_range(table, start_time, end_time, symbol)
        
        if before_count == 0:
            logger.info(f"No records to delete in {table} for specified range")
            return {
                'table': table,
                'before_count': 0,
                'after_count': 0,
                'deleted_count': 0,
                'dry_run': dry_run
            }
        
        logger.info(f"Found {before_count} records to delete in {table}")
        
        if dry_run:
            logger.info(f"DRY RUN: Would delete {before_count} records from {table}")
            return {
                'table': table,
                'before_count': before_count,
                'after_count': before_count,
                'deleted_count': 0,
                'dry_run': True
            }
        
        # Build delete query
        delete_query = f"""
        DELETE FROM {table}
        WHERE timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        if symbol:
            delete_query += f" AND symbol = '{symbol}'"
        
        try:
            # Execute deletion
            self.client.execute(delete_query)
            
            # Count records after deletion
            after_count = self.count_records_in_range(table, start_time, end_time, symbol)
            deleted_count = before_count - after_count
            
            logger.info(f"Deleted {deleted_count} records from {table}")
            
            return {
                'table': table,
                'before_count': before_count,
                'after_count': after_count,
                'deleted_count': deleted_count,
                'dry_run': False
            }
        
        except Exception as e:
            logger.error(f"Failed to delete from {table}: {e}")
            return {
                'table': table,
                'before_count': before_count,
                'after_count': before_count,
                'deleted_count': 0,
                'error': str(e),
                'dry_run': False
            }
    
    def delete_time_range(self, start_time: datetime, end_time: datetime, symbol: str = None, tables: list = None, dry_run: bool = False) -> Dict[str, Any]:
        """Delete records from specified time range across tables"""
        
        if tables is None:
            tables = self.tables
        
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Deleting records from {start_time} to {end_time}")
        if symbol:
            logger.info(f"Symbol filter: {symbol}")
        
        results = {}
        total_deleted = 0
        
        for table in tables:
            result = self.delete_records_in_range(table, start_time, end_time, symbol, dry_run)
            results[table] = result
            total_deleted += result['deleted_count']
        
        summary = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'symbol': symbol,
            'total_deleted': total_deleted,
            'table_results': results,
            'dry_run': dry_run
        }
        
        return summary
    
    def delete_first_hour(self, symbol: str = 'ETH_USDT', dry_run: bool = False) -> Dict[str, Any]:
        """Delete the first hour of data for testing"""
        
        # Find the actual start time of data
        earliest_time = None
        for table in self.tables:
            query = f"SELECT MIN(timestamp) FROM {table}"
            if symbol:
                query += f" WHERE symbol = '{symbol}'"
            
            result = self.client.execute(query)
            if result and result[0] and result[0][0]:
                table_earliest = result[0][0]
                if earliest_time is None or table_earliest < earliest_time:
                    earliest_time = table_earliest
        
        if earliest_time is None:
            logger.error("No data found to delete")
            return {'error': 'No data found'}
        
        # Calculate one hour from the earliest time
        start_time = earliest_time
        end_time = start_time + timedelta(hours=1)
        
        logger.info(f"Deleting first hour of data: {start_time} to {end_time}")
        
        return self.delete_time_range(start_time, end_time, symbol, dry_run=dry_run)
    
    def delete_specific_hour(self, target_hour: int, base_date: str = "2025-07-04", symbol: str = 'ETH_USDT', dry_run: bool = False) -> Dict[str, Any]:
        """Delete a specific hour of data"""
        
        try:
            base_datetime = datetime.strptime(base_date, '%Y-%m-%d')
            start_time = base_datetime + timedelta(hours=target_hour)
            end_time = start_time + timedelta(hours=1)
            
            logger.info(f"Deleting hour {target_hour} ({start_time} to {end_time})")
            
            return self.delete_time_range(start_time, end_time, symbol, dry_run=dry_run)
        
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return {'error': f'Invalid date format: {e}'}

def main():
    parser = argparse.ArgumentParser(description='Delete test data for gap-fill testing')
    
    # Deletion modes
    deletion_group = parser.add_mutually_exclusive_group(required=True)
    deletion_group.add_argument('--first-hour', action='store_true', help='Delete the first hour of data')
    deletion_group.add_argument('--specific-hour', type=int, help='Delete a specific hour (0-23) from base date')
    deletion_group.add_argument('--time-range', nargs=2, help='Delete specific time range (start_time end_time)')
    
    # Options
    parser.add_argument('--base-date', type=str, default='2025-07-04', help='Base date for specific hour deletion (YYYY-MM-DD)')
    parser.add_argument('--symbol', type=str, default='ETH_USDT', help='Symbol filter')
    parser.add_argument('--tables', nargs='+', choices=['deals', 'klines', 'tickers', 'depth'], help='Specific tables to delete from')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    # Initialize deleter
    deleter = DataDeleter()
    
    try:
        if args.first_hour:
            result = deleter.delete_first_hour(args.symbol, args.dry_run)
        
        elif args.specific_hour is not None:
            result = deleter.delete_specific_hour(args.specific_hour, args.base_date, args.symbol, args.dry_run)
        
        elif args.time_range:
            start_time = datetime.strptime(args.time_range[0], '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(args.time_range[1], '%Y-%m-%d %H:%M:%S')
            result = deleter.delete_time_range(start_time, end_time, args.symbol, args.tables, args.dry_run)
        
        # Print results
        logger.info("=== DELETION SUMMARY ===")
        logger.info(f"Total records deleted: {result.get('total_deleted', 0)}")
        logger.info(f"Dry run: {result.get('dry_run', False)}")
        
        if 'table_results' in result:
            logger.info("\\nPer-table results:")
            for table, table_result in result['table_results'].items():
                logger.info(f"  {table}: {table_result['deleted_count']} deleted ({table_result['before_count']} -> {table_result['after_count']})")
                if 'error' in table_result:
                    logger.error(f"    Error: {table_result['error']}")
        
        if 'error' in result:
            logger.error(f"Error: {result['error']}")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Deletion failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()