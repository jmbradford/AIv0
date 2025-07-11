# ch_data_info.py
# Utility script to check existing data ranges in ClickHouse
# Useful for planning exports and understanding data coverage

import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from clickhouse_driver import Client
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataInfoChecker:
    def __init__(self):
        self.client = self.connect_to_clickhouse()
        self.tables = ['deals', 'klines', 'tickers', 'depth']
    
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
            logger.info("Connected to ClickHouse")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def get_table_info(self, table: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive information about a table"""
        try:
            # Base query for counts and time ranges
            query = f"""
            SELECT 
                COUNT(*) as record_count,
                MIN(timestamp) as min_timestamp,
                MAX(timestamp) as max_timestamp,
                COUNT(DISTINCT symbol) as symbol_count
            FROM {table}
            """
            
            if symbol:
                query += f" WHERE symbol = '{symbol}'"
            
            result = self.client.execute(query)
            
            if result and result[0]:
                count, min_time, max_time, symbol_count = result[0]
                
                # Get symbol list if not filtered
                symbols = []
                if not symbol:
                    symbol_query = f"SELECT symbol, COUNT(*) as count FROM {table} GROUP BY symbol ORDER BY count DESC"
                    symbol_result = self.client.execute(symbol_query)
                    symbols = [{'symbol': s[0], 'count': s[1]} for s in symbol_result]
                
                # Get recent activity (last 24 hours)
                recent_query = f"""
                SELECT COUNT(*) as recent_count
                FROM {table}
                WHERE timestamp >= now() - INTERVAL 24 HOUR
                """
                if symbol:
                    recent_query += f" AND symbol = '{symbol}'"
                
                recent_result = self.client.execute(recent_query)
                recent_count = recent_result[0][0] if recent_result else 0
                
                return {
                    'table': table,
                    'symbol_filter': symbol,
                    'record_count': count,
                    'min_timestamp': min_time,
                    'max_timestamp': max_time,
                    'symbol_count': symbol_count,
                    'symbols': symbols,
                    'recent_24h_count': recent_count,
                    'data_span_hours': (max_time - min_time).total_seconds() / 3600 if min_time and max_time else 0
                }
            
            return {
                'table': table,
                'symbol_filter': symbol,
                'record_count': 0,
                'error': 'No data found'
            }
        
        except Exception as e:
            logger.error(f"Failed to get info for table {table}: {e}")
            return {
                'table': table,
                'symbol_filter': symbol,
                'error': str(e)
            }
    
    def get_data_gaps(self, table: str, symbol: Optional[str] = None, interval_minutes: int = 5) -> list:
        """Find gaps in data coverage"""
        try:
            query = f"""
            WITH time_series AS (
                SELECT 
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM {table}
                WHERE timestamp >= now() - INTERVAL 7 DAY
            """
            
            if symbol:
                query += f" AND symbol = '{symbol}'"
            
            query += f"""
            )
            SELECT 
                prev_timestamp,
                timestamp,
                dateDiff('minute', prev_timestamp, timestamp) as gap_minutes
            FROM time_series
            WHERE dateDiff('minute', prev_timestamp, timestamp) > {interval_minutes}
            ORDER BY gap_minutes DESC
            LIMIT 10
            """
            
            result = self.client.execute(query)
            gaps = []
            
            for row in result:
                if len(row) >= 3:
                    gaps.append({
                        'start_time': row[0],
                        'end_time': row[1],
                        'gap_minutes': row[2]
                    })
            
            return gaps
        
        except Exception as e:
            logger.error(f"Failed to get gaps for table {table}: {e}")
            return []
    
    def check_data_freshness(self, table: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Check how fresh the data is"""
        try:
            query = f"""
            SELECT 
                MAX(timestamp) as latest_timestamp,
                COUNT(*) as count_last_hour,
                AVG(toFloat64(volume)) as avg_volume
            FROM {table}
            WHERE timestamp >= now() - INTERVAL 1 HOUR
            """
            
            if symbol:
                query += f" AND symbol = '{symbol}'"
            
            # Add volume column check for tables that have it
            if table in ['deals', 'klines', 'tickers']:
                if table == 'deals':
                    query = query.replace('toFloat64(volume)', 'toFloat64(volume)')
                elif table == 'klines':
                    query = query.replace('toFloat64(volume)', 'toFloat64(volume)')
                elif table == 'tickers':
                    query = query.replace('toFloat64(volume)', 'toFloat64(volume_24h)')
            else:
                query = query.replace(', AVG(toFloat64(volume)) as avg_volume', '')
            
            result = self.client.execute(query)
            
            if result and result[0]:
                latest_timestamp = result[0][0]
                count_last_hour = result[0][1]
                avg_volume = result[0][2] if len(result[0]) > 2 else None
                
                if latest_timestamp:
                    age_minutes = (datetime.now() - latest_timestamp).total_seconds() / 60
                    
                    return {
                        'table': table,
                        'symbol_filter': symbol,
                        'latest_timestamp': latest_timestamp,
                        'age_minutes': age_minutes,
                        'count_last_hour': count_last_hour,
                        'avg_volume': avg_volume,
                        'is_fresh': age_minutes < 10,  # Consider fresh if less than 10 minutes old
                        'status': 'fresh' if age_minutes < 10 else 'stale' if age_minutes < 60 else 'very_stale'
                    }
            
            return {
                'table': table,
                'symbol_filter': symbol,
                'status': 'no_recent_data'
            }
        
        except Exception as e:
            logger.error(f"Failed to check freshness for table {table}: {e}")
            return {
                'table': table,
                'symbol_filter': symbol,
                'error': str(e)
            }
    
    def get_migration_info(self, cutoff_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get information useful for planning a migration"""
        cutoff_time = cutoff_time or datetime.now()
        
        migration_info = {
            'cutoff_time': cutoff_time.isoformat(),
            'tables': {}
        }
        
        for table in self.tables:
            try:
                # Get data before cutoff (what would be migrated)
                before_query = f"""
                SELECT 
                    COUNT(*) as count_before,
                    MIN(timestamp) as min_before,
                    MAX(timestamp) as max_before
                FROM {table}
                WHERE timestamp < '{cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}'
                """
                
                before_result = self.client.execute(before_query)
                
                # Get data after cutoff (what would be preserved)
                after_query = f"""
                SELECT 
                    COUNT(*) as count_after,
                    MIN(timestamp) as min_after,
                    MAX(timestamp) as max_after
                FROM {table}
                WHERE timestamp >= '{cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}'
                """
                
                after_result = self.client.execute(after_query)
                
                migration_info['tables'][table] = {
                    'before_cutoff': {
                        'count': before_result[0][0] if before_result else 0,
                        'min_timestamp': before_result[0][1] if before_result and before_result[0][1] else None,
                        'max_timestamp': before_result[0][2] if before_result and before_result[0][2] else None
                    },
                    'after_cutoff': {
                        'count': after_result[0][0] if after_result else 0,
                        'min_timestamp': after_result[0][1] if after_result and after_result[0][1] else None,
                        'max_timestamp': after_result[0][2] if after_result and after_result[0][2] else None
                    }
                }
                
            except Exception as e:
                logger.error(f"Failed to get migration info for table {table}: {e}")
                migration_info['tables'][table] = {'error': str(e)}
        
        return migration_info

def main():
    parser = argparse.ArgumentParser(description='Check ClickHouse data information')
    parser.add_argument('--table', type=str, help='Specific table to check (deals, klines, tickers, depth)')
    parser.add_argument('--symbol', type=str, help='Symbol filter (e.g., ETH_USDT)')
    parser.add_argument('--gaps', action='store_true', help='Check for data gaps')
    parser.add_argument('--freshness', action='store_true', help='Check data freshness')
    parser.add_argument('--migration-info', action='store_true', help='Get migration planning information')
    parser.add_argument('--cutoff-time', type=str, help='Cutoff time for migration info (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--cutoff-hours', type=int, help='Set cutoff time to N hours ago')
    
    args = parser.parse_args()
    
    # Parse cutoff time
    cutoff_time = None
    if args.cutoff_time:
        try:
            cutoff_time = datetime.strptime(args.cutoff_time, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logger.error(f"Invalid cutoff time format: {e}")
            return
    elif args.cutoff_hours:
        cutoff_time = datetime.now() - timedelta(hours=args.cutoff_hours)
    
    # Initialize checker
    checker = DataInfoChecker()
    
    try:
        tables_to_check = [args.table] if args.table else checker.tables
        
        if args.migration_info:
            # Show migration planning information
            migration_info = checker.get_migration_info(cutoff_time)
            
            print("\n=== MIGRATION PLANNING INFO ===")
            print(f"Cutoff time: {migration_info['cutoff_time']}")
            print()
            
            for table, info in migration_info['tables'].items():
                if 'error' in info:
                    print(f"{table}: ERROR - {info['error']}")
                    continue
                
                before = info['before_cutoff']
                after = info['after_cutoff']
                
                print(f"{table}:")
                print(f"  Before cutoff: {before['count']:,} records")
                if before['min_timestamp']:
                    print(f"    Time range: {before['min_timestamp']} to {before['max_timestamp']}")
                print(f"  After cutoff: {after['count']:,} records")
                if after['min_timestamp']:
                    print(f"    Time range: {after['min_timestamp']} to {after['max_timestamp']}")
                print()
        
        else:
            # Show table information
            for table in tables_to_check:
                print(f"\n=== {table.upper()} TABLE INFO ===")
                
                # Basic info
                info = checker.get_table_info(table, args.symbol)
                
                if 'error' in info:
                    print(f"ERROR: {info['error']}")
                    continue
                
                print(f"Total records: {info['record_count']:,}")
                if info['min_timestamp']:
                    print(f"Time range: {info['min_timestamp']} to {info['max_timestamp']}")
                    print(f"Data span: {info['data_span_hours']:.1f} hours")
                print(f"Recent activity (24h): {info['recent_24h_count']:,} records")
                print(f"Symbol count: {info['symbol_count']}")
                
                # Show top symbols if not filtered
                if not args.symbol and info['symbols']:
                    print("\nTop symbols:")
                    for sym_info in info['symbols'][:5]:
                        print(f"  {sym_info['symbol']}: {sym_info['count']:,} records")
                
                # Check freshness
                if args.freshness:
                    freshness = checker.check_data_freshness(table, args.symbol)
                    print(f"\nData freshness:")
                    print(f"  Status: {freshness.get('status', 'unknown')}")
                    if 'latest_timestamp' in freshness:
                        print(f"  Latest record: {freshness['latest_timestamp']}")
                        print(f"  Age: {freshness['age_minutes']:.1f} minutes")
                        print(f"  Last hour count: {freshness['count_last_hour']}")
                
                # Check gaps
                if args.gaps:
                    gaps = checker.get_data_gaps(table, args.symbol)
                    if gaps:
                        print(f"\nData gaps (>5 min intervals):")
                        for gap in gaps:
                            print(f"  {gap['start_time']} to {gap['end_time']} ({gap['gap_minutes']} minutes)")
                    else:
                        print("\nNo significant data gaps found")
    
    except Exception as e:
        logger.error(f"Data info check failed: {e}")

if __name__ == "__main__":
    main()