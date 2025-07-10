# verify_export.py
# Verify integrity of exported Parquet files

import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import sys
from clickhouse_driver import Client
import config

def connect_to_clickhouse():
    """Connect to ClickHouse"""
    client = Client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB
    )
    return client

def verify_parquet_file(file_path, table_name, start_time, end_time, symbol):
    """Verify a single Parquet file"""
    print(f"\n=== VERIFYING {table_name.upper()} ===")
    
    # Read Parquet file
    df = pd.read_parquet(file_path)
    
    # Basic file info
    print(f"File: {file_path}")
    print(f"Records in file: {len(df):,}")
    print(f"File size: {Path(file_path).stat().st_size:,} bytes")
    print(f"Columns: {list(df.columns)}")
    
    # Check data types
    print(f"Data types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")
    
    # Check time range
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        min_time = df['timestamp'].min()
        max_time = df['timestamp'].max()
        
        print(f"Time range in file: {min_time} to {max_time}")
        
        # Check if times are within expected range
        expected_start = pd.to_datetime(start_time, utc=True)
        expected_end = pd.to_datetime(end_time, utc=True)
        
        if min_time >= expected_start and max_time <= expected_end:
            print("[OK] Time range is within expected bounds")
        else:
            print("[ERROR] Time range is outside expected bounds")
            print(f"  Expected: {expected_start} to {expected_end}")
    
    # Check symbol consistency
    if 'symbol' in df.columns:
        unique_symbols = df['symbol'].unique()
        print(f"Symbols in file: {unique_symbols}")
        
        if len(unique_symbols) == 1 and unique_symbols[0] == symbol:
            print("[OK] Symbol is consistent")
        else:
            print("[ERROR] Symbol inconsistency detected")
    
    # Check for null values
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    
    print(f"Null values:")
    for col, count in null_counts.items():
        if count > 0:
            print(f"  {col}: {count}")
    
    if total_nulls == 0:
        print("[OK] No null values found")
    else:
        print(f"[WARNING] {total_nulls} null values found")
    
    # Check for duplicates based on timestamp and symbol
    if 'timestamp' in df.columns and 'symbol' in df.columns:
        duplicates = df.duplicated(subset=['timestamp', 'symbol']).sum()
        print(f"Duplicate records: {duplicates}")
        
        if duplicates == 0:
            print("[OK] No duplicate records found")
        else:
            print("[ERROR] Duplicate records detected")
    
    # Sample some data
    print(f"\nSample data (first 3 records):")
    print(df.head(3).to_string(index=False))
    
    return {
        'file_path': file_path,
        'table': table_name,
        'record_count': len(df),
        'file_size': Path(file_path).stat().st_size,
        'min_time': min_time if 'timestamp' in df.columns else None,
        'max_time': max_time if 'timestamp' in df.columns else None,
        'null_count': total_nulls,
        'duplicate_count': duplicates if 'timestamp' in df.columns and 'symbol' in df.columns else 0,
        'symbols': unique_symbols.tolist() if 'symbol' in df.columns else []
    }

def verify_against_source(export_dir, start_time, end_time, symbol):
    """Verify exported data against source database"""
    print(f"\n=== COMPARING WITH SOURCE DATABASE ===")
    
    client = connect_to_clickhouse()
    
    # Get source data counts for the same time period
    source_counts = {}
    tables = ['deals', 'klines', 'tickers', 'depth']
    
    for table in tables:
        query = f"""
        SELECT 
            COUNT(*) as count,
            MIN(timestamp) as min_time,
            MAX(timestamp) as max_time
        FROM {table}
        WHERE timestamp >= '{start_time}' 
        AND timestamp <= '{end_time}'
        AND symbol = '{symbol}'
        """
        
        result = client.execute(query)
        if result and result[0]:
            count, min_time, max_time = result[0]
            source_counts[table] = {
                'count': count,
                'min_time': min_time,
                'max_time': max_time
            }
    
    # Compare with exported files
    export_path = Path(export_dir)
    
    for table in tables:
        parquet_files = list(export_path.glob(f"{table}_*.parquet"))
        
        if not parquet_files:
            print(f"[ERROR] No Parquet file found for {table}")
            continue
        
        # Read parquet file
        df = pd.read_parquet(parquet_files[0])
        exported_count = len(df)
        
        # Compare with source
        source_info = source_counts.get(table, {})
        source_count = source_info.get('count', 0)
        
        print(f"\n{table.upper()}:")
        print(f"  Source DB: {source_count:,} records")
        print(f"  Exported:  {exported_count:,} records")
        
        if source_count == exported_count:
            print(f"  [OK] Record counts match")
        else:
            print(f"  [ERROR] Record count mismatch: {abs(source_count - exported_count)} difference")
        
        # Compare time ranges
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            export_min = df['timestamp'].min()
            export_max = df['timestamp'].max()
            source_min = source_info.get('min_time')
            source_max = source_info.get('max_time')
            
            if source_min and source_max:
                print(f"  Source time range: {source_min} to {source_max}")
                print(f"  Export time range: {export_min} to {export_max}")
                
                if (abs((export_min - source_min).total_seconds()) < 1 and 
                    abs((export_max - source_max).total_seconds()) < 1):
                    print(f"  [OK] Time ranges match")
                else:
                    print(f"  [ERROR] Time ranges don't match")

def main():
    export_dir = "test_export"
    start_time = "2025-07-04 19:51:20"
    end_time = "2025-07-05 19:51:20"
    symbol = "ETH_USDT"
    
    print(f"=== EXPORT VERIFICATION ===")
    print(f"Export directory: {export_dir}")
    print(f"Time range: {start_time} to {end_time}")
    print(f"Symbol: {symbol}")
    
    # Verify each Parquet file
    export_path = Path(export_dir)
    tables = ['deals', 'klines', 'tickers', 'depth']
    
    verification_results = []
    
    for table in tables:
        parquet_files = list(export_path.glob(f"{table}_*.parquet"))
        
        if parquet_files:
            result = verify_parquet_file(
                parquet_files[0], 
                table, 
                start_time, 
                end_time, 
                symbol
            )
            verification_results.append(result)
        else:
            print(f"[ERROR] No Parquet file found for {table}")
    
    # Verify against source
    verify_against_source(export_dir, start_time, end_time, symbol)
    
    # Summary
    print(f"\n=== VERIFICATION SUMMARY ===")
    total_records = sum(r['record_count'] for r in verification_results)
    total_size = sum(r['file_size'] for r in verification_results)
    
    print(f"Total files verified: {len(verification_results)}")
    print(f"Total records exported: {total_records:,}")
    print(f"Total file size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
    
    # Check for any issues
    issues = []
    for result in verification_results:
        if result['null_count'] > 0:
            issues.append(f"{result['table']}: {result['null_count']} null values")
        if result['duplicate_count'] > 0:
            issues.append(f"{result['table']}: {result['duplicate_count']} duplicates")
    
    if issues:
        print(f"\n[WARNING] Issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print(f"\n[OK] No data integrity issues found")

if __name__ == "__main__":
    main()