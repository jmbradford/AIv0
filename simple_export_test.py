#!/usr/bin/env python3
"""
Simple export test - just export current data to Parquet without rotation
"""

import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from clickhouse_driver import Client

# Add path for imports
sys.path.append('/app')
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
)

def test_simple_export():
    """Test exporting current data without rotation."""
    print("🧪 Testing Simple Data Export")
    print("="*40)
    
    # Connect to ClickHouse
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )
    
    symbols = ['btc', 'eth', 'sol']
    export_dir = "/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    for symbol in symbols:
        print(f"\n📊 Processing {symbol.upper()}")
        
        # Get all data from table
        try:
            query = f"SELECT ts, mt, m FROM {symbol} ORDER BY ts"
            data = client.execute(query)
            
            if not data:
                print(f"⚠️  No data in {symbol} table")
                continue
                
            print(f"📥 Found {len(data)} records in {symbol}")
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
            
            # Convert mt enum to string
            mt_map = {1: 't', 2: 'd', 3: 'dp', 4: 'dl'}
            df['mt'] = df['mt'].map(mt_map)
            
            # Create filename
            now = datetime.now()
            filename = f"{symbol}_{now.strftime('%Y%m%d_%H%M%S')}_test.parquet"
            filepath = os.path.join(export_dir, filename)
            
            # Export to Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, filepath, compression='snappy')
            
            # Verify
            file_size = os.path.getsize(filepath)
            verify_table = pq.read_table(filepath)
            row_count = verify_table.num_rows
            
            print(f"✅ Exported {symbol}: {filename}")
            print(f"   📁 File: {file_size:,} bytes, {row_count} rows")
            
        except Exception as e:
            print(f"❌ Failed to export {symbol}: {e}")
    
    print(f"\n🎯 Export test complete!")
    print(f"📁 Check /exports directory for Parquet files")

if __name__ == "__main__":
    test_simple_export()