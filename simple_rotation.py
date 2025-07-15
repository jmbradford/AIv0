#!/usr/bin/env python3
"""
Simple rotation: Export current data and truncate tables (simulating .bin file rotation)
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

def simple_rotation():
    """Export current data and truncate tables - simulates .bin file rotation."""
    print("🔄 Simple Table Rotation Test")
    print("="*50)
    
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
        print(f"\n📊 Rotating {symbol.upper()}")
        
        try:
            # Step 1: Count current data
            count_query = f"SELECT COUNT(*) FROM {symbol}"
            current_count = client.execute(count_query)[0][0]
            
            if current_count == 0:
                print(f"⚠️  No data in {symbol} table to rotate")
                continue
                
            print(f"📥 Current data: {current_count} records")
            
            # Step 2: Export all current data
            query = f"SELECT ts, mt, m FROM {symbol} ORDER BY ts"
            data = client.execute(query)
            
            # Convert to DataFrame and export
            df = pd.DataFrame(data, columns=['ts', 'mt', 'm'])
            mt_map = {1: 't', 2: 'd', 3: 'dp', 4: 'dl'}
            df['mt'] = df['mt'].map(mt_map)
            
            # Create export filename
            now = datetime.now()
            filename = f"{symbol}_{now.strftime('%Y%m%d_%H%M%S')}_rotation.parquet"
            filepath = os.path.join(export_dir, filename)
            
            # Export to Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, filepath, compression='snappy')
            
            # Verify export
            file_size = os.path.getsize(filepath)
            verify_table = pq.read_table(filepath)
            exported_count = verify_table.num_rows
            
            if exported_count != current_count:
                print(f"❌ Export verification failed: {exported_count} != {current_count}")
                continue
                
            print(f"✅ Exported: {filename} ({file_size:,} bytes, {exported_count} rows)")
            
            # Step 3: Truncate table (simulates new .bin file)
            truncate_query = f"TRUNCATE TABLE {symbol}"
            client.execute(truncate_query)
            
            # Verify truncation
            after_count = client.execute(count_query)[0][0]
            print(f"🗑️  Table truncated: {current_count} → {after_count} records")
            
            if after_count == 0:
                print(f"✅ {symbol.upper()} rotation complete - new .bin file ready")
            else:
                print(f"⚠️  {symbol.upper()} truncation incomplete")
                
        except Exception as e:
            print(f"❌ Failed to rotate {symbol}: {e}")
    
    print(f"\n🎯 Rotation complete!")
    print(f"📁 Exported files are in /exports directory")
    print(f"🔄 Tables are now empty (simulating fresh .bin files)")
    print(f"💾 Clients will continue writing to the 'new' .bin files")

if __name__ == "__main__":
    simple_rotation()