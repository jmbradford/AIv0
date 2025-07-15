#!/usr/bin/env python3
import pandas as pd
import pyarrow.parquet as pq
import os

def analyze_parquet_file(filepath, symbol):
    """Analyze a parquet file for data integrity"""
    print(f"\n=== {symbol.upper()} Debug Parquet Analysis ===")
    
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return False
    
    try:
        # Read parquet file
        df = pd.read_parquet(filepath)
        
        # Basic info
        print(f"✅ File loaded successfully")
        print(f"📊 Shape: {df.shape} (rows, columns)")
        print(f"📋 Columns: {list(df.columns)}")
        print(f"💾 Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Data types
        print(f"\n📝 Data types:")
        for col, dtype in df.dtypes.items():
            print(f"   {col}: {dtype}")
        
        # Check for nulls
        null_counts = df.isnull().sum()
        print(f"\n🔍 Null values:")
        for col, count in null_counts.items():
            status = "✅" if count == 0 else "⚠️"
            print(f"   {status} {col}: {count}")
        
        # Message type distribution
        if 'mt' in df.columns:
            print(f"\n📈 Message type distribution:")
            mt_counts = df['mt'].value_counts()
            for mt, count in mt_counts.items():
                print(f"   {mt}: {count}")
        
        # Timestamp analysis
        if 'ts' in df.columns:
            print(f"\n⏰ Timestamp analysis:")
            print(f"   Min timestamp: {df['ts'].min()}")
            print(f"   Max timestamp: {df['ts'].max()}")
            print(f"   Time range: {df['ts'].max() - df['ts'].min()}")
        
        # Sample data
        print(f"\n📄 Sample data (first 3 rows):")
        print(df.head(3).to_string())
        
        # Message content analysis
        if 'm' in df.columns:
            print(f"\n📝 Message content analysis:")
            avg_msg_len = df['m'].str.len().mean()
            print(f"   Average message length: {avg_msg_len:.1f} characters")
            print(f"   Max message length: {df['m'].str.len().max()}")
            print(f"   Min message length: {df['m'].str.len().min()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error analyzing {symbol} parquet: {e}")
        return False

def main():
    """Main function to analyze all debug parquet files"""
    exports_dir = "/home/jmbra/AIv0/pipe/exports"
    
    # Files to analyze - check for latest files
    import glob
    debug_files = glob.glob(os.path.join(exports_dir, "*_debug.parquet"))
    debug_files.sort(key=os.path.getmtime, reverse=True)  # Sort by modification time, newest first
    
    files_to_check = []
    for pattern in ["btc_*_debug.parquet", "eth_*_debug.parquet", "sol_*_debug.parquet"]:
        matches = glob.glob(os.path.join(exports_dir, pattern))
        if matches:
            # Get the newest file for each symbol
            newest = max(matches, key=os.path.getmtime)
            symbol = pattern.split('_')[0].upper()
            files_to_check.append((os.path.basename(newest), symbol))
    
    print(f"🔍 Found debug files to analyze: {[f[0] for f in files_to_check]}")
    
    success_count = 0
    total_files = len(files_to_check)
    
    for filename, symbol in files_to_check:
        filepath = os.path.join(exports_dir, filename)
        if analyze_parquet_file(filepath, symbol):
            success_count += 1
    
    print(f"\n{'='*50}")
    print(f"📊 SUMMARY: {success_count}/{total_files} files analyzed successfully")
    
    if success_count == total_files:
        print("✅ All debug parquet files passed integrity check")
    else:
        print("⚠️  Some files had issues - see details above")

if __name__ == "__main__":
    main()