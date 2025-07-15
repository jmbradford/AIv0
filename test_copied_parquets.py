#!/usr/bin/env python3
"""Test the copied parquet files for mt column issues"""

import pandas as pd
import sys

def test_parquet_file(filepath, symbol):
    """Test a single parquet file"""
    try:
        print(f"\n=== Testing {symbol} Debug Parquet ===")
        df = pd.read_parquet(filepath)
        
        print(f"✅ File loaded successfully: {filepath}")
        print(f"📊 Shape: {df.shape}")
        print(f"📋 Columns: {df.columns.tolist()}")
        
        # Check mt column specifically
        print(f"\n🔍 Mt column analysis:")
        print(f"    Data type: {df['mt'].dtype}")
        print(f"    Unique values: {df['mt'].unique()}")
        print(f"    Value counts: {df['mt'].value_counts()}")
        print(f"    Null count: {df['mt'].isnull().sum()}")
        
        # Check for any issues
        if df['mt'].isnull().sum() > 0:
            print(f"❌ Found {df['mt'].isnull().sum()} null values in mt column")
            return False
        else:
            print(f"✅ No null values in mt column")
            return True
            
    except Exception as e:
        print(f"❌ Error testing {symbol}: {e}")
        return False

def main():
    """Test all copied parquet files"""
    files_to_test = [
        ("btc_debug.parquet", "BTC"),
        ("eth_debug.parquet", "ETH"),
        ("sol_debug.parquet", "SOL")
    ]
    
    success_count = 0
    for filepath, symbol in files_to_test:
        if test_parquet_file(filepath, symbol):
            success_count += 1
    
    print(f"\n{'='*50}")
    print(f"📊 SUMMARY: {success_count}/{len(files_to_test)} files passed")
    
    if success_count == len(files_to_test):
        print("✅ All files passed mt column test!")
        sys.exit(0)
    else:
        print("❌ Some files failed mt column test")
        sys.exit(1)

if __name__ == "__main__":
    main()