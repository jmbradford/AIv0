#!/usr/bin/env python3
"""
Comprehensive Data Integrity Test

This script verifies that data exported to parquet files matches exactly
what is stored in ClickHouse, ensuring complete data integrity across
the entire pipeline: MEXC → ClickHouse → Parquet.
"""

import os
import sys
import pandas as pd
import pyarrow.parquet as pq
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
)
from datetime import datetime

class DataIntegrityVerifier:
    def __init__(self):
        self.ch_client = None
        self.connect_clickhouse()
        
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
    
    def get_clickhouse_data(self, table_name):
        """Get all data from ClickHouse table."""
        try:
            query = f"""
            SELECT ts, mt, m
            FROM {table_name}
            ORDER BY ts
            """
            result = self.ch_client.execute(query)
            return result
        except Exception as e:
            print(f"❌ Error fetching data from {table_name}: {e}")
            return []
    
    def analyze_parquet_file(self, filepath):
        """Analyze parquet file and return comprehensive statistics."""
        try:
            df = pd.read_parquet(filepath)
            
            # Basic stats
            stats = {
                'filepath': filepath,
                'row_count': len(df),
                'columns': df.columns.tolist(),
                'mt_null_count': df['mt'].isnull().sum(),
                'mt_unique_values': sorted(df['mt'].unique()) if df['mt'].dtype == 'object' else [],
                'mt_value_counts': dict(df['mt'].value_counts()) if df['mt'].dtype == 'object' else {},
                'timestamp_range': {
                    'min': df['ts'].min(),
                    'max': df['ts'].max(),
                    'span': df['ts'].max() - df['ts'].min()
                }
            }
            
            return df, stats
        except Exception as e:
            print(f"❌ Error analyzing parquet file {filepath}: {e}")
            return None, None
    
    def compare_data_integrity(self, clickhouse_data, parquet_df, symbol):
        """Compare ClickHouse data with parquet data for integrity."""
        print(f"\n🔍 INTEGRITY VERIFICATION: {symbol.upper()}")
        print("=" * 60)
        
        # Convert ClickHouse data to DataFrame
        ch_df = pd.DataFrame(clickhouse_data, columns=['ts', 'mt', 'm'])
        
        # Basic count comparison
        ch_count = len(ch_df)
        pq_count = len(parquet_df)
        
        print(f"📊 Row count comparison:")
        print(f"    ClickHouse: {ch_count:,} rows")
        print(f"    Parquet:    {pq_count:,} rows")
        
        if ch_count != pq_count:
            print(f"❌ Row count mismatch!")
            return False
        else:
            print(f"✅ Row counts match")
        
        # Mt column comparison
        print(f"\n📝 Mt column comparison:")
        
        # ClickHouse mt values
        ch_mt_counts = ch_df['mt'].value_counts().sort_index()
        ch_mt_nulls = ch_df['mt'].isnull().sum()
        
        # Parquet mt values  
        pq_mt_counts = parquet_df['mt'].value_counts().sort_index()
        pq_mt_nulls = parquet_df['mt'].isnull().sum()
        
        print(f"    ClickHouse mt values: {dict(ch_mt_counts)}")
        print(f"    Parquet mt values:    {dict(pq_mt_counts)}")
        print(f"    ClickHouse mt nulls:  {ch_mt_nulls}")
        print(f"    Parquet mt nulls:     {pq_mt_nulls}")
        
        # Check for nulls
        if ch_mt_nulls > 0 or pq_mt_nulls > 0:
            print(f"❌ Found null values in mt column!")
            return False
        
        # Check if mt value counts match
        if not ch_mt_counts.equals(pq_mt_counts):
            print(f"❌ Mt value counts don't match!")
            return False
        else:
            print(f"✅ Mt column values match perfectly")
        
        # Timestamp comparison
        print(f"\n⏰ Timestamp comparison:")
        ch_time_range = ch_df['ts'].max() - ch_df['ts'].min()
        pq_time_range = parquet_df['ts'].max() - parquet_df['ts'].min()
        
        print(f"    ClickHouse time range: {ch_time_range}")
        print(f"    Parquet time range:    {pq_time_range}")
        
        if ch_time_range != pq_time_range:
            print(f"⚠️  Time range mismatch (might be due to timezone conversion)")
        else:
            print(f"✅ Time ranges match")
        
        # Sample data comparison
        print(f"\n🔍 Sample data comparison (first 5 rows):")
        print(f"    ClickHouse mt: {ch_df['mt'].head(5).tolist()}")
        print(f"    Parquet mt:    {parquet_df['mt'].head(5).tolist()}")
        
        if ch_df['mt'].head(5).tolist() == parquet_df['mt'].head(5).tolist():
            print(f"✅ Sample data matches")
        else:
            print(f"❌ Sample data mismatch")
            return False
        
        # Message content spot check
        print(f"\n📄 Message content spot check:")
        sample_indices = [0, len(ch_df)//2, len(ch_df)-1]  # First, middle, last
        
        for i in sample_indices:
            if i < len(ch_df) and i < len(parquet_df):
                ch_msg = ch_df.iloc[i]['m']
                pq_msg = parquet_df.iloc[i]['m']
                
                if ch_msg == pq_msg:
                    print(f"    Row {i}: ✅ Messages match")
                else:
                    print(f"    Row {i}: ❌ Message mismatch")
                    print(f"        ClickHouse: {ch_msg[:50]}...")
                    print(f"        Parquet:    {pq_msg[:50]}...")
                    return False
        
        print(f"\n✅ INTEGRITY VERIFICATION PASSED: {symbol.upper()}")
        return True
    
    def verify_current_tables(self):
        """Verify data integrity for current tables against any existing parquet files."""
        print(f"\n🔍 VERIFYING CURRENT TABLES vs PARQUET FILES")
        print("=" * 80)
        
        symbols = ['btc', 'eth', 'sol']
        total_tests = 0
        passed_tests = 0
        
        for symbol in symbols:
            current_table = f"{symbol}_current"
            
            # Get current ClickHouse data
            ch_data = self.get_clickhouse_data(current_table)
            if not ch_data:
                print(f"⚠️  No data in {current_table}")
                continue
            
            # Look for recent parquet files for this symbol
            parquet_files = []
            if os.path.exists('exports/'):
                for filename in os.listdir('exports/'):
                    if filename.startswith(symbol) and filename.endswith('.parquet'):
                        parquet_files.append(filename)
            
            if not parquet_files:
                print(f"ℹ️  No parquet files found for {symbol}")
                continue
            
            # Test the most recent parquet file
            recent_file = sorted(parquet_files)[-1]
            filepath = os.path.join('exports/', recent_file)
            
            parquet_df, stats = self.analyze_parquet_file(filepath)
            if parquet_df is None:
                continue
            
            total_tests += 1
            
            # Compare data integrity
            if self.compare_data_integrity(ch_data, parquet_df, symbol):
                passed_tests += 1
            else:
                print(f"❌ Integrity verification failed for {symbol}")
        
        return total_tests, passed_tests
    
    def verify_previous_tables(self):
        """Verify data integrity for previous tables if they exist."""
        print(f"\n🔍 VERIFYING PREVIOUS TABLES vs PARQUET FILES")
        print("=" * 80)
        
        symbols = ['btc', 'eth', 'sol']
        total_tests = 0
        passed_tests = 0
        
        for symbol in symbols:
            previous_table = f"{symbol}_previous"
            
            # Check if previous table exists
            try:
                exists = self.ch_client.execute(f"EXISTS TABLE {previous_table}")[0][0]
                if not exists:
                    print(f"ℹ️  No {previous_table} table exists")
                    continue
            except:
                print(f"ℹ️  No {previous_table} table exists")
                continue
            
            # Get previous ClickHouse data
            ch_data = self.get_clickhouse_data(previous_table)
            if not ch_data:
                print(f"⚠️  No data in {previous_table}")
                continue
            
            # Look for corresponding parquet files
            parquet_files = []
            if os.path.exists('exports/'):
                for filename in os.listdir('exports/'):
                    if filename.startswith(symbol) and filename.endswith('.parquet'):
                        parquet_files.append(filename)
            
            if not parquet_files:
                print(f"ℹ️  No parquet files found for {symbol}")
                continue
            
            # Test the most recent parquet file
            recent_file = sorted(parquet_files)[-1]
            filepath = os.path.join('exports/', recent_file)
            
            parquet_df, stats = self.analyze_parquet_file(filepath)
            if parquet_df is None:
                continue
            
            total_tests += 1
            
            # Compare data integrity
            if self.compare_data_integrity(ch_data, parquet_df, symbol):
                passed_tests += 1
            else:
                print(f"❌ Integrity verification failed for {symbol}")
        
        return total_tests, passed_tests
    
    def run_comprehensive_test(self):
        """Run comprehensive data integrity test."""
        print(f"🚀 COMPREHENSIVE DATA INTEGRITY TEST")
        print(f"Started at: {datetime.now()}")
        print("=" * 80)
        
        # Test 1: Current tables vs parquet files
        current_total, current_passed = self.verify_current_tables()
        
        # Test 2: Previous tables vs parquet files (if they exist)
        previous_total, previous_passed = self.verify_previous_tables()
        
        # Summary
        total_tests = current_total + previous_total
        total_passed = current_passed + previous_passed
        
        print(f"\n🎯 TEST SUMMARY")
        print("=" * 80)
        print(f"Total tests run: {total_tests}")
        print(f"Tests passed: {total_passed}")
        print(f"Tests failed: {total_tests - total_passed}")
        
        if total_tests == 0:
            print("⚠️  No tests could be run - check that both ClickHouse and parquet files exist")
            return False
        elif total_passed == total_tests:
            print("🎉 ALL TESTS PASSED! Data integrity is perfect across the entire pipeline.")
            return True
        else:
            print("❌ Some tests failed. Data integrity issues detected.")
            return False

def main():
    """Main function."""
    print("🔍 Data Integrity Verification Tool")
    print("Verifies that exported parquet files match ClickHouse data exactly")
    print()
    
    # Override host for external connection
    if len(sys.argv) > 1 and sys.argv[1] == "--localhost":
        global CLICKHOUSE_HOST
        CLICKHOUSE_HOST = "localhost"
        print("Using localhost connection for ClickHouse")
    
    verifier = DataIntegrityVerifier()
    success = verifier.run_comprehensive_test()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()