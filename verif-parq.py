#!/usr/bin/env python3
"""Enhanced parquet verification showing 3 most recent entries per message type per asset"""

import pandas as pd
import os
import sys
from datetime import datetime

def extract_asset_from_filename(filename):
    """Extract asset name from parquet filename"""
    # Expected format: asset_YYYYMMDD_HHMM.parquet
    parts = filename.split('_')
    if len(parts) >= 1:
        return parts[0].upper()
    return filename.replace('.parquet', '').upper()

def show_recent_entries_by_message_type(df, asset_name, filename):
    """Show 3 most recent entries per message type for verification"""
    print(f"\n📋 {asset_name} - Recent Message Samples ({filename})")
    print("=" * 60)
    
    if 'mt' not in df.columns or 'ts' not in df.columns or 'm' not in df.columns:
        print("⚠️  Required columns (ts, mt, m) not found")
        return False
    
    # Convert timestamp to datetime for sorting
    df['ts'] = pd.to_datetime(df['ts'])
    
    # Sort by timestamp descending to get most recent first
    df_sorted = df.sort_values('ts', ascending=False)
    
    # Message type mapping for display
    mt_names = {
        't': 'ticker',
        'd': 'deal', 
        'dp': 'depth',
        'dl': 'deadletter'
    }
    
    # Get unique message types in the data
    unique_types = df_sorted['mt'].unique()
    total_samples = 0
    
    for mt in sorted(unique_types):
        mt_name = mt_names.get(mt, mt)
        mt_data = df_sorted[df_sorted['mt'] == mt].head(3)
        
        if len(mt_data) > 0:
            print(f"\n📊 {mt.upper()} ({mt_name}) - {len(mt_data)} most recent entries:")
            for i, row in mt_data.iterrows():
                ts_str = row['ts'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # milliseconds
                message = row['m']
                
                # Truncate long messages for display
                if len(message) > 80:
                    message = message[:77] + "..."
                    
                print(f"    {ts_str} | {message}")
                total_samples += 1
        else:
            print(f"\n📊 {mt.upper()} ({mt_name}): No entries found")
    
    return total_samples > 0

def test_all_parquet_files():
    """Test all available parquet files and show recent entries"""
    print("🔍 ENHANCED PARQUET VERIFICATION")
    print("=" * 60)
    print("📋 Showing 3 most recent entries per message type per asset")
    print("=" * 60)
    
    # Test files in different locations
    test_locations = [
        ("./", "Current directory"),
        ("exports/", "Exports directory"),
        ("/exports", "Container exports"),  # Add container path
    ]
    
    files_found = 0
    files_passed = 0
    total_samples_shown = 0
    
    for location, description in test_locations:
        print(f"\n📍 Checking {description} ({location})")
        
        if not os.path.exists(location):
            print(f"⚠️  Directory doesn't exist: {location}")
            continue
            
        parquet_files = [f for f in os.listdir(location) if f.endswith('.parquet')]
        
        if not parquet_files:
            print(f"⚠️  No parquet files found in {location}")
            continue
            
        print(f"📁 Found {len(parquet_files)} parquet files")
        
        # Sort files by modification time (newest first)
        file_info = []
        for filename in parquet_files:
            filepath = os.path.join(location, filename)
            mtime = os.path.getmtime(filepath)
            file_info.append((filename, filepath, mtime))
        
        file_info.sort(key=lambda x: x[2], reverse=True)  # Sort by mtime descending
        
        for filename, filepath, mtime in file_info:
            files_found += 1
            
            try:
                print(f"\n🔍 Processing: {filename}")
                df = pd.read_parquet(filepath)
                
                # Extract asset name from filename
                asset_name = extract_asset_from_filename(filename)
                
                # Basic file info
                file_time = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                print(f"    📄 File created: {file_time}")
                print(f"    📊 Shape: {df.shape}")
                print(f"    📋 Columns: {df.columns.tolist()}")
                
                # Check mt column health
                if 'mt' in df.columns:
                    null_count = df['mt'].isnull().sum()
                    unique_values = sorted(df['mt'].unique())
                    value_counts = df['mt'].value_counts()
                    
                    print(f"    🔍 MT unique values: {unique_values}")
                    print(f"    📈 MT distribution: {dict(value_counts)}")
                    
                    if null_count == 0 and len(unique_values) > 0 and 'dl' not in unique_values:
                        print(f"    ✅ MT column healthy - no nulls, no deadletters")
                        files_passed += 1
                        
                        # Show recent entries by message type
                        samples_shown = show_recent_entries_by_message_type(df, asset_name, filename)
                        if samples_shown:
                            total_samples_shown += 1
                            
                    elif 'dl' in unique_values:
                        print(f"    ⚠️  Found deadletter messages - may indicate enum mapping issues")
                    else:
                        print(f"    ❌ MT column issues: {null_count} nulls")
                else:
                    print(f"    ❌ No mt column found")
                    
            except Exception as e:
                print(f"    ❌ Error reading {filename}: {e}")
    
    print(f"\n{'='*60}")
    print(f"📊 VERIFICATION SUMMARY:")
    print(f"   📁 Files found: {files_found}")
    print(f"   ✅ Files with healthy MT columns: {files_passed}")
    print(f"   📋 Files with samples shown: {total_samples_shown}")
    
    if files_found == 0:
        print("\n⚠️  No parquet files found to test")
        return False
    elif files_passed == files_found and total_samples_shown > 0:
        print(f"\n🎉 SUCCESS: All {files_found} parquet files have proper MT columns!")
        print(f"✅ Displayed recent message samples from {total_samples_shown} files")
        return True
    else:
        failed_files = files_found - files_passed
        print(f"\n❌ {failed_files} files still have MT column issues")
        if total_samples_shown == 0:
            print("⚠️  No sample entries were displayed")
        return False

def main():
    """Main test function"""
    print("🚀 Starting final comprehensive test...")
    
    success = test_all_parquet_files()
    
    if success:
        print("\n✅ VERIFICATION COMPLETE: Both directory creation and mt column issues resolved!")
        sys.exit(0)
    else:
        print("\n❌ Some issues remain")
        sys.exit(1)

if __name__ == "__main__":
    main()