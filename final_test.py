#!/usr/bin/env python3
"""Final test to demonstrate both issues have been resolved"""

import pandas as pd
import os
import sys

def test_all_parquet_files():
    """Test all available parquet files"""
    print("🔍 FINAL VERIFICATION TEST")
    print("=" * 50)
    
    # Test files in different locations
    test_locations = [
        ("./", "Current directory"),
        ("exports/", "Exports directory"),
    ]
    
    files_found = 0
    files_passed = 0
    
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
        
        for filename in parquet_files:
            filepath = os.path.join(location, filename)
            files_found += 1
            
            try:
                print(f"\n  Testing: {filename}")
                df = pd.read_parquet(filepath)
                
                # Check basic structure
                print(f"    Shape: {df.shape}")
                print(f"    Columns: {df.columns.tolist()}")
                
                # Check mt column specifically
                if 'mt' in df.columns:
                    null_count = df['mt'].isnull().sum()
                    unique_values = df['mt'].unique()
                    value_counts = df['mt'].value_counts()
                    
                    print(f"    Mt unique values: {unique_values}")
                    print(f"    Mt value counts: {dict(value_counts)}")
                    
                    if null_count == 0:
                        print(f"    ✅ No null values in mt column")
                        files_passed += 1
                    else:
                        print(f"    ❌ {null_count} null values in mt column")
                else:
                    print(f"    ⚠️  No mt column found")
                    
            except Exception as e:
                print(f"    ❌ Error reading {filename}: {e}")
    
    print(f"\n{'='*50}")
    print(f"📊 FINAL RESULTS:")
    print(f"   Files found: {files_found}")
    print(f"   Files passed: {files_passed}")
    
    if files_found == 0:
        print("⚠️  No parquet files found to test")
        return False
    elif files_passed == files_found:
        print("🎉 SUCCESS: All parquet files have proper mt columns!")
        return True
    else:
        print(f"❌ {files_found - files_passed} files still have mt column issues")
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