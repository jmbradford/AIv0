#!/usr/bin/env python3
"""
Test script to demonstrate table rotation system
"""

import os
import sys
from datetime import datetime, timedelta

# Add the current directory to the path so we can import modules
sys.path.append('/app')

from hourly_exporter import HourlyTableRotator

def test_rotation():
    """Test the rotation system with current data."""
    print("🧪 Testing Table Rotation System")
    print("="*50)
    
    # Create rotator instance
    rotator = HourlyTableRotator()
    
    # Override debug mode to skip uptime checks
    rotator.debug_mode = True
    
    # Calculate a period that includes our test data
    period_start = datetime(2025, 7, 14, 11, 55, 0)  # 11:55
    period_end = datetime(2025, 7, 14, 12, 5, 0)     # 12:05
    
    print(f"Test period: {period_start} to {period_end}")
    print(f"This should include our test data at 12:00:xx")
    
    # Process just BTC to demonstrate
    success = rotator.process_symbol_rotation("btc", period_start, period_end)
    
    if success:
        print("\n✅ BTC rotation completed successfully!")
        print("Check the exports/ directory for the exported Parquet file")
        print("Check ClickHouse - the btc_current table should have new UUID directory")
    else:
        print("\n❌ BTC rotation failed")

if __name__ == "__main__":
    test_rotation()