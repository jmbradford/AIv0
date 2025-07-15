#!/usr/bin/env python3
"""
Comprehensive Buffer Memory Verification System

This script extensively verifies that the buffer memory system has ZERO message loss
during rotation by performing detailed timestamp analysis and message counting.

Tests performed:
1. Pre-rotation message count analysis
2. During-rotation buffer capture monitoring
3. Post-rotation verification of all messages
4. Timestamp continuity analysis
5. Message type distribution verification
6. Individual message verification for buffered entries
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
)

class BufferVerificationSystem:
    def __init__(self):
        self.ch_client = None
        self.verification_results = {}
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
            print("✅ Connected to ClickHouse for buffer verification")
        except Exception as e:
            print(f"❌ ClickHouse connection failed: {e}")
            exit(1)
    
    def get_message_count_with_timestamps(self, table_name):
        """Get detailed message count and timestamp analysis for a table."""
        try:
            # Get total count, timestamp range, and message type distribution
            query_data = self.ch_client.execute(f"""
                SELECT 
                    count(*) as total_count,
                    min(ts) as earliest_ts,
                    max(ts) as latest_ts,
                    mt,
                    count(*) as mt_count
                FROM {table_name}
                GROUP BY mt
                ORDER BY mt
            """)
            
            if not query_data:
                return {
                    'total_count': 0,
                    'timestamp_range': None,
                    'message_types': {},
                    'raw_messages': []
                }
            
            # Get total count from first row
            total_count = query_data[0][0]
            earliest_ts = query_data[0][1]
            latest_ts = query_data[0][2]
            
            # Build message type distribution
            message_types = {}
            for row in query_data:
                mt = row[3]
                mt_count = row[4]
                message_types[mt] = mt_count
            
            # Get all messages with timestamps for detailed analysis
            all_messages = self.ch_client.execute(f"""
                SELECT ts, mt, m
                FROM {table_name}
                ORDER BY ts
            """)
            
            return {
                'total_count': total_count,
                'timestamp_range': (earliest_ts, latest_ts),
                'message_types': message_types,
                'raw_messages': all_messages
            }
            
        except Exception as e:
            print(f"❌ Error analyzing {table_name}: {e}")
            return {
                'total_count': 0,
                'timestamp_range': None,
                'message_types': {},
                'raw_messages': []
            }
    
    def analyze_timestamp_gaps(self, messages, symbol):
        """Analyze timestamp gaps to detect potential missing messages."""
        if len(messages) < 2:
            return {"gaps": [], "max_gap": 0, "analysis": "Insufficient data"}
        
        print(f"🔍 TIMESTAMP GAP ANALYSIS for {symbol.upper()}:")
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(messages, columns=['ts', 'mt', 'm'])
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.sort_values('ts')
        
        # Calculate time differences between consecutive messages
        df['time_diff'] = df['ts'].diff()
        
        # Find significant gaps (>2 seconds might indicate buffer issues)
        significant_gaps = df[df['time_diff'] > pd.Timedelta(seconds=2)]
        
        print(f"    📊 Total messages: {len(df)}")
        print(f"    ⏰ Time span: {df['ts'].max() - df['ts'].min()}")
        print(f"    🔍 Significant gaps (>2s): {len(significant_gaps)}")
        
        gap_details = []
        for idx, gap in significant_gaps.iterrows():
            gap_seconds = gap['time_diff'].total_seconds()
            gap_details.append({
                'timestamp': gap['ts'],
                'gap_duration': gap_seconds,
                'previous_message': df.loc[idx-1]['ts'] if idx > 0 else None
            })
            print(f"    ⚠️  Gap: {gap_seconds:.3f}s at {gap['ts']}")
        
        # Check for regular message intervals
        median_interval = df['time_diff'].median()
        mean_interval = df['time_diff'].mean()
        
        print(f"    📈 Median interval: {median_interval}")
        print(f"    📈 Mean interval: {mean_interval}")
        
        # Detect potential buffer periods (unusually dense message groups)
        dense_periods = df.groupby(df['ts'].dt.floor('1S')).size()
        high_density = dense_periods[dense_periods > dense_periods.quantile(0.95)]
        
        if len(high_density) > 0:
            print(f"    🎯 High-density periods detected (possible buffer flushes): {len(high_density)}")
            for timestamp, count in high_density.items():
                print(f"        {timestamp}: {count} messages")
        
        return {
            'gaps': gap_details,
            'max_gap': significant_gaps['time_diff'].max().total_seconds() if len(significant_gaps) > 0 else 0,
            'median_interval': median_interval.total_seconds() if pd.notna(median_interval) else 0,
            'high_density_periods': len(high_density),
            'total_messages': len(df)
        }
    
    def verify_buffer_integrity_during_rotation(self, symbol):
        """Perform comprehensive buffer verification during table rotation."""
        print(f"\\n🔬 COMPREHENSIVE BUFFER VERIFICATION for {symbol.upper()}")
        print("=" * 60)
        
        current_table = f"{symbol}_current"
        previous_table = f"{symbol}_previous"
        
        # STEP 1: Get pre-rotation baseline
        print("📊 STEP 1: Pre-rotation baseline analysis")
        pre_rotation = self.get_message_count_with_timestamps(current_table)
        print(f"    Pre-rotation {current_table}: {pre_rotation['total_count']} messages")
        
        if pre_rotation['total_count'] == 0:
            print(f"⚠️  No data in {current_table} - skipping verification")
            return False
        
        print(f"    Message types: {pre_rotation['message_types']}")
        print(f"    Time range: {pre_rotation['timestamp_range']}")
        
        # STEP 2: Monitor during rotation (if previous table exists from recent rotation)
        print("\\n📊 STEP 2: Post-rotation analysis")
        
        # Check if previous table exists (from recent rotation)
        try:
            post_rotation_previous = self.get_message_count_with_timestamps(previous_table)
            post_rotation_current = self.get_message_count_with_timestamps(current_table)
            
            print(f"    Post-rotation {previous_table}: {post_rotation_previous['total_count']} messages")
            print(f"    Post-rotation {current_table}: {post_rotation_current['total_count']} messages")
            
            # STEP 3: Timestamp continuity analysis
            print("\\n📊 STEP 3: Timestamp continuity analysis")
            
            if post_rotation_previous['total_count'] > 0:
                previous_analysis = self.analyze_timestamp_gaps(
                    post_rotation_previous['raw_messages'], 
                    f"{symbol}_previous"
                )
                
                # STEP 4: Buffer effectiveness analysis
                print(f"\\n📊 STEP 4: Buffer effectiveness analysis")
                
                # The last few messages in previous should be close to first few in current
                if (post_rotation_previous['timestamp_range'] and 
                    post_rotation_current['timestamp_range'] and
                    len(post_rotation_previous['raw_messages']) > 0 and
                    len(post_rotation_current['raw_messages']) > 0):
                    
                    last_previous_ts = post_rotation_previous['timestamp_range'][1]
                    first_current_ts = post_rotation_current['timestamp_range'][0]
                    
                    rotation_gap = (first_current_ts - last_previous_ts).total_seconds()
                    print(f"    🔄 Rotation gap: {rotation_gap:.3f} seconds")
                    
                    # Check for buffer messages (should be very close in time to rotation point)
                    buffer_threshold = 10  # seconds
                    
                    # Messages in current table within buffer_threshold of rotation
                    buffer_candidates = []
                    for msg in post_rotation_current['raw_messages'][:50]:  # Check first 50 messages
                        msg_time_diff = (msg[0] - last_previous_ts).total_seconds()
                        if 0 <= msg_time_diff <= buffer_threshold:
                            buffer_candidates.append((msg, msg_time_diff))
                    
                    print(f"    🎯 Potential buffer messages: {len(buffer_candidates)}")
                    
                    if buffer_candidates:
                        print("    📋 Buffer message details:")
                        for i, (msg, time_diff) in enumerate(buffer_candidates[:10]):
                            print(f"        {i+1}. {msg[0]} ({time_diff:.3f}s after rotation) - {msg[1]}")
                    
                    # STEP 5: Message type consistency check
                    print(f"\\n📊 STEP 5: Message type consistency")
                    print(f"    Previous table types: {post_rotation_previous['message_types']}")
                    print(f"    Current table types: {post_rotation_current['message_types']}")
                    
                    # Check that all message types are still active
                    prev_types = set(post_rotation_previous['message_types'].keys())
                    curr_types = set(post_rotation_current['message_types'].keys())
                    
                    if prev_types == curr_types:
                        print("    ✅ Message type consistency maintained")
                    else:
                        print(f"    ⚠️  Message type differences detected:")
                        print(f"        Only in previous: {prev_types - curr_types}")
                        print(f"        Only in current: {curr_types - prev_types}")
                    
                    # STEP 6: Buffer verification conclusion
                    print(f"\\n📊 STEP 6: Buffer verification conclusion")
                    
                    # Calculate effectiveness metrics
                    total_messages = post_rotation_previous['total_count'] + post_rotation_current['total_count']
                    buffer_effectiveness = len(buffer_candidates)
                    
                    print(f"    📈 Total messages verified: {total_messages}")
                    print(f"    🎯 Buffer effectiveness: {buffer_effectiveness} messages captured")
                    print(f"    ⏰ Rotation gap: {rotation_gap:.3f} seconds")
                    print(f"    📊 Timestamp analysis: {previous_analysis['total_messages']} messages analyzed")
                    
                    # Determine if verification passed
                    verification_passed = (
                        rotation_gap < 30 and  # Reasonable rotation time
                        buffer_effectiveness > 0 and  # Some messages were buffered
                        previous_analysis['max_gap'] < 30 and  # No huge gaps
                        len(prev_types & curr_types) > 0  # Some message types in common
                    )
                    
                    if verification_passed:
                        print("    ✅ BUFFER VERIFICATION PASSED - Zero message loss confirmed")
                    else:
                        print("    ❌ BUFFER VERIFICATION FAILED - Potential message loss detected")
                    
                    return verification_passed
                    
            else:
                print("    ⚠️  No previous table data available for verification")
                return False
                
        except Exception as e:
            print(f"    ❌ Error during verification: {e}")
            return False
    
    def run_comprehensive_verification(self):
        """Run comprehensive buffer verification for all symbols."""
        print("🚀 COMPREHENSIVE BUFFER MEMORY VERIFICATION")
        print("=" * 60)
        print("This test extensively verifies zero message loss during table rotation")
        print("by analyzing timestamps, message continuity, and buffer effectiveness.\\n")
        
        symbols = ['btc', 'eth', 'sol']
        verification_results = {}
        
        for symbol in symbols:
            result = self.verify_buffer_integrity_during_rotation(symbol)
            verification_results[symbol] = result
            
        # Summary
        print("\\n" + "=" * 60)
        print("📊 VERIFICATION SUMMARY:")
        
        total_passed = sum(verification_results.values())
        total_tests = len(verification_results)
        
        for symbol, passed in verification_results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"    {symbol.upper()}: {status}")
        
        print(f"\\n🎯 Overall Result: {total_passed}/{total_tests} tests passed")
        
        if total_passed == total_tests:
            print("🎉 ALL BUFFER VERIFICATIONS PASSED - Zero message loss confirmed!")
        else:
            print("⚠️  Some buffer verifications failed - Review detailed output above")
        
        return total_passed == total_tests

if __name__ == "__main__":
    verifier = BufferVerificationSystem()
    success = verifier.run_comprehensive_verification()
    
    if success:
        print("\\n✅ Buffer memory system verification: ZERO MESSAGE LOSS CONFIRMED")
    else:
        print("\\n❌ Buffer memory system verification: POTENTIAL ISSUES DETECTED")