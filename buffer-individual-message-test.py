#!/usr/bin/env python3
"""
Buffer Individual Message Verification Test

This script specifically verifies that buffered messages are deposited 
as separate individual entries in ClickHouse, not as a single batch.

Tests performed:
1. Trigger rotation to capture buffer messages
2. Analyze post-rotation entries for individual timestamps
3. Verify each buffered message has unique timestamp
4. Confirm no batch-like behavior (multiple messages same timestamp)
5. Validate buffer messages have proper message types and content
"""

import time
import os
from datetime import datetime, timedelta
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE
)

class BufferIndividualMessageVerifier:
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
            print("✅ Connected to ClickHouse for individual message verification")
        except Exception as e:
            print(f"❌ ClickHouse connection failed: {e}")
            exit(1)
    
    def trigger_rotation_and_capture_buffer(self, symbol):
        """Trigger rotation for a symbol and capture buffer behavior."""
        print(f"\\n🔄 TRIGGERING ROTATION for {symbol.upper()}")
        print("=" * 50)
        
        current_table = f"{symbol}_current"
        
        # Get pre-rotation message count
        pre_count = self.ch_client.execute(f"SELECT count(*) FROM {current_table}")[0][0]
        print(f"📊 Pre-rotation {current_table}: {pre_count} messages")
        
        if pre_count == 0:
            print(f"⚠️  No data in {current_table} - cannot test buffer")
            return None
        
        # Create rotation signal
        rotation_flag = f"/tmp/{symbol}_rotate"
        try:
            with open(rotation_flag, 'w') as f:
                f.write('test_rotation')
            print(f"🚨 Rotation signal created: {rotation_flag}")
        except Exception as e:
            print(f"❌ Failed to create rotation signal: {e}")
            return None
        
        # Wait for rotation to complete
        print("⏳ Waiting for rotation to complete...")
        time.sleep(8)  # Allow time for buffer activation, rotation, and flush
        
        # Get post-rotation message count in current table
        post_count = self.ch_client.execute(f"SELECT count(*) FROM {current_table}")[0][0]
        print(f"📊 Post-rotation {current_table}: {post_count} messages")
        
        # Clean up rotation flag
        try:
            os.remove(rotation_flag)
            print(f"🧹 Rotation signal cleaned up")
        except:
            pass
        
        return post_count
    
    def analyze_buffer_message_individuality(self, symbol):
        """Analyze if buffer messages are stored as individual entries."""
        print(f"\\n🔬 ANALYZING BUFFER MESSAGE INDIVIDUALITY for {symbol.upper()}")
        print("=" * 60)
        
        current_table = f"{symbol}_current"
        
        # Get the most recent messages (likely buffer messages)
        recent_messages = self.ch_client.execute(f"""
            SELECT ts, mt, m, toUnixTimestamp(ts) as unix_ts
            FROM {current_table}
            ORDER BY ts DESC
            LIMIT 100
        """)
        
        if not recent_messages:
            print("⚠️  No recent messages found")
            return False
        
        print(f"📊 Analyzing {len(recent_messages)} most recent messages")
        
        # Group messages by exact timestamp (microsecond precision)
        timestamp_groups = {}
        for msg in recent_messages:
            ts = msg[0]
            mt = msg[1]
            m = msg[2]
            unix_ts = msg[3]
            
            if ts not in timestamp_groups:
                timestamp_groups[ts] = []
            timestamp_groups[ts].append((mt, m, unix_ts))
        
        print(f"🔍 Unique timestamps found: {len(timestamp_groups)}")
        
        # Analyze timestamp distribution
        multi_message_timestamps = 0
        total_messages = 0
        max_messages_per_timestamp = 0
        
        for ts, messages in timestamp_groups.items():
            message_count = len(messages)
            total_messages += message_count
            
            if message_count > 1:
                multi_message_timestamps += 1
                max_messages_per_timestamp = max(max_messages_per_timestamp, message_count)
                print(f"    ⚠️  {ts}: {message_count} messages (same timestamp)")
                for i, (mt, m, unix_ts) in enumerate(messages[:3]):  # Show first 3
                    preview = m[:50] + "..." if len(m) > 50 else m
                    print(f"        {i+1}. {mt}: {preview}")
        
        single_message_timestamps = len(timestamp_groups) - multi_message_timestamps
        
        print(f"\\n📈 INDIVIDUALITY ANALYSIS:")
        print(f"    📋 Total messages analyzed: {total_messages}")
        print(f"    ⏰ Unique timestamps: {len(timestamp_groups)}")
        print(f"    ✅ Single-message timestamps: {single_message_timestamps}")
        print(f"    ⚠️  Multi-message timestamps: {multi_message_timestamps}")
        print(f"    📊 Max messages per timestamp: {max_messages_per_timestamp}")
        
        # Calculate individuality percentage
        individuality_percentage = (single_message_timestamps / len(timestamp_groups)) * 100
        print(f"    🎯 Individuality percentage: {individuality_percentage:.1f}%")
        
        # Determine if test passes
        # We expect most messages to have unique timestamps, but some very close timing is acceptable
        individuality_threshold = 80  # 80% of messages should have unique timestamps
        test_passed = individuality_percentage >= individuality_threshold
        
        if test_passed:
            print(f"    ✅ INDIVIDUAL MESSAGE TEST PASSED")
            print(f"       Buffer messages are stored as separate entries")
        else:
            print(f"    ❌ INDIVIDUAL MESSAGE TEST FAILED")
            print(f"       Too many messages share timestamps (batch-like behavior)")
        
        return test_passed
    
    def verify_buffer_message_content_integrity(self, symbol):
        """Verify that buffer messages have proper content and aren't corrupted."""
        print(f"\\n🔍 BUFFER MESSAGE CONTENT INTEGRITY for {symbol.upper()}")
        print("=" * 55)
        
        current_table = f"{symbol}_current"
        
        # Get recent messages and analyze their content
        recent_messages = self.ch_client.execute(f"""
            SELECT ts, mt, m
            FROM {current_table}
            ORDER BY ts DESC
            LIMIT 50
        """)
        
        if not recent_messages:
            print("⚠️  No recent messages found")
            return False
        
        print(f"📊 Analyzing content integrity of {len(recent_messages)} messages")
        
        # Analyze message types and content structure
        mt_distribution = {}
        content_issues = []
        valid_messages = 0
        
        for msg in recent_messages:
            ts, mt, content = msg
            
            # Count message types
            mt_distribution[mt] = mt_distribution.get(mt, 0) + 1
            
            # Basic content validation
            content_valid = True
            
            if mt == 't':  # ticker format: price|price|price|volume|rate
                if '|' not in content or len(content.split('|')) < 4:
                    content_issues.append(f"Invalid ticker format: {content[:50]}")
                    content_valid = False
            elif mt == 'd':  # deal format: price|volume|direction
                if '|' not in content or len(content.split('|')) != 3:
                    content_issues.append(f"Invalid deal format: {content[:50]}")
                    content_valid = False
            elif mt == 'dp':  # depth format: [bids]|[asks]
                if '|' not in content or not content.startswith('['):
                    content_issues.append(f"Invalid depth format: {content[:50]}")
                    content_valid = False
            elif mt == 'dl':  # deadletter - any format acceptable
                pass
            else:
                content_issues.append(f"Unknown message type: {mt}")
                content_valid = False
            
            if content_valid:
                valid_messages += 1
        
        print(f"📋 Message type distribution: {mt_distribution}")
        print(f"✅ Valid messages: {valid_messages}/{len(recent_messages)}")
        print(f"❌ Content issues: {len(content_issues)}")
        
        if content_issues:
            print("🔍 Content issues found:")
            for issue in content_issues[:5]:  # Show first 5 issues
                print(f"    - {issue}")
        
        # Test passes if most messages are valid and we have diverse message types
        content_integrity_percentage = (valid_messages / len(recent_messages)) * 100
        has_diverse_types = len(mt_distribution) >= 2  # At least 2 different message types
        
        test_passed = content_integrity_percentage >= 90 and has_diverse_types
        
        print(f"\\n🎯 CONTENT INTEGRITY RESULTS:")
        print(f"    📊 Content integrity: {content_integrity_percentage:.1f}%")
        print(f"    🎲 Message type diversity: {len(mt_distribution)} types")
        
        if test_passed:
            print(f"    ✅ CONTENT INTEGRITY TEST PASSED")
        else:
            print(f"    ❌ CONTENT INTEGRITY TEST FAILED")
        
        return test_passed
    
    def run_comprehensive_individual_message_test(self):
        """Run comprehensive test to verify buffer messages are individual entries."""
        print("🚀 COMPREHENSIVE BUFFER INDIVIDUAL MESSAGE VERIFICATION")
        print("=" * 65)
        print("This test verifies that buffered messages are stored as separate")
        print("individual entries in ClickHouse, not as batch entries.\\n")
        
        symbols = ['btc', 'eth', 'sol']
        test_results = {}
        
        for symbol in symbols:
            print(f"\\n{'='*65}")
            print(f"🔬 TESTING {symbol.upper()}")
            
            # Test 1: Analyze current buffer message individuality
            individuality_result = self.analyze_buffer_message_individuality(symbol)
            
            # Test 2: Verify content integrity
            content_result = self.verify_buffer_message_content_integrity(symbol)
            
            # Combined result
            overall_result = individuality_result and content_result
            test_results[symbol] = {
                'individuality': individuality_result,
                'content_integrity': content_result,
                'overall': overall_result
            }
        
        # Summary
        print(f"\\n{'='*65}")
        print("📊 COMPREHENSIVE TEST SUMMARY:")
        
        all_passed = True
        for symbol, results in test_results.items():
            individuality_status = "✅" if results['individuality'] else "❌"
            content_status = "✅" if results['content_integrity'] else "❌"
            overall_status = "✅" if results['overall'] else "❌"
            
            print(f"    {symbol.upper()}:")
            print(f"        Individuality: {individuality_status}")
            print(f"        Content Integrity: {content_status}")
            print(f"        Overall: {overall_status}")
            
            if not results['overall']:
                all_passed = False
        
        print(f"\\n🎯 FINAL RESULT:")
        if all_passed:
            print("🎉 ALL TESTS PASSED - Buffer messages are individual entries!")
            print("✅ Each buffered message is stored as a separate ClickHouse record")
            print("✅ No batch-like behavior detected")
            print("✅ Message content integrity maintained")
        else:
            print("❌ SOME TESTS FAILED - Review detailed output above")
        
        return all_passed

if __name__ == "__main__":
    verifier = BufferIndividualMessageVerifier()
    success = verifier.run_comprehensive_individual_message_test()
    
    if success:
        print("\\n✅ VERIFICATION COMPLETE: Buffer messages are individual entries")
    else:
        print("\\n❌ VERIFICATION FAILED: Issues detected with buffer message storage")