#!/usr/bin/env python3
import json
import time
import threading
import websocket
from datetime import datetime
from clickhouse_driver import Client
from config import (
    MEXC_WS_URL, PING_INTERVAL, RECONNECT_DELAY, MAX_RECONNECT_ATTEMPTS,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, 
    MessageType, STATS_INTERVAL, MAX_ERROR_COUNT, BTC_CONFIG
)

class BtcDataPipeline:
    def __init__(self):
        self.ws = None
        self.ch_client = None
        self.running = False
        self.symbol = BTC_CONFIG["symbol"]
        self.table_name = BTC_CONFIG["table_name"]
        self.subscriptions = BTC_CONFIG["subscriptions"]
        self.stats = {
            'total_records': 0,
            'ticker_count': 0,
            'deal_count': 0,
            'depth_count': 0,
            'deadletter_count': 0,
            'errors': 0,
            'last_reset': time.time()
        }
        self.reconnect_count = 0
        
    def connect_clickhouse(self):
        """Establish ClickHouse connection."""
        try:
            self.ch_client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE
            )
            
            # Verify connection and table exists
            table_exists = self.ch_client.execute(f"EXISTS TABLE {self.table_name}")[0][0]
            
            if not table_exists:
                print(f"❌ Table {self.table_name} missing - run ch_setup.py first")
                return False
                
            print(f"✅ Connected to ClickHouse - {self.symbol} append-only mode ready")
            print(f"  Table: {self.table_name}.bin")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to ClickHouse: {e}")
            return False
    
    def extract_timestamp(self, data):
        """Extract timestamp from MEXC message data."""
        if 'data' in data and 'ts' in data['data']:
            return data['data']['ts'] / 1000  # Convert to seconds
        elif 'ts' in data:
            return data['ts'] / 1000
        return time.time()
    
    def format_ticker_data(self, data):
        """Format ticker data for unified message column."""
        d = data.get('data', {})
        
        # Format funding rate as standard decimal (not scientific notation)
        funding_rate = d.get('fundingRate', '0')
        if funding_rate and funding_rate != '0':
            try:
                funding_rate = f"{float(funding_rate):.8f}"
            except (ValueError, TypeError):
                funding_rate = '0.00000000'
        else:
            funding_rate = '0.00000000'
        
        values = [
            str(d.get('lastPrice', '0')),
            str(d.get('fairPrice', '0')),
            str(d.get('indexPrice', '0')),
            str(d.get('holdVol', '0')),  # Fixed: was holdVol24h
            funding_rate
        ]
        return '|'.join(values)
    
    def format_deal_data(self, data):
        """Format deal data for unified message column."""
        d = data.get('data', {})
        price = str(d.get('p', '0'))
        volume = str(d.get('v', '0'))
        direction = str(1 if d.get('T') == 1 else 2)  # 1=BUY, 2=SELL
        return f"{price}|{volume}|{direction}"
    
    def format_depth_data(self, data):
        """Format depth data for unified message column."""
        d = data.get('data', {})
        
        # Format bids - use only price and amount (first 2 elements)
        bids = d.get('bids', [])
        bid_str = ','.join([f"[{b[0]},{b[1]}]" for b in bids[:20]])  # Limit to 20
        
        # Format asks - use only price and amount (first 2 elements)
        asks = d.get('asks', [])
        ask_str = ','.join([f"[{a[0]},{a[1]}]" for a in asks[:20]])  # Limit to 20
        
        return f"{bid_str}|{ask_str}"
    
    def process_message(self, message):
        """Process incoming WebSocket message."""
        try:
            # Handle string messages (like pong responses)
            if isinstance(message, str) and message.strip() in ['pong', 'ping']:
                return
            
            # Try to parse as JSON
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            # Handle ping/pong responses
            if isinstance(data, dict) and data.get('channel') == 'pong':
                return
            
            # Skip non-data messages and subscription confirmations
            if not isinstance(data, dict) or 'channel' not in data:
                return
            
            channel = data.get('channel', '')
            
            # Skip subscription confirmations and ping/pong
            if channel.startswith('rs.') or channel == 'pong':
                return
            
            # Only process push messages (actual data)
            if not channel.startswith('push.'):
                return
            
            timestamp = self.extract_timestamp(data)
            dt = datetime.fromtimestamp(timestamp)
            
            # Determine message type and format data
            if 'ticker' in channel:
                msg_type = MessageType.TICKER.value
                formatted_data = self.format_ticker_data(data)
                self.stats['ticker_count'] += 1
            elif 'deal' in channel:
                msg_type = MessageType.DEAL.value
                formatted_data = self.format_deal_data(data)
                self.stats['deal_count'] += 1
            elif 'depth' in channel:
                msg_type = MessageType.DEPTH.value
                formatted_data = self.format_depth_data(data)
                self.stats['depth_count'] += 1
            else:
                # Deadletter for unknown message types
                msg_type = MessageType.DEADLETTER.value
                formatted_data = str(data)[:500]  # Limit size
                self.stats['deadletter_count'] += 1
            
            # Insert into ClickHouse unified table
            self.insert_data(dt, msg_type, formatted_data)
            
        except json.JSONDecodeError:
            # Handle non-JSON messages
            pass
        except Exception as e:
            print(f"Error processing message: {e}")
            self.stats['errors'] += 1
            if self.stats['errors'] > MAX_ERROR_COUNT:
                print("Maximum error count reached. Shutting down...")
                self.running = False
    
    def insert_data(self, timestamp, msg_type, message_data):
        """Insert data into unified table."""
        try:
            self.ch_client.execute(
                f"INSERT INTO {self.table_name} (ts, mt, m) VALUES",
                [(timestamp, msg_type, message_data)]
            )
            print(f"✓ {msg_type} data appended to {self.table_name}.bin")
            self.stats['total_records'] += 1
        except Exception as e:
            print(f"❌ {self.symbol} insert failed: {e}")
            self.stats['errors'] += 1
    
    def on_message(self, ws, message):
        """WebSocket message handler."""
        self.process_message(message)
    
    def on_error(self, ws, error):
        """WebSocket error handler."""
        print(f"WebSocket error: {error}")
        self.stats['errors'] += 1
    
    def on_close(self, ws, close_status_code, close_msg):
        """WebSocket close handler."""
        print(f"WebSocket closed: {close_status_code} - {close_msg}")
        if self.running and self.reconnect_count < MAX_RECONNECT_ATTEMPTS:
            print(f"Attempting reconnection in {RECONNECT_DELAY} seconds...")
            time.sleep(RECONNECT_DELAY)
            self.connect_websocket()
    
    def on_open(self, ws):
        """WebSocket open handler."""
        print(f"WebSocket connected for {self.symbol}")
        self.reconnect_count = 0
        
        # Subscribe to channels
        for sub in self.subscriptions:
            ws.send(json.dumps(sub))
            print(f"Subscribed to: {sub['method']} for {self.symbol}")
        
        # Start ping thread
        def ping_thread():
            while self.running and ws.sock and ws.sock.connected:
                ws.send(json.dumps({"method": "ping"}))
                time.sleep(PING_INTERVAL)
        
        threading.Thread(target=ping_thread, daemon=True).start()
    
    def connect_websocket(self):
        """Connect to MEXC WebSocket."""
        self.reconnect_count += 1
        self.ws = websocket.WebSocketApp(
            MEXC_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # Run in separate thread
        ws_thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        ws_thread.start()
    
    def print_statistics(self):
        """Print statistics every STATS_INTERVAL seconds."""
        while self.running:
            time.sleep(STATS_INTERVAL)
            elapsed = time.time() - self.stats['last_reset']
            
            print("\n" + "="*50)
            print(f"{self.symbol} STATISTICS (Last {STATS_INTERVAL}s)")
            print("="*50)
            print(f"Total Records Appended: {self.stats['total_records']}")
            print(f"Ticker Messages: {self.stats['ticker_count']} → {self.table_name}.bin")
            print(f"Deal Messages: {self.stats['deal_count']} → {self.table_name}.bin")
            print(f"Depth Messages: {self.stats['depth_count']} → {self.table_name}.bin")
            print(f"Skipped Messages: {self.stats['deadletter_count']}")
            print(f"Errors: {self.stats['errors']}")
            print(f"Rate: {self.stats['total_records']/elapsed:.2f} records/sec")
            print("="*50)
    
    def check_file_sizes(self):
        """Check the size of append-only file."""
        try:
            print(f"📊 Checking {self.symbol} append-only file size...")
            count = self.ch_client.execute(f"SELECT COUNT(*) FROM {self.table_name}")[0][0]
            print(f"  {self.table_name}.bin: {count} records appended")
            
        except Exception as e:
            print(f"❌ File size check failed: {e}")
            self.stats['errors'] += 1
    
    def run(self):
        """Main run loop."""
        print(f"Starting {self.symbol} Data Pipeline...")
        
        # Connect to ClickHouse
        if not self.connect_clickhouse():
            print("Failed to connect to ClickHouse. Exiting...")
            return
        
        self.running = True
        
        # Start statistics thread
        stats_thread = threading.Thread(target=self.print_statistics, daemon=True)
        stats_thread.start()
        
        # Connect to WebSocket
        self.connect_websocket()
        
        try:
            while self.running:
                time.sleep(10)  # Check every 10 seconds
                self.check_file_sizes()
                
        except KeyboardInterrupt:
            print(f"\nShutting down {self.symbol} pipeline gracefully...")
        finally:
            self.running = False
            if self.ws:
                self.ws.close()
            
            print(f"Final {self.symbol} file size check...")
            self.check_file_sizes()
            
            if self.ch_client:
                self.ch_client.disconnect()
            
            print(f"{self.symbol} append-only pipeline stopped.")

if __name__ == "__main__":
    pipeline = BtcDataPipeline()
    pipeline.run()