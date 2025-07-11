import asyncio
import json
import logging
import websockets
import config
import requests
from datetime import datetime
import time
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ClickHouseRealtimeClient:
    """Real-time ClickHouse HTTP client for immediate per-change MEXC data ingestion."""
    
    def __init__(self):
        self.clickhouse_url = f"http://{config.CLICKHOUSE_HOST}:8123"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'X-ClickHouse-User': config.CLICKHOUSE_USER,
            'X-ClickHouse-Key': config.CLICKHOUSE_PASSWORD,
            'X-ClickHouse-Database': config.CLICKHOUSE_DB
        })
        
        # Statistics tracking
        self.stats = {
            'deals': 0,
            'klines': 0, 
            'tickers': 0,
            'depth': 0,
            'errors': 0,
            'total_inserts': 0
        }
        self.last_stats_time = time.time()
        self.last_message_time = {}  # Track timing for real-time verification
    
    def get_table_from_channel(self, channel: str) -> Optional[str]:
        """Maps a WebSocket channel to a ClickHouse table (direct, no buffering)."""
        if 'push.deal' in channel:
            return "deals"
        elif 'push.kline' in channel:
            return "klines"
        elif 'push.ticker' in channel:
            return "tickers"
        elif 'push.depth.full' in channel:
            return "depth"
        return None
    
    def extract_data_from_mexc_message(self, data: Dict[str, Any], table_name: str, raw_json: str) -> Optional[Dict[str, Any]]:
        """Extract and transform MEXC WebSocket data into ClickHouse format with raw preservation."""
        try:
            if table_name == "deals":
                return {
                    'ts': datetime.fromtimestamp(int(data['data']['t']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'timestamp': datetime.fromtimestamp(int(data['data']['t']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'symbol': data['symbol'],
                    'price': float(data['data']['p']),
                    'volume': int(data['data']['v']),
                    'side': 'buy' if int(data['data']['T']) == 1 else 'sell',
                    'raw_message': raw_json  # Preserve complete raw JSON
                }
                
            elif table_name == "klines":
                return {
                    'ts': datetime.fromtimestamp(int(data['data']['t'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'timestamp': datetime.fromtimestamp(int(data['data']['t'])).strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': data['symbol'],
                    'kline_type': data['data']['interval'],
                    'open': float(data['data']['o']),
                    'close': float(data['data']['c']),
                    'high': float(data['data']['h']),
                    'low': float(data['data']['l']),
                    'volume': float(data['data']['q']),
                    'raw_message': raw_json  # Preserve complete raw JSON
                }
                
            elif table_name == "tickers":
                return {
                    'ts': datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'timestamp': datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'symbol': data['symbol'],
                    'last_price': float(data['data']['lastPrice']),
                    'bid1_price': float(data['data']['bid1']),
                    'ask1_price': float(data['data']['ask1']),
                    'volume_24h': float(data['data']['volume24']),
                    'hold_vol': float(data['data']['holdVol']),
                    'fair_price_from_ticker': float(data['data']['fairPrice']) if 'fairPrice' in data['data'] else None,
                    'index_price_from_ticker': float(data['data']['indexPrice']) if 'indexPrice' in data['data'] else None,
                    'funding_rate_from_ticker': data['data']['fundingRate'] if 'fundingRate' in data['data'] else None,
                    'raw_message': raw_json  # Preserve complete raw JSON
                }
                
            elif table_name == "depth":
                # Convert bid/ask arrays to ClickHouse tuple format
                bids = [(float(bid[0]), float(bid[1])) for bid in data['data']['bids']]
                asks = [(float(ask[0]), float(ask[1])) for ask in data['data']['asks']]
                
                return {
                    'ts': datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'timestamp': datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'symbol': data['symbol'],
                    'version': int(data['data']['version']),
                    'bids': bids,
                    'asks': asks,
                    'raw_message': raw_json  # Preserve complete raw JSON
                }
                
        except (KeyError, ValueError, TypeError) as e:
            logging.error(f"Data extraction error for {table_name}: {e}, data: {data}")
            return None
        
        return None
    
    def send_to_clickhouse_immediate(self, table_name: str, record: Dict[str, Any]) -> bool:
        """Send a single record immediately to ClickHouse MergeTree table (no buffering)."""
        try:
            # Track message receive time for real-time monitoring
            self.last_message_time[table_name] = time.time()
            
            # Convert record to INSERT statement
            columns = list(record.keys())
            values = list(record.values())
            
            # Format values for ClickHouse with proper escaping
            formatted_values = []
            for value in values:
                if value is None:
                    formatted_values.append('NULL')
                elif isinstance(value, str):
                    # Proper SQL escaping for strings
                    escaped_value = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
                    formatted_values.append(f"'{escaped_value}'")
                elif isinstance(value, (list, tuple)):
                    # Format arrays/tuples for ClickHouse
                    formatted_values.append(str(value).replace("'", ""))
                else:
                    formatted_values.append(str(value))
            
            # Build INSERT statement for immediate execution
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(formatted_values)})"
            
            # Send immediately to ClickHouse (no batching, no buffering)
            response = self.session.post(
                self.clickhouse_url,
                data=query,
                timeout=2  # Short timeout for real-time performance
            )
            
            if response.status_code == 200:
                self.stats['total_inserts'] += 1
                
                # Real-time verification: log insert latency
                insert_latency = (time.time() - self.last_message_time[table_name]) * 1000
                if insert_latency > 100:  # Log if insert takes >100ms
                    logging.warning(f"⚠️ High insert latency for {table_name}: {insert_latency:.1f}ms")
                
                return True
            else:
                logging.error(f"ClickHouse error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"Failed to send to ClickHouse: {e}")
            return False
    
    def log_error_to_dead_letter(self, table_name: str, raw_data: str, error_msg: str):
        """Log failed records to dead letter table."""
        try:
            escaped_raw = raw_data.replace("\\", "\\\\").replace("'", "\\'")
            escaped_error = error_msg.replace("\\", "\\\\").replace("'", "\\'")
            query = f"""INSERT INTO dead_letter (table_name, raw_data, error_message) 
                       VALUES ('{table_name}', '{escaped_raw}', '{escaped_error}')"""
            self.session.post(self.clickhouse_url, data=query, timeout=2)
        except Exception as e:
            logging.error(f"Failed to log to dead letter table: {e}")
    
    def print_realtime_statistics(self):
        """Print real-time performance statistics."""
        current_time = time.time()
        elapsed = current_time - self.last_stats_time
        
        if elapsed >= 30:  # Print stats every 30 seconds for real-time monitoring
            total_records = sum(self.stats[k] for k in ['deals', 'klines', 'tickers', 'depth'])
            rate = total_records / elapsed if elapsed > 0 else 0
            
            # Calculate real-time latencies
            latency_info = ""
            for table_name in ['deals', 'klines', 'tickers', 'depth']:
                if table_name in self.last_message_time:
                    last_msg_age = (current_time - self.last_message_time[table_name])
                    latency_info += f"{table_name}:{last_msg_age:.1f}s "
            
            logging.info(f"📊 Real-time Stats (last {elapsed:.0f}s): "
                        f"Rate: {rate:.1f} msg/sec, "
                        f"Total: {self.stats['total_inserts']}, "
                        f"Errors: {self.stats['errors']}, "
                        f"Last msgs: {latency_info}")
            
            # Reset stats
            for key in ['deals', 'klines', 'tickers', 'depth', 'errors']:
                self.stats[key] = 0
            self.last_stats_time = current_time

# Global client instance
clickhouse_client = None

async def send_ping(ws):
    """Sends a ping to the server every 20 seconds to keep the connection alive."""
    while True:
        try:
            await ws.send(json.dumps({"method": "ping"}))
            await asyncio.sleep(20)
        except websockets.exceptions.ConnectionClosed:
            logging.info("Connection closed, stopping ping task.")
            break

async def subscribe(ws):
    """Subscribes to all channels defined in the config."""
    for sub in config.SUBSCRIPTIONS.values():
        await ws.send(json.dumps(sub))
        logging.info(f"Subscribed to {sub['method']} for {sub['param'].get('symbol')}")
        await asyncio.sleep(0.1)  # Avoid rate limiting

async def mexc_websocket_client():
    """The main WebSocket client loop with immediate per-change processing."""
    uri = config.MEXC_WSS_URL
    async for ws in websockets.connect(uri, ping_interval=None):
        try:
            # Start the background ping task
            ping_task = asyncio.create_task(send_ping(ws))
            
            await subscribe(ws)
            while True:
                message = await ws.recv()
                message_receive_time = time.time()  # Track for real-time monitoring
                
                data = json.loads(message)
                
                # Handle PONG response for keep-alive
                if data.get("method") == "PING":
                    await ws.send(json.dumps({"method": "PONG"}))
                    continue

                channel = data.get('channel')
                if not channel:
                    continue

                table_name = clickhouse_client.get_table_from_channel(channel)
                if table_name:
                    # Extract and transform data with raw JSON preservation
                    record = clickhouse_client.extract_data_from_mexc_message(data, table_name, message)
                    if record:
                        # Send IMMEDIATELY to ClickHouse (no batching, no buffering)
                        success = clickhouse_client.send_to_clickhouse_immediate(table_name, record)
                        if success:
                            # Update statistics
                            clickhouse_client.stats[table_name] += 1
                            
                            # Real-time monitoring: log high-frequency streams less verbosely
                            if table_name in ['deals', 'depth']:
                                if clickhouse_client.stats[table_name] % 10 == 0:  # Log every 10th for high-freq
                                    logging.info(f"🟢 {table_name} #{clickhouse_client.stats[table_name]} → ClickHouse")
                            else:
                                logging.info(f"🟢 {table_name} → ClickHouse (immediate)")
                        else:
                            clickhouse_client.stats['errors'] += 1
                            clickhouse_client.log_error_to_dead_letter(table_name, message, "Insert failed")
                    else:
                        clickhouse_client.stats['errors'] += 1
                        clickhouse_client.log_error_to_dead_letter(table_name, message, "Data extraction failed")
                
                # Print periodic real-time statistics
                clickhouse_client.print_realtime_statistics()

        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"Connection closed: {e}. Reconnecting...")
            ping_task.cancel()
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            ping_task.cancel()
            break

async def main():
    """Initializes real-time client and runs the WebSocket connection."""
    global clickhouse_client
    
    try:
        clickhouse_client = ClickHouseRealtimeClient()
        logging.info("✅ ClickHouse real-time client initialized (immediate per-change inserts).")
        
        while True:
            await mexc_websocket_client()
            logging.info("Reconnecting after 5 seconds...")
            await asyncio.sleep(5)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("Shutdown signal received, initiating graceful exit...")
    except Exception as e:
        logging.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        if clickhouse_client and clickhouse_client.session:
            clickhouse_client.session.close()
            logging.info("ClickHouse session closed.")
        logging.info("Real-time client has shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Real-time client shutdown complete.")