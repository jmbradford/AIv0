import asyncio
import json
import logging
import websockets
import config
import requests
from datetime import datetime
import time
from typing import Dict, Any, Optional
from collections import deque
import threading
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ClickHouseState(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"
    RECOVERING = "recovering"

class ClickHouseClient:
    """ClickHouse HTTP client for MEXC data ingestion."""
    
    def __init__(self):
        self.clickhouse_url = f"http://{config.CLICKHOUSE_HOST}:8123"
        self.session = requests.Session()
        headers = {
            'Content-Type': 'application/json',
            'X-ClickHouse-Database': config.CLICKHOUSE_DB,
            'X-ClickHouse-User': config.CLICKHOUSE_USER
        }
        # Only add password header if password is set
        if config.CLICKHOUSE_PASSWORD:
            headers['X-ClickHouse-Key'] = config.CLICKHOUSE_PASSWORD
        self.session.headers.update(headers)
        
        # ClickHouse health monitoring
        self.clickhouse_state = ClickHouseState.HEALTHY
        self.consecutive_failures = 0
        self.max_failures_before_circuit_break = 3
        self.last_health_check = 0
        self.health_check_interval = 1  # Check every 1 second when healthy
        self.recovery_check_interval = 0.1  # Check every 100ms when recovering
        self.circuit_breaker_timeout = 5  # Seconds to wait before attempting recovery
        self.last_successful_insert = time.time()
        
        # Emergency data buffer for ClickHouse outages (FIFO for sequential integrity)
        self.emergency_buffer = deque(maxlen=10000)  # Buffer up to 10k records
        self.buffer_flush_lock = threading.Lock()
        self.max_buffer_age = 30  # Seconds before buffered data expires
        
        # Statistics tracking
        self.stats = {
            'deal': 0,
            'kline': 0, 
            'ticker': 0,
            'depth': 0,
            'errors': 0,
            'total_inserts': 0,
            'buffered_records': 0,
            'recovered_records': 0,
            'lost_records': 0
        }
        self.last_stats_time = time.time()
        self.start_time = time.time()  # Track total runtime
        self.last_message_time = {}  # Track timing for real-time verification
        self.insert_latencies = []  # Track ClickHouse insert latencies
        
        # Start background health monitoring
        self.health_monitor_thread = threading.Thread(target=self._health_monitor_loop, daemon=True)
        self.health_monitor_thread.start()
    
    def get_message_type_from_channel(self, channel: str) -> Optional[str]:
        """Maps a WebSocket channel to the message type for the mexc_messages table."""
        if 'push.deal' in channel:
            return "deal"
        elif 'push.kline' in channel:
            return "kline"
        elif 'push.ticker' in channel:
            return "ticker"
        elif 'push.depth.full' in channel:
            return "depth"
        return None
    
    def _health_monitor_loop(self):
        """Background thread that continuously monitors ClickHouse health."""
        while True:
            try:
                current_time = time.time()
                
                # Determine check interval based on current state
                if self.clickhouse_state in [ClickHouseState.DOWN, ClickHouseState.RECOVERING]:
                    check_interval = self.recovery_check_interval
                else:
                    check_interval = self.health_check_interval
                
                if current_time - self.last_health_check >= check_interval:
                    self._perform_health_check()
                    self.last_health_check = current_time
                
                # Attempt to flush buffer if ClickHouse is healthy
                if self.clickhouse_state == ClickHouseState.HEALTHY and len(self.emergency_buffer) > 0:
                    self._flush_emergency_buffer()
                
                time.sleep(0.05)  # 50ms loop for fast recovery
                
            except Exception as e:
                logging.error(f"Health monitor error: {e}")
                time.sleep(1)
    
    def _perform_health_check(self):
        """Perform a lightweight health check on ClickHouse."""
        try:
            # Quick SELECT 1 test
            response = self.session.post(
                self.clickhouse_url,
                data="SELECT 1",
                timeout=1  # Very short timeout for health checks
            )
            
            if response.status_code == 200:
                if self.clickhouse_state != ClickHouseState.HEALTHY:
                    logging.info(f"ClickHouse RECOVERED! State: {self.clickhouse_state.value} → HEALTHY")
                    self.clickhouse_state = ClickHouseState.HEALTHY
                    self.consecutive_failures = 0
                return True
            else:
                self._handle_health_check_failure(f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self._handle_health_check_failure(str(e))
            return False
    
    def _handle_health_check_failure(self, error_msg: str):
        """Handle failed health checks with circuit breaker logic."""
        self.consecutive_failures += 1
        
        if self.consecutive_failures >= self.max_failures_before_circuit_break:
            if self.clickhouse_state != ClickHouseState.DOWN:
                logging.error(f"ClickHouse DOWN! {self.consecutive_failures} consecutive failures. Error: {error_msg}")
                self.clickhouse_state = ClickHouseState.DOWN
        else:
            if self.clickhouse_state == ClickHouseState.HEALTHY:
                logging.warning(f"ClickHouse DEGRADED! Failure {self.consecutive_failures}/{self.max_failures_before_circuit_break}. Error: {error_msg}")
                self.clickhouse_state = ClickHouseState.DEGRADED
    
    def _flush_emergency_buffer(self):
        """Flush buffered records to ClickHouse when healthy (maintains sequential order)."""
        if not self.emergency_buffer:
            return
            
        with self.buffer_flush_lock:
            records_to_flush = []
            current_time = time.time()
            
            # Process buffer in FIFO order to maintain sequential integrity
            while self.emergency_buffer:
                buffered_record = self.emergency_buffer.popleft()
                record_age = current_time - buffered_record['buffered_at']
                
                # Drop expired records to prevent stale data
                if record_age > self.max_buffer_age:
                    self.stats['lost_records'] += 1
                    logging.warning(f"Dropping expired buffered record (age: {record_age:.1f}s)")
                    continue
                
                records_to_flush.append(buffered_record)
                
                # Flush in small batches to maintain real-time performance
                if len(records_to_flush) >= 50:
                    break
            
            # Attempt to flush collected records
            successful_flushes = 0
            for buffered_record in records_to_flush:
                message_type = buffered_record['message_type']
                table_name = f"mexc_{message_type}"
                if self._send_to_clickhouse_direct(table_name, buffered_record['record']):
                    successful_flushes += 1
                    self.stats['recovered_records'] += 1
                else:
                    # Put failed records back at front of buffer (maintain order)
                    self.emergency_buffer.appendleft(buffered_record)
                    break  # Stop flushing if we hit another failure
            
            if successful_flushes > 0:
                logging.info(f"Recovered {successful_flushes} buffered records from emergency buffer")
    
    def extract_data_from_mexc_message(self, data: Dict[str, Any], message_type: str, raw_json: str) -> Optional[Dict[str, Any]]:
        """Extract timestamp and parse all values from MEXC message into typed columns."""
        try:
            # Extract timestamp based on message type
            if message_type == "deal":
                ts = datetime.fromtimestamp(int(data['data']['t']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            elif message_type == "kline":
                ts = datetime.fromtimestamp(int(data['data']['t'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            elif message_type == "ticker":
                ts = datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            elif message_type == "depth":
                ts = datetime.fromtimestamp(int(data['ts']) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            else:
                # Fallback timestamp
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # Extract and structure data based on message type with multi-line format
            if message_type == "deal":
                return self._extract_deal_data_multiline(data, ts)
            elif message_type == "kline":
                return self._extract_kline_data_multiline(data, ts)
            elif message_type == "ticker":
                return self._extract_ticker_data_multiline(data, ts)
            elif message_type == "depth":
                return self._extract_depth_data_multiline(data, ts)
            
            return None
                
        except (KeyError, ValueError, TypeError) as e:
            logging.error(f"Data extraction error for {message_type}: {e}, data: {data}")
            return None
    
    def _extract_deal_data_multiline(self, data: Dict[str, Any], ts: str) -> Dict[str, Any]:
        """Extract deal fields into multi-line string format for mexc_data table."""
        deal_data = []
        deal_data.append(f"symbol={data.get('symbol', '')}")
        deal_data.append(f"price={float(data['data'].get('p', 0))}")
        deal_data.append(f"volume={float(data['data'].get('v', 0))}")
        deal_data.append(f"side={'buy' if data['data'].get('T') == 1 else 'sell'}")
        deal_data.append(f"tradeType={int(data['data'].get('T', 0))}")
        deal_data.append(f"orderType={int(data['data'].get('O', 0))}")
        deal_data.append(f"matchType={int(data['data'].get('M', 0))}")
        deal_data.append(f"tradeTime={int(data['data'].get('t', 0))}")
        
        return {
            'ts': ts,
            'ticker': '',
            'kline': '',
            'deal': '\n'.join(deal_data),
            'depth': '',
            'dl': ''
        }
    
    def _extract_kline_data_multiline(self, data: Dict[str, Any], ts: str) -> Dict[str, Any]:
        """Extract kline fields into multi-line string format for mexc_data table."""
        kline_data = []
        kline_data.append(f"symbol={data.get('symbol', '')}")
        kline_data.append(f"interval={data['data'].get('interval', '')}")
        kline_data.append(f"startTime={int(data['data'].get('t', 0))}")
        kline_data.append(f"open={float(data['data'].get('o', 0))}")
        kline_data.append(f"close={float(data['data'].get('c', 0))}")
        kline_data.append(f"high={float(data['data'].get('h', 0))}")
        kline_data.append(f"low={float(data['data'].get('l', 0))}")
        kline_data.append(f"amount={float(data['data'].get('a', 0))}")
        kline_data.append(f"quantity={int(data['data'].get('q', 0))}")
        kline_data.append(f"realOpen={float(data['data'].get('ro', 0))}")
        kline_data.append(f"realClose={float(data['data'].get('rc', 0))}")
        kline_data.append(f"realHigh={float(data['data'].get('rh', 0))}")
        kline_data.append(f"realLow={float(data['data'].get('rl', 0))}")
        
        return {
            'ts': ts,
            'ticker': '',
            'kline': '\n'.join(kline_data),
            'deal': '',
            'depth': '',
            'dl': ''
        }
    
    def _extract_ticker_data_multiline(self, data: Dict[str, Any], ts: str) -> Dict[str, Any]:
        """Extract ticker fields into multi-line string format for mexc_data table."""
        ticker_data = []
        ticker_data.append(f"symbol={data.get('symbol', '')}")
        ticker_data.append(f"lastPrice={float(data['data'].get('lastPrice', 0))}")
        ticker_data.append(f"riseFallRate={float(data['data'].get('riseFallRate', 0))}")
        ticker_data.append(f"riseFallValue={float(data['data'].get('riseFallValue', 0))}")
        ticker_data.append(f"fairPrice={float(data['data'].get('fairPrice', 0))}")
        ticker_data.append(f"indexPrice={float(data['data'].get('indexPrice', 0))}")
        ticker_data.append(f"volume24={int(data['data'].get('volume24', 0))}")
        ticker_data.append(f"amount24={float(data['data'].get('amount24', 0))}")
        ticker_data.append(f"high24Price={float(data['data'].get('high24Price', 0))}")
        ticker_data.append(f"lower24Price={float(data['data'].get('lower24Price', 0))}")
        ticker_data.append(f"maxBidPrice={float(data['data'].get('maxBidPrice', 0))}")
        ticker_data.append(f"minAskPrice={float(data['data'].get('minAskPrice', 0))}")
        ticker_data.append(f"fundingRate={float(data['data'].get('fundingRate', 0))}")
        ticker_data.append(f"bid1={float(data['data'].get('bid1', 0))}")
        ticker_data.append(f"ask1={float(data['data'].get('ask1', 0))}")
        ticker_data.append(f"holdVol={int(data['data'].get('holdVol', 0))}")
        ticker_data.append(f"timestamp={int(data['data'].get('timestamp', 0))}")
        ticker_data.append(f"zone={data['data'].get('zone', 'UTC+8')}")
        
        # Handle arrays by converting to string representation
        rise_fall_rates = data['data'].get('riseFallRates', [])
        if rise_fall_rates:
            ticker_data.append(f"riseFallRates={','.join(map(str, rise_fall_rates))}")
        
        rise_fall_rates_tz = data['data'].get('riseFallRatesOfTimezone', [])
        if rise_fall_rates_tz:
            ticker_data.append(f"riseFallRatesOfTimezone={','.join(map(str, rise_fall_rates_tz))}")
        
        return {
            'ts': ts,
            'ticker': '\n'.join(ticker_data),
            'kline': '',
            'deal': '',
            'depth': '',
            'dl': ''
        }
    
    def _extract_depth_data_multiline(self, data: Dict[str, Any], ts: str) -> Dict[str, Any]:
        """Extract depth fields into multi-line string format for mexc_data table."""
        depth_info = data['data']
        depth_data = []
        
        depth_data.append(f"symbol={data.get('symbol', '')}")
        depth_data.append(f"version={int(depth_info.get('version', 0))}")
        depth_data.append(f"begin={int(depth_info.get('begin', 0))}")
        depth_data.append(f"end={int(depth_info.get('end', 0))}")
        
        # Extract and format bid/ask arrays
        bids = depth_info.get('bids', [])
        asks = depth_info.get('asks', [])
        
        if bids:
            best_bid_price = float(bids[0][0]) if bids else 0.0
            best_bid_qty = int(bids[0][1]) if bids else 0
            depth_data.append(f"bestBidPrice={best_bid_price}")
            depth_data.append(f"bestBidQty={best_bid_qty}")
            depth_data.append(f"bidLevels={len(bids)}")
            
            # Convert bids array to string format
            bids_str = '|'.join([f"{float(bid[0])},{int(bid[1])},{int(bid[2]) if len(bid) > 2 else 1}" for bid in bids])
            depth_data.append(f"bids={bids_str}")
        
        if asks:
            best_ask_price = float(asks[0][0]) if asks else 0.0
            best_ask_qty = int(asks[0][1]) if asks else 0
            depth_data.append(f"bestAskPrice={best_ask_price}")
            depth_data.append(f"bestAskQty={best_ask_qty}")
            depth_data.append(f"askLevels={len(asks)}")
            
            # Convert asks array to string format
            asks_str = '|'.join([f"{float(ask[0])},{int(ask[1])},{int(ask[2]) if len(ask) > 2 else 1}" for ask in asks])
            depth_data.append(f"asks={asks_str}")
        
        return {
            'ts': ts,
            'ticker': '',
            'kline': '',
            'deal': '',
            'depth': '\n'.join(depth_data),
            'dl': ''
        }
    
    def send_to_clickhouse(self, message_type: str, record: Dict[str, Any]) -> bool:
        """Send a single record to ClickHouse specialized table with fallback buffering."""
        # Track message receive time for monitoring
        self.last_message_time[message_type] = time.time()
        
        # Check ClickHouse state before attempting insert
        if self.clickhouse_state == ClickHouseState.DOWN:
            return self._buffer_record_for_recovery(message_type, record)
        
        # Route to single optimized table
        table_name = "mexc_data"
        success = self._send_to_clickhouse_direct(table_name, record)
        
        if success:
            self.last_successful_insert = time.time()
            # Reset failure counter on successful insert
            if self.consecutive_failures > 0:
                self.consecutive_failures = max(0, self.consecutive_failures - 1)
                if self.clickhouse_state == ClickHouseState.DEGRADED and self.consecutive_failures == 0:
                    self.clickhouse_state = ClickHouseState.HEALTHY
                    logging.info("ClickHouse state restored to HEALTHY")
            return True
        else:
            # Handle failure based on current state
            if self.clickhouse_state == ClickHouseState.HEALTHY:
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.max_failures_before_circuit_break:
                    self.clickhouse_state = ClickHouseState.DOWN
                    logging.error(f"ClickHouse circuit breaker activated - switching to buffer mode")
                else:
                    self.clickhouse_state = ClickHouseState.DEGRADED
                    logging.warning(f"ClickHouse degraded - insert failure {self.consecutive_failures}/{self.max_failures_before_circuit_break}")
            
            # Buffer the failed record for later recovery
            return self._buffer_record_for_recovery(message_type, record)
    
    def _send_to_clickhouse_direct(self, table_name: str, record: Dict[str, Any]) -> bool:
        """Direct ClickHouse insert without buffering logic."""
        try:
            # Convert record to INSERT statement
            columns = list(record.keys())
            values = list(record.values())
            
            # Format values for ClickHouse with proper escaping and type handling
            formatted_values = []
            for value in values:
                if value is None:
                    formatted_values.append('NULL')
                elif isinstance(value, str):
                    # Proper SQL escaping for strings
                    escaped_value = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
                    formatted_values.append(f"'{escaped_value}'")
                elif isinstance(value, list):
                    # Format arrays for ClickHouse - handle different array types
                    if len(value) > 0:
                        if isinstance(value[0], tuple):
                            # Array of tuples (for depth data)
                            tuple_strings = []
                            for item in value:
                                tuple_values = [f"'{str(v)}'" if isinstance(v, str) else str(v) for v in item]
                                tuple_strings.append(f"({', '.join(tuple_values)})")
                            formatted_values.append(f"[{', '.join(tuple_strings)}]")
                        else:
                            # Array of simple values - handle numeric arrays
                            if all(isinstance(v, (int, float)) for v in value):
                                array_values = [str(v) for v in value]
                            else:
                                array_values = [f"'{str(v)}'" for v in value]
                            formatted_values.append(f"[{', '.join(array_values)}]")
                    else:
                        formatted_values.append('[]')
                elif isinstance(value, tuple):
                    # Format tuples for ClickHouse
                    tuple_values = [f"'{str(v)}'" if isinstance(v, str) else str(v) for v in value]
                    formatted_values.append(f"({', '.join(tuple_values)})")
                else:
                    formatted_values.append(str(value))
            
            # Build INSERT statement for immediate execution
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(formatted_values)})"
            
            # Send to ClickHouse with adaptive timeout and latency tracking
            timeout = 0.5 if self.clickhouse_state == ClickHouseState.HEALTHY else 2
            start_time = time.time()
            response = self.session.post(
                self.clickhouse_url,
                data=query,
                timeout=timeout
            )
            insert_latency = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            if response.status_code == 200:
                self.stats['total_inserts'] += 1
                
                # Track insert latency for statistics (keep last 100 measurements)
                self.insert_latencies.append(insert_latency)
                if len(self.insert_latencies) > 100:
                    self.insert_latencies.pop(0)
                
                # Log insert latency if unusually high
                if insert_latency > 500:  # Log if insert takes >500ms
                    logging.warning(f"High insert latency: {insert_latency:.1f}ms")
                
                return True
            else:
                logging.error(f"ClickHouse HTTP error: {response.status_code} - {response.text[:200]}")
                return False
                
        except requests.exceptions.Timeout:
            logging.error(f"ClickHouse timeout (timeout: {timeout}s)")
            return False
        except requests.exceptions.ConnectionError:
            logging.error(f"ClickHouse connection error")
            return False
        except Exception as e:
            logging.error(f"ClickHouse unexpected error: {e}")
            return False
    
    def _buffer_record_for_recovery(self, message_type: str, record: Dict[str, Any]) -> bool:
        """Buffer record for later recovery when ClickHouse is available."""
        try:
            buffered_record = {
                'message_type': message_type,
                'record': record,
                'buffered_at': time.time()
            }
            
            with self.buffer_flush_lock:
                self.emergency_buffer.append(buffered_record)
                self.stats['buffered_records'] += 1
            
            # Log buffer status periodically
            if len(self.emergency_buffer) % 100 == 0:
                logging.warning(f"Emergency buffer size: {len(self.emergency_buffer)} records")
            
            return True  # Successfully buffered
            
        except Exception as e:
            logging.error(f"Failed to buffer record: {e}")
            self.stats['lost_records'] += 1
            return False
    
    def log_error_to_dead_letter(self, message_type: str, raw_data: str, error_msg: str):
        """Log failed records to dead letter table."""
        try:
            # Log to dead letter table
            escaped_raw = raw_data.replace("\\", "\\\\").replace("'", "\\'")
            escaped_error = error_msg.replace("\\", "\\\\").replace("'", "\\'")
            query = f"""INSERT INTO dead_letter (table_name, raw_data, error_message) 
                       VALUES ('{message_type}', '{escaped_raw}', '{escaped_error}')"""
            self.session.post(self.clickhouse_url, data=query, timeout=2)
            
        except Exception as e:
            logging.error(f"Failed to log to dead letter: {e}")
    
    def print_statistics(self):
        """Print performance statistics with reduced frequency and enhanced metrics."""
        current_time = time.time()
        elapsed = current_time - self.last_stats_time
        
        if elapsed >= 60:  # Print stats every 60 seconds (reduced from 30)
            total_records = sum(self.stats[k] for k in ['deal', 'kline', 'ticker', 'depth'])
            rate = total_records / elapsed if elapsed > 0 else 0
            
            # Calculate latency metrics
            avg_latency = 0
            max_latency = 0
            if self.insert_latencies:
                avg_latency = sum(self.insert_latencies) / len(self.insert_latencies)
                max_latency = max(self.insert_latencies)
            
            # Calculate data freshness
            latency_info = ""
            max_age = 0
            for message_type in ['deal', 'kline', 'ticker', 'depth']:
                if message_type in self.last_message_time:
                    last_msg_age = (current_time - self.last_message_time[message_type])
                    latency_info += f"{message_type}:{last_msg_age:.1f}s "
                    max_age = max(max_age, last_msg_age)
            
            # Health status indicators
            health_indicator = {
                ClickHouseState.HEALTHY: "OK",
                ClickHouseState.DEGRADED: "WARN", 
                ClickHouseState.DOWN: "DOWN",
                ClickHouseState.RECOVERING: "RECOVER"
            }.get(self.clickhouse_state, "UNKNOWN")
            
            # Check for data flow issues
            data_flow_status = ""
            if max_age > 60:  # No data for over 1 minute
                data_flow_status = "STALE_DATA "
            elif max_age > 30:  # No data for over 30 seconds
                data_flow_status = "SLOW_FLOW "
            
            # Enhanced status report with new metrics
            buffer_info = f"Buffer: {len(self.emergency_buffer)}" if len(self.emergency_buffer) > 0 else ""
            recovery_info = f"Recovered: {self.stats['recovered_records']}, Lost: {self.stats['lost_records']}" if self.stats['recovered_records'] > 0 or self.stats['lost_records'] > 0 else ""
            latency_info_display = f"Latency: avg={avg_latency:.1f}ms, max={max_latency:.1f}ms" if self.insert_latencies else ""
            
            logging.info(f"STATS {health_indicator} {data_flow_status}(last {elapsed:.0f}s): "
                        f"Rate: {rate:.1f} msg/sec, "
                        f"Total: {self.stats['total_inserts']:,}, "
                        f"Failures: {self.consecutive_failures}, "
                        f"Errors: {self.stats['errors']} "
                        f"{latency_info_display} "
                        f"{buffer_info} {recovery_info} "
                        f"Last msgs: {latency_info}")
            
            # Alert on critical conditions
            if self.clickhouse_state == ClickHouseState.DOWN:
                logging.error(f"CRITICAL: ClickHouse DOWN for {current_time - self.last_successful_insert:.0f}s! Buffer: {len(self.emergency_buffer)} records")
            elif len(self.emergency_buffer) > 5000:
                logging.warning(f"WARNING: Emergency buffer growing large ({len(self.emergency_buffer)} records)")
            elif avg_latency > 1000:  # Alert on high latency
                logging.warning(f"WARNING: High insert latency detected - avg: {avg_latency:.1f}ms, max: {max_latency:.1f}ms")
            
            # Reset periodic stats but keep cumulative counters
            for key in ['deal', 'kline', 'ticker', 'depth', 'errors']:
                self.stats[key] = 0
            self.last_stats_time = current_time
            self.insert_latencies.clear()  # Reset latency tracking

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
    """The main WebSocket client loop with immediate per-change processing and gap detection."""
    uri = config.MEXC_WSS_URL
    reconnect_count = 0
    
    async for ws in websockets.connect(uri, ping_interval=None):
        try:
            reconnect_count += 1
            if reconnect_count > 1:
                logging.info(f"WebSocket reconnected (attempt #{reconnect_count})")
            
            # Start the background ping task
            ping_task = asyncio.create_task(send_ping(ws))
            
            await subscribe(ws)
            message_count = 0
            last_message_time = time.time()
            
            while True:
                try:
                    # Use timeout to detect message gaps
                    message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    message_receive_time = time.time()
                    message_count += 1
                    
                    # Check for message gaps (indicating potential MEXC connectivity issues)
                    gap_duration = message_receive_time - last_message_time
                    if gap_duration > 10 and message_count > 1:  # Ignore first message
                        logging.warning(f"MEXC data gap detected: {gap_duration:.1f}s since last message")
                    
                    last_message_time = message_receive_time
                    
                    data = json.loads(message)
                    
                    # Handle PONG response for keep-alive
                    if data.get("method") == "PING":
                        await ws.send(json.dumps({"method": "PONG"}))
                        continue

                    channel = data.get('channel')
                    if not channel:
                        continue

                    message_type = clickhouse_client.get_message_type_from_channel(channel)
                    if message_type:
                        # Extract and transform data
                        record = clickhouse_client.extract_data_from_mexc_message(data, message_type, message)
                        if record:
                            # Send to ClickHouse with error handling
                            success = clickhouse_client.send_to_clickhouse(message_type, record)
                            if success:
                                # Update statistics
                                clickhouse_client.stats[message_type] += 1
                                
                                # Much quieter logging - only debug level for successful inserts
                                if message_type in ['deal', 'depth']:
                                    if clickhouse_client.stats[message_type] % 100 == 0:  # Log every 100th for high-freq
                                        status_icon = "✓" if clickhouse_client.clickhouse_state == ClickHouseState.HEALTHY else "B"
                                        logging.debug(f"{status_icon} {message_type} #{clickhouse_client.stats[message_type]} → ClickHouse")
                                else:
                                    if clickhouse_client.stats[message_type] % 10 == 0:  # Log every 10th for ticker/kline
                                        status_icon = "✓" if clickhouse_client.clickhouse_state == ClickHouseState.HEALTHY else "B"
                                        logging.debug(f"{status_icon} {message_type} #{clickhouse_client.stats[message_type]} → ClickHouse")
                            else:
                                clickhouse_client.stats['errors'] += 1
                                # Note: buffering is handled inside send_to_clickhouse now
                        else:
                            clickhouse_client.stats['errors'] += 1
                            clickhouse_client.log_error_to_dead_letter(message_type, message, "Data extraction failed")
                    
                    # Print periodic statistics
                    clickhouse_client.print_statistics()
                    
                except asyncio.TimeoutError:
                    # No message received within timeout - potential MEXC connection issue
                    current_time = time.time()
                    gap_duration = current_time - last_message_time
                    logging.error(f"MEXC TIMEOUT: No messages for {gap_duration:.1f}s - connection may be stalled")
                    
                    # Send ping to test connection
                    try:
                        await ws.send(json.dumps({"method": "ping"}))
                        logging.info("Sent keep-alive ping to MEXC")
                    except:
                        logging.error("Failed to send ping - connection appears dead")
                        break  # Force reconnection

        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"WebSocket connection closed: {e}. Initiating fast reconnect...")
            ping_task.cancel()
            await asyncio.sleep(0.1)  # Very short delay for fast reconnection
            continue
        except Exception as e:
            logging.error(f"Unexpected WebSocket error: {e}", exc_info=True)
            ping_task.cancel()
            await asyncio.sleep(1)  # Short delay before retry
            break

async def main():
    """Initializes client and runs the WebSocket connection with error recovery."""
    global clickhouse_client
    
    try:
        clickhouse_client = ClickHouseClient()
        logging.info("ClickHouse client initialized with monitoring and recovery.")
        
        # Verify initial ClickHouse connectivity
        if not clickhouse_client._perform_health_check():
            logging.warning("Initial ClickHouse health check failed - starting in buffer mode")
        
        reconnect_attempts = 0
        max_reconnect_delay = 5
        
        while True:
            try:
                await mexc_websocket_client()
                reconnect_attempts = 0  # Reset on successful connection
                
            except Exception as e:
                reconnect_attempts += 1
                # Exponential backoff with cap for fast recovery
                delay = min(0.1 * (2 ** min(reconnect_attempts, 5)), max_reconnect_delay)
                
                logging.error(f"WebSocket client failed (attempt #{reconnect_attempts}): {e}")
                logging.info(f"Fast reconnect in {delay:.1f}s...")
                
                await asyncio.sleep(delay)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("Shutdown signal received, initiating graceful exit...")
        
        # Final buffer flush attempt
        if clickhouse_client and len(clickhouse_client.emergency_buffer) > 0:
            logging.info(f"Attempting final flush of {len(clickhouse_client.emergency_buffer)} buffered records...")
            clickhouse_client._flush_emergency_buffer()
            
    except Exception as e:
        logging.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        if clickhouse_client and clickhouse_client.session:
            clickhouse_client.session.close()
            logging.info("ClickHouse session closed.")
        logging.info("Client has shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Client shutdown complete.")