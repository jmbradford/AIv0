# MEXC Multi-Symbol Cryptocurrency Data Pipeline

A high-performance, real-time cryptocurrency data pipeline that streams live market data from MEXC exchange directly into optimized ClickHouse storage. Designed for minimal storage overhead with continuous append-only file growth and multi-instance support for different cryptocurrencies.

## Architecture Overview

### Core Design Principles
- **Symbol-specific file growth**: Each cryptocurrency (BTC, ETH, SOL) writes to its own continuously growing binary file
- **Zero-complexity storage**: StripeLog engine eliminates ClickHouse parts, merging, and metadata overhead
- **Real-time persistence**: Every message immediately appended with no buffering delays
- **Multi-instance ready**: Run separate clients for different symbols/IP tunneling requirements
- **Ultra-low storage overhead**: Target <500MB/day for entire ClickHouse volume

### Database Architecture

**Three StripeLog Tables (Append-Only):**
```sql
-- BTC data → /var/lib/clickhouse/data/mexc_data/btc/data.bin
CREATE TABLE btc (
    ts DateTime64(3),                           -- MEXC message timestamp  
    mt Enum8('t'=1, 'd'=2, 'dp'=3, 'dl'=4),   -- Message type (1 byte)
    m String                                    -- Unified message data
) ENGINE = StripeLog;

-- ETH data → /var/lib/clickhouse/data/mexc_data/eth/data.bin  
CREATE TABLE eth (...same schema...);

-- SOL data → /var/lib/clickhouse/data/mexc_data/sol/data.bin
CREATE TABLE sol (...same schema...);
```

**Storage Pattern:**
- **btc/data.bin**: Continuous append-only growth for all BTC_USDT messages
- **eth/data.bin**: Continuous append-only growth for all ETH_USDT messages  
- **sol/data.bin**: Continuous append-only growth for all SOL_USDT messages
- **Total files**: Exactly 3 data.bin files in separate directories, no parts/metadata

### Message Format Specification

All messages stored in unified format in `m` column with `mt` indicating type:

**Ticker Messages (mt='t'):**
```
Format: lastPrice|fairPrice|indexPrice|holdVol|fundingRate
Example: 122122.3|122122.3|122133.7|102648627|0.00010000
Fields: All prices in USDT, holdVol as integer, fundingRate as 8-decimal standard format
```

**Deal Messages (mt='d'):**
```
Format: price|volume|direction  
Example: 122115.1|12|2
Fields: price in USDT, volume as decimal, direction (1=BUY, 2=SELL)
```

**Depth Messages (mt='dp'):**
```
Format: bids_string|asks_string
Example: [122125.8,103673],[122125.7,100518]...|[122125.9,110108],[122126,113191]...
Fields: Top 20 bid/ask levels, [price,amount] format
```

**Deadletter (mt='dl'):**
```
Format: raw_message_data (truncated to 500 chars)
Purpose: Captures unknown/malformed messages for debugging
```

## Project Structure

```
/home/user/mexc-pipeline/
├── README.md                 # This comprehensive guide
├── requirements.txt          # Python dependencies
├── config.py                 # Symbol configurations (BTC_CONFIG, ETH_CONFIG, SOL_CONFIG)
├── docker-compose.yml        # ClickHouse container with optimized settings
├── clickhouse-config.xml     # Disables system logs to prevent storage bloat
├── ch_setup.py               # Creates symbol-specific StripeLog tables
├── btcdat.py                 # BTC_USDT WebSocket client → btc/data.bin
├── ethdat.py                 # ETH_USDT WebSocket client → eth/data.bin  
├── soldat.py                 # SOL_USDT WebSocket client → sol/data.bin
├── verif.py                  # Data verification and storage statistics
├── CLAUDE.md                 # Development notes and behaviors
└── venv/                     # Python virtual environment
```

## Complete Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- Python 3.8+ with pip
- Minimum 2GB available disk space
- Stable internet connection for WebSocket streams

### Step 1: Environment Setup
```bash
# Clone or create project directory
mkdir mexc-pipeline && cd mexc-pipeline

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: ClickHouse Setup
```bash
# Start ClickHouse container (downloads ~200MB on first run)
docker-compose up -d

# Wait for ClickHouse to initialize (check with docker ps)
sleep 10

# Create symbol-specific tables and disable system logs
python3 ch_setup.py
```

**Expected output:**
```
Dropping system log tables to prevent storage bloat...
  Dropped system.metric_log
  ...
Creating append-only btc table (btc/data.bin equivalent)...
Creating append-only eth table (eth/data.bin equivalent)...  
Creating append-only sol table (sol/data.bin equivalent)...

Setup Summary:
  BTC data: btc table → btc/data.bin (continuous growth)
  ETH data: eth table → eth/data.bin (continuous growth)
  SOL data: sol table → sol/data.bin (continuous growth)
```

### Step 3: Data Collection (Choose Your Symbol)

**Option A: BTC_USDT Data Collection**
```bash
python3 btcdat.py
```

**Option B: ETH_USDT Data Collection**  
```bash
python3 ethdat.py
```

**Option C: SOL_USDT Data Collection**
```bash
python3 soldat.py
```

**Option D: Multi-Symbol (Run in separate terminals/screens)**
```bash
# Terminal 1
python3 btcdat.py

# Terminal 2  
python3 ethdat.py

# Terminal 3
python3 soldat.py
```

### Step 4: Verification and Monitoring
```bash
# Check data collection status
python3 verif.py
```

## Configuration Details

### config.py Symbol Configurations
```python
BTC_CONFIG = {
    "symbol": "BTC_USDT",
    "table_name": "btc", 
    "subscriptions": [
        {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}},
        {"method": "sub.deal", "param": {"symbol": "BTC_USDT"}},
        {"method": "sub.depth.full", "param": {"symbol": "BTC_USDT", "limit": 20}}
    ]
}
# ETH_CONFIG and SOL_CONFIG follow same pattern
```

### docker-compose.yml Key Settings
```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.3-alpine
    ports:
      - "8123:8123"  # HTTP interface  
      - "9000:9000"  # Native client interface
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./clickhouse-config.xml:/etc/clickhouse-server/config.d/custom-config.xml:ro
    environment:
      CLICKHOUSE_DB: mexc_data
      CLICKHOUSE_USER: default
```

### clickhouse-config.xml (Essential)
```xml
<clickhouse>
    <!-- Disable ALL system logging to prevent storage bloat -->
    <query_log remove="1"/>
    <trace_log remove="1"/>
    <metric_log remove="1"/>
    <!-- ... all system logs disabled ... -->
    <logger>
        <level>warning</level>
        <console>false</console>
    </logger>
</clickhouse>
```

## Real-Time Monitoring

### Client Output (Every 15 seconds)
```
==================================================
BTC_USDT STATISTICS (Last 15s)
==================================================
Total Records Appended: 190
Ticker Messages: 7 → btc/data.bin
Deal Messages: 140 → btc/data.bin  
Depth Messages: 43 → btc/data.bin
Skipped Messages: 0
Errors: 0
Rate: 12.66 records/sec
==================================================
```

### Storage Growth Verification
```bash
python3 verif.py
```

**Expected verification output:**
```
💾 Symbol-specific storage status:

Total records appended: 428
  btc/data.bin: 428 records
  eth/data.bin: 0 records  
  sol/data.bin: 0 records

Records by symbol and message type:
  BTC: 428 total
    t: 13    # Ticker messages
    d: 323   # Deal messages  
    dp: 92   # Depth messages

STORAGE STATISTICS
  Total ClickHouse volume size: 4.1M
  btc/data.bin: 124.0K
  eth/data.bin: File not found yet
  sol/data.bin: File not found yet
```

## Performance Characteristics

### Storage Efficiency
- **File count**: Exactly 3 data.bin files in separate directories (btc/, eth/, sol/)
- **Growth rate**: ~200KB per symbol for 1000+ messages
- **Volume overhead**: <4MB base + data (vs. 100s of MB with traditional ClickHouse)
- **Target daily growth**: <500MB for entire ClickHouse volume
- **No parts/merging**: Zero ClickHouse complexity overhead

### Throughput Metrics
- **BTC_USDT**: ~10-15 messages/second typical
- **ETH_USDT**: ~8-12 messages/second typical  
- **SOL_USDT**: ~6-10 messages/second typical
- **Insert latency**: <5ms per message (direct append)
- **Disk I/O**: Minimal (pure append-only writes)

## Multi-Instance Deployment

### IP Tunneling Support (Required for sub.depth.full)
MEXC's `sub.depth.full` allows only one subscription per IP address. For multi-symbol deployment:

**Option 1: VPN/Proxy Rotation**
```bash
# Terminal 1 (Direct IP)
python3 btcdat.py

# Terminal 2 (VPN IP 1) 
python3 ethdat.py

# Terminal 3 (VPN IP 2)
python3 soldat.py
```

**Option 2: Cloud Deployment**
Deploy each client on separate cloud instances with different IP addresses.

**Option 3: Sequential Collection**
Run one symbol at a time if IP limitations prevent simultaneous collection.

## Troubleshooting Guide

### ClickHouse Connection Issues
```bash
# Check Docker status
docker ps

# Check ClickHouse logs
docker-compose logs clickhouse

# Verify port availability  
netstat -an | grep 9000

# Restart ClickHouse
docker-compose restart clickhouse
```

### WebSocket Connection Issues
```bash
# Test internet connectivity
ping 8.8.8.8

# Check MEXC API status
curl -I https://contract.mexc.com

# Monitor client error counts in statistics output
# If errors > 0, check network stability
```

### Data Not Appearing
```bash
# Verify tables exist
python3 verif.py

# Check table creation
docker-compose exec clickhouse clickhouse-client -q "SHOW TABLES FROM mexc_data"

# Monitor client subscription confirmations:
# Should see: "Subscribed to: sub.ticker for BTC_USDT"
```

### Storage Growing Too Fast
```bash
# Check system logs were disabled
python3 verif.py  # Should show <5MB total volume

# If volume >50MB, system logs may still be enabled
# Re-run: python3 ch_setup.py
```

## Maintenance Operations

### Clean Restart (Preserves Data)
```bash
docker-compose restart clickhouse
# No need to re-run ch_setup.py - tables persist
```

### Complete Reset (Deletes All Data)
```bash
docker-compose down --remove-orphans --volumes
docker-compose up -d
sleep 10
python3 ch_setup.py
```

### Backup Operations
```bash
# Export data (while ClickHouse running)
docker-compose exec clickhouse clickhouse-client -q "SELECT * FROM mexc_data.btc FORMAT TabSeparated" > btc_backup.tsv

# Restore data (after clean setup)
cat btc_backup.tsv | docker-compose exec -T clickhouse clickhouse-client -q "INSERT INTO mexc_data.btc FORMAT TabSeparated"
```

## Development and Customization

### Adding New Symbols
1. **Add configuration to config.py:**
```python
ADA_CONFIG = {
    "symbol": "ADA_USDT",
    "table_name": "ada",
    "subscriptions": [
        {"method": "sub.ticker", "param": {"symbol": "ADA_USDT"}},
        {"method": "sub.deal", "param": {"symbol": "ADA_USDT"}},
        {"method": "sub.depth.full", "param": {"symbol": "ADA_USDT", "limit": 20}}
    ]
}
```

2. **Update ch_setup.py to create ada table:**
```python
client.execute("""
CREATE TABLE ada (
    ts DateTime64(3),
    mt Enum8('t' = 1, 'd' = 2, 'dp' = 3, 'dl' = 4),
    m String
) ENGINE = StripeLog
""")
```

3. **Create adaaat.py client** (copy btcdat.py and change imports)

4. **Update verif.py** to include ada in verification

### Message Format Modifications
To change data extraction (e.g., add more ticker fields):

1. **Modify format_ticker_data() in all client files**
2. **Update README message format documentation**  
3. **Test with verif.py to ensure proper storage**

### Performance Tuning
```python
# config.py adjustments
PING_INTERVAL = 15          # WebSocket keep-alive (10-30s recommended)
STATS_INTERVAL = 15         # Statistics output frequency  
MAX_ERROR_COUNT = 100       # Shutdown threshold
```

## Technical Specifications

### System Requirements
- **RAM**: 512MB minimum, 1GB recommended
- **Storage**: 1GB minimum, 10GB+ for extended collection
- **CPU**: 1 core minimum, 2+ cores recommended for multi-symbol
- **Network**: Stable broadband, <100ms latency to MEXC

### Dependencies
```txt
websocket-client>=1.8.0     # MEXC WebSocket connection
clickhouse-driver>=0.2.9    # ClickHouse native client  
asyncio-pool>=0.6.0         # Async processing support
aiohttp>=3.9.5              # HTTP client library
python-dateutil>=2.9.0      # Date/time utilities
pandas>=2.3.1               # Data manipulation (for verif.py)
pyarrow>=20.0.0             # Columnar data support
```

### MEXC API Endpoints
- **WebSocket**: `wss://contract.mexc.com/edge`
- **Documentation**: https://mexcdevelop.github.io/apidocs/contract_v1_en/?python#websocket-api
- **Rate Limits**: ~1 subscription per IP for sub.depth.full
- **Message Types**: ticker (1-2/min), deal (10-30/sec), depth (5-10/sec)

## Security and Privacy

### Data Collection
- **No personal data**: Only public market data collected
- **No API keys**: Uses public WebSocket streams  
- **Local storage**: All data remains on your infrastructure
- **No external transmission**: Data never sent outside your environment

### Network Security
- **Read-only access**: No trading or account access
- **Public endpoints**: Only connects to public MEXC streams
- **No credentials**: No authentication required for market data

## License and Disclaimer

This software is provided for educational and research purposes. Users are responsible for:
- Compliance with MEXC Terms of Service
- Appropriate use of collected market data
- System security and maintenance
- Data backup and recovery procedures

**Investment Disclaimer**: This tool provides market data only. Not financial advice. Past performance does not guarantee future results.

---

## Quick Reference Commands

```bash
# Setup
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
docker-compose up -d && sleep 10 && python3 ch_setup.py

# Run clients  
python3 btcdat.py    # BTC data collection
python3 ethdat.py    # ETH data collection
python3 soldat.py    # SOL data collection

# Monitor
python3 verif.py     # Check data and storage

# Maintain
docker-compose restart clickhouse                        # Restart database
docker-compose down --remove-orphans --volumes          # Complete reset
```

**Result**: 3 continuously growing data.bin files in organized directories (btc/, eth/, sol/) with real-time cryptocurrency market data in minimal storage footprint.