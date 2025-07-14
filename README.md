# MEXC Multi-Symbol Cryptocurrency Data Pipeline

A high-performance, real-time cryptocurrency data pipeline that streams live market data from MEXC exchange directly into optimized ClickHouse storage. Designed for minimal storage overhead with continuous append-only file growth and multi-instance support for different cryptocurrencies.

## Architecture Overview

### Core Design Principles
- **Symbol-specific file growth**: Each cryptocurrency (BTC, ETH, SOL) writes to its own continuously growing binary file
- **Zero-complexity storage**: StripeLog engine eliminates ClickHouse parts, merging, and metadata overhead
- **Real-time persistence**: Every message immediately appended with no buffering delays
- **Multi-instance ready**: Run separate clients for different symbols/IP tunneling requirements

### Database Architecture

**Three StripeLog Tables (Append-Only):**
```sql
-- BTC data → /var/lib/clickhouse/store/b7c/...UUID.../data.bin
CREATE TABLE btc (
    ts DateTime64(3),                           -- MEXC message timestamp  
    mt Enum8('t'=1, 'd'=2, 'dp'=3, 'dl'=4),   -- Message type (1 byte)
    m String                                    -- Unified message data
) ENGINE = StripeLog;

-- ETH data → /var/lib/clickhouse/store/e74/...UUID.../data.bin  
CREATE TABLE eth (...same schema...);

-- SOL data → /var/lib/clickhouse/store/507/...UUID.../data.bin
CREATE TABLE sol (...same schema...);
```

**Storage Pattern:**
- **b7c/data.bin**: Continuous append-only growth for all BTC_USDT messages
- **e74/data.bin**: Continuous append-only growth for all ETH_USDT messages  
- **507/data.bin**: Continuous append-only growth for all SOL_USDT messages
- **Total files**: Exactly 3 data.bin files in identifier-based directories (b7c=BTC, e74=ETH, 507=SOL)

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
├── README.md                     # This comprehensive guide
├── DEPLOYMENT.md                 # 2-command deployment instructions
├── VPN-SETUP.md                  # VPN configuration and verification
├── requirements.txt              # Python dependencies
├── setup.sh                      # One-command environment setup script
├── docker-compose.yml            # Multi-container VPN deployment with ClickHouse
├── Dockerfile                    # VPN-enabled multi-service container
├── .env                          # VPN credentials (create from .env.example)
├── .env.example                  # VPN credentials template
├── config.py                     # Symbol configurations (BTC_CONFIG, ETH_CONFIG, SOL_CONFIG)
├── clickhouse-config-simple.xml  # Optimized ClickHouse settings for containers
├── ch_setup.py                   # Creates symbol-specific StripeLog tables with UUID control
├── btcdat.py                     # BTC_USDT WebSocket client → btc/data.bin (via US VPN)
├── ethdat.py                     # ETH_USDT WebSocket client → eth/data.bin (via UK VPN)
├── soldat.py                     # SOL_USDT WebSocket client → sol/data.bin (via CA VPN)
├── verif.py                      # Data verification and storage statistics
├── CLAUDE.md                     # Development notes and behaviors
└── venv/                         # Python virtual environment (created by setup.sh)
```

## Complete Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- NordVPN subscription (for VPN separation)
- Minimum 2GB available disk space
- Stable internet connection for WebSocket streams

### 2-Command VPN Deployment

### Step 1: Environment Setup
```bash
# Run automated setup script
./setup.sh
```
**This script automatically:**
- ✅ Validates Docker installation
- ✅ Creates Python virtual environment
- ✅ Installs all dependencies
- ✅ Downloads Docker base images
- ✅ Validates project files

### Step 2: VPN Configuration
```bash
# Configure your NordVPN credentials
nano .env
```
**Add your credentials:**
```env
NORDVPN_USER=your_nordvpn_username
NORDVPN_PASS=your_nordvpn_password
```

### Step 3: Deploy Multi-Container System
```bash
# Start complete system with VPN separation
docker-compose up -d
```
**This single command:**
- 🚀 Starts ClickHouse database
- 🛠️ Auto-runs database setup (ch_setup.py)
- 🌐 Launches BTC client with US VPN
- 🌐 Launches ETH client with UK VPN
- 🌐 Launches SOL client with Canadian VPN
- 📊 Begins real-time data collection with IP separation

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
python3 ./btcdat.py
```

**Option B: ETH_USDT Data Collection**  
```bash
python3 ./ethdat.py
```

**Option C: SOL_USDT Data Collection**
```bash
python3 ./soldat.py
```

**Option D: Multi-Symbol (Run in separate terminals/screens)**
```bash
# Terminal 1
python3 ./btcdat.py

# Terminal 2  
python3 ./ethdat.py

# Terminal 3
python3 ./soldat.py
```

### Step 4: VPN Verification and Monitoring
```bash
# Verify VPN IP separation (should show 3 different IPs)
docker-compose exec btc-client curl -s https://ipinfo.io/ip
docker-compose exec eth-client curl -s https://ipinfo.io/ip  
docker-compose exec sol-client curl -s https://ipinfo.io/ip

# Check data collection status
source venv/bin/activate && python3 verif.py

# Monitor container status
docker-compose ps
```

**Expected VPN verification output:**
```
# BTC Client (US VPN):
198.54.117.xxx

# ETH Client (UK VPN):  
185.202.220.xxx

# SOL Client (Canadian VPN):
199.19.224.xxx
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
Rate: 12.66 records/sec #varies
==================================================
```

### Storage Growth Verification
```bash
python3 ./verif.py
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
- **Volume overhead**: <4MB base + data (vs. 100s of MB with traditional ClickHouse)
- **No parts/merging**: Zero ClickHouse complexity overhead

### Throughput Metrics
- **BTC_USDT**: ~10-15 messages/second typical
- **ETH_USDT**: ~8-12 messages/second typical  
- **SOL_USDT**: ~6-10 messages/second typical
- **Insert latency**: <100ms per message (direct append)
- **Disk I/O**: Minimal (pure append-only writes)

## Multi-Instance VPN Deployment

### Automated VPN IP Separation (Required for sub.depth.full)
MEXC's `sub.depth.full` allows only one subscription per IP address. This project includes **automated VPN separation** using Docker containers with NordVPN integration.

**2-Command VPN Deployment:**
```bash
# Step 1: Environment setup
./setup.sh

# Step 2: Configure VPN credentials in .env
NORDVPN_USER=your_username
NORDVPN_PASS=your_password

# Step 3: Deploy with automatic VPN separation
docker-compose up -d
```

**Automatic VPN Routing:**
- **BTC Client**: Routes through US NordVPN servers
- **ETH Client**: Routes through UK NordVPN servers  
- **SOL Client**: Routes through Canadian NordVPN servers

**VPN Connection Verification:**
```bash
# Check each client's external IP (should be different)
docker-compose exec btc-client curl -s https://ipinfo.io/ip
docker-compose exec eth-client curl -s https://ipinfo.io/ip
docker-compose exec sol-client curl -s https://ipinfo.io/ip

# Expected output: 3 different IP addresses from US/UK/Canada
```

**Architecture:**
```
┌─────────────────────────────────────────────────┐
│                Docker Network                   │
│  ┌────────────┐    ┌─────────────────────────┐  │
│  │ ClickHouse │◄───┤      Setup Container    │  │
│  │ Database   │    └─────────────────────────┘  │
│  └─────┬──────┘                                 │
│        │                                        │
│        ▼                                        │
│  ┌─────────────────────────────────────────┐    │
│  │        VPN-Separated Clients            │    │
│  │ ┌──────────┐ ┌──────────┐ ┌──────────┐  │    │
│  │ │BTC Client│ │ETH Client│ │SOL Client│  │    │
│  │ │(US VPN)  │ │(UK VPN)  │ │(CA VPN)  │  │    │
│  │ │External  │ │External  │ │External  │  │    │
│  │ │IP: US    │ │IP: UK    │ │IP: CA    │  │    │
│  │ └──────────┘ └──────────┘ └──────────┘  │    │
│  └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
```

**Legacy Options (if VPN unavailable):**
- **Cloud Deployment**: Deploy each client on separate cloud instances
- **Sequential Collection**: Run one symbol at a time if IP limitations prevent simultaneous collection

## Troubleshooting Guide

### VPN Connection Issues
```bash
# Check VPN authentication status
docker-compose logs btc-client | grep -A 5 "VPN"

# Expected outputs:
# ✅ Success: "VPN connected successfully" + different external IPs
# ❌ Auth Failed: "VPN authentication failed - check NordVPN credentials" 
# ⚠️  No VPN: "No VPN credentials provided, running without VPN"

# Verify external IPs are different
docker-compose exec btc-client curl -s https://ipinfo.io/ip
docker-compose exec eth-client curl -s https://ipinfo.io/ip  
docker-compose exec sol-client curl -s https://ipinfo.io/ip

# Check VPN logs for detailed diagnostics
docker-compose exec btc-client cat /tmp/openvpn.log
```

**Common VPN Issues:**
- **AUTH_FAILED**: Invalid NordVPN credentials in .env file
- **Same IPs**: VPN connection failed, all clients using host IP
- **No VPN logs**: VPN setup skipped (check .env file exists with credentials)

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

## Development and Customization

### Adding New Symbols
1. **Add configuration to config.py:**
```python
XRP_CONFIG = {
    "symbol": "XRP_USDT",
    "table_name": "xrp",
    "subscriptions": [
        {"method": "sub.ticker", "param": {"symbol": "XRP_USDT"}},
        {"method": "sub.deal", "param": {"symbol": "XRP_USDT"}},
        {"method": "sub.depth.full", "param": {"symbol": "XRP_USDT", "limit": 20}}
    ]
}
```

2. **Update ch_setup.py to create xrp table:**
```python
client.execute("""
CREATE TABLE xrp (
    ts DateTime64(3),
    mt Enum8('t' = 1, 'd' = 2, 'dp' = 3, 'dl' = 4),
    m String
) ENGINE = StripeLog
""")
```

3. **Create xrpdat.py client** (copy btcdat.py and change imports)

4. **Update verif.py** to include xrp in verification

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
- **Message Types**: ticker (5-20/min), deal (2-30/sec), depth (2-10/sec)

## Quick Reference Commands

```bash
# 2-Command VPN Deployment
./setup.sh                    # Environment setup + dependency installation
docker-compose up -d          # Deploy with automatic VPN separation

# VPN Verification
docker-compose exec btc-client curl -s https://ipinfo.io/ip    # Check BTC VPN IP
docker-compose exec eth-client curl -s https://ipinfo.io/ip    # Check ETH VPN IP  
docker-compose exec sol-client curl -s https://ipinfo.io/ip    # Check SOL VPN IP

# Monitor
source venv/bin/activate && python3 verif.py              # Check data and storage
docker-compose ps                                         # Container status
docker-compose logs -f btc-client                         # VPN connection logs

# Maintain
docker-compose restart btc-client                         # Restart specific client
docker-compose down --volumes                             # Complete reset
```

**VPN Architecture Result**: 3 continuously growing data.bin files with real-time cryptocurrency data collected through separate VPN connections (US/UK/Canada) for MEXC API compliance.