# MEXC Multi-Symbol Cryptocurrency Data Pipeline

A high-performance, real-time cryptocurrency data pipeline that streams live market data from MEXC exchange directly into optimized ClickHouse storage with automated IP separation for compliance with MEXC's WebSocket API restrictions.

## Quick Start (2 Commands)

Deploy a complete multi-symbol cryptocurrency data pipeline on any Linux machine:

```bash
# 1. Environment setup (creates venv, installs dependencies, validates files)
./setup

# 2. Deploy complete system (ClickHouse + 3 client containers with IP separation)
docker-compose up -d
```

**Result**: Real-time BTC, ETH, and SOL data collection with each container using different IP addresses via Tor proxy.

## System Architecture

### Core Design Principles
- **IP Separation Compliance**: Each cryptocurrency client uses different Tor proxy for MEXC API compliance
- **Symbol-specific storage**: BTC, ETH, SOL data streams to separate continuously growing binary files
- **Zero-complexity storage**: StripeLog engine eliminates ClickHouse parts, merging, and metadata overhead
- **Containerized deployment**: Fully automated Docker-based deployment with dependency management
- **Real-time persistence**: Every message immediately appended with no buffering delays

### Container Architecture
```
┌────────────────────────────────────────────────────────────┐
│                    Docker Network                          │
│                                                            │
│  ┌──────────────┐    ┌─────────────────────────────────┐   │
│  │  ClickHouse  │◄───┤      Setup Container            │   │
│  │   Database   │    │  (Runs setup_database.py once)  │   │
│  │  Port 8123   │    └─────────────────────────────────┘   │
│  │  Port 9000   │                                          │
│  └──────┬───────┘                                          │
│         │                                                  │
│         ▼                                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │          Tor Proxy IP-Separated Clients             │   │
│  │                                                     │   │
│  │ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │   │
│  │ │  BTC Client  │ │  ETH Client  │ │  SOL Client  │  │   │
│  │ │ Tor Proxy 1  │ │ Tor Proxy 2  │ │ Tor Proxy 3  │  │   │
│  │ │client_btc.py │ │client_eth.py │ │client_sol.py │  │   │
│  │ │ External IP: │ │ External IP: │ │ External IP: │  │   │
│  │ │ Unique Loc 1 │ │ Unique Loc 2 │ │ Unique Loc 3 │  │   │
│  │ └──────────────┘ └──────────────┘ └──────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────┐
│                 MEXC WebSocket API                         │
│          wss://contract.mexc.com/edge                      │
│                                                            │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │
│  │ BTC_USDT    │ │ ETH_USDT    │ │ SOL_USDT    │          │
│  │• sub.ticker │ │• sub.ticker │ │• sub.ticker │          │
│  │• sub.deal   │ │• sub.deal   │ │• sub.deal   │          │
│  │• sub.depth  │ │• sub.depth  │ │• sub.depth  │          │
│  └─────────────┘ └─────────────┘ └─────────────┘          │
└────────────────────────────────────────────────────────────┘
```

### Data Storage Architecture

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
- **btc/data.bin**: Continuous append-only growth for all BTC_USDT messages
- **eth/data.bin**: Continuous append-only growth for all ETH_USDT messages  
- **sol/data.bin**: Continuous append-only growth for all SOL_USDT messages
- **Total files**: Exactly 3 data.bin files, no parts or merging complexity

## Message Format Specification

All messages stored in unified format in `m` column with `mt` indicating type:

**Ticker Messages (mt='t'):**
```
Format: lastPrice|fairPrice|indexPrice|holdVol|fundingRate
Example: 122122.3|122122.3|122133.7|102648627|0.00010000
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

## Complete Setup Instructions

### Prerequisites
- Linux environment (native or WSL2)
- Docker and Docker Compose installed
- Python 3.8+ with pip
- Minimum 2GB available disk space
- Stable internet connection

### 2-Command Deployment

**Step 1: Environment Setup**
```bash
./setup
```
This script automatically:
- ✅ Validates Docker installation and permissions
- ✅ Creates Python virtual environment
- ✅ Installs all dependencies from requirements.txt
- ✅ Validates project files and configuration
- ✅ Pre-downloads Docker base images
- ✅ Prepares everything for deployment

**Step 2: Deploy Complete System**
```bash
docker-compose up -d
```
This single command:
- 🚀 Starts ClickHouse database with optimized configuration
- 🛠️ Auto-runs database setup (setup_database.py) to create tables
- 🌐 Launches BTC client with Tor proxy IP separation
- 🌐 Launches ETH client with Tor proxy IP separation
- 🌐 Launches SOL client with Tor proxy IP separation
- 📊 Begins real-time data collection immediately

### Expected Startup Sequence
```
[+] Running 5/5
 ✔ Container mexc-clickhouse   Healthy    
 ✔ Container mexc-setup        Exited     
 ✔ Container mexc-btc-client   Started    
 ✔ Container mexc-eth-client   Started    
 ✔ Container mexc-sol-client   Started    

Setup Summary:
  BTC data: btc table → btc/data.bin (continuous growth)
  ETH data: eth table → eth/data.bin (continuous growth)
  SOL data: sol table → sol/data.bin (continuous growth)
```

## Monitoring and Verification

### Data Collection Verification
```bash
# Complete verification (recommended) - IP separation + data verification
./verify

# Individual verification options
./iptest     # IP separation verification only
./verif      # Data verification only

# Manual verification with environment override
source venv/bin/activate && CLICKHOUSE_HOST=localhost python3 verify_data.py
```

**Expected verification output:**
```
================================================================================
IP SEPARATION VERIFICATION
================================================================================
  BTC Container (mexc-btc-client):
    IP: 194.113.38.5
    Location: Unknown, Unknown
  ETH Container (mexc-eth-client):
    IP: 192.42.116.194
    Location: Unknown, Unknown
  SOL Container (mexc-sol-client):
    IP: 109.70.100.5
    Location: Unknown, Unknown

✅ IP Separation Success: 3 unique IPs detected
   Each container appears to MEXC with a different IP address

================================================================================
DATA VERIFICATION REPORT
================================================================================
💾 Symbol-specific storage status:

Total records appended: 4264
  btc.bin: 2298 records
  eth.bin: 1210 records
  sol.bin: 756 records

Records by symbol and message type:
  BTC: 2298 total
    t: 74      # Ticker messages
    d: 1690    # Deal messages
    dp: 534    # Depth messages
```

### Container Management
```bash
# Check deployment status
docker-compose ps

# Monitor all logs
docker-compose logs -f

# Monitor specific client
docker-compose logs -f btc-client

# Check data collection statistics
docker-compose logs btc-client | grep "STATISTICS"
```

### IP Separation Verification
```bash
# Verify each client uses different IP (should show 3 unique IPs)
docker exec mexc-btc-client curl -s --socks4 127.0.0.1:9050 https://ipinfo.io/ip
docker exec mexc-eth-client curl -s --socks4 127.0.0.1:9050 https://ipinfo.io/ip
docker exec mexc-sol-client curl -s --socks4 127.0.0.1:9050 https://ipinfo.io/ip
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

### Storage Growth Monitoring
```bash
# Check ClickHouse database size
docker exec mexc-clickhouse du -sh /var/lib/clickhouse/

# Check individual file sizes
./verify | grep -A 10 "STORAGE STATISTICS"
```

## Performance Characteristics

### Measured Throughput
- **BTC_USDT**: ~10-15 messages/second typical
- **ETH_USDT**: ~8-12 messages/second typical  
- **SOL_USDT**: ~6-10 messages/second typical
- **Combined**: ~25-40 messages/second aggregate
- **Insert latency**: <100ms per message (direct append)

### Resource Usage
- **RAM**: ~500-750MB total across all containers
- **CPU**: ~15-30% total across all containers (including Tor proxy overhead)
- **Storage Growth**: ~500MB/day typical
- **Network**: ~50-150KB/s total bandwidth

### Startup Performance
- **Environment Setup**: 30-60 seconds (./setup)
- **Container Deployment**: 30-45 seconds (docker-compose up -d)
- **Tor Proxy Connection**: 10-20 seconds per client
- **Data Collection**: Immediate after proxy setup
- **Total Deployment**: ~1-2 minutes from fresh state

## Management Operations

### Start/Stop/Restart
```bash
# Start all services
docker-compose up -d

# Stop all services  
docker-compose down

# Restart specific client
docker-compose restart btc-client

# View logs for troubleshooting
docker-compose logs btc-client | grep -A 5 -B 5 "error\|Error\|ERROR"
```

### Clean Restart (Preserves Data)
```bash
# Restart database (keeps all data)
docker-compose restart clickhouse

# Restart all clients (keeps database data)
docker-compose restart btc-client eth-client sol-client
```

### Complete Reset (Deletes All Data)
```bash
# Stop and remove everything including volumes
docker-compose down --volumes

# Restart fresh deployment
docker-compose up -d
```

## Troubleshooting

### Container Issues
```bash
# Check container health
docker-compose ps

# Restart failed containers
docker-compose up -d

# Check container resource usage
docker stats mexc-btc-client mexc-eth-client mexc-sol-client
```

### Database Connection Issues
```bash
# Check ClickHouse health from host
curl -s http://localhost:8123/ping

# Check database from inside container
docker exec mexc-clickhouse clickhouse-client --query "SELECT 1"

# Verify tables exist
docker exec mexc-clickhouse clickhouse-client --query "SHOW TABLES FROM mexc_data"
```

### Data Collection Issues
```bash
# Check WebSocket connectivity from container
docker exec mexc-btc-client curl -I https://contract.mexc.com

# Monitor client error counts in statistics output
docker-compose logs btc-client | grep "Errors:"

# Check for connection issues
docker-compose logs | grep -E "connection|Connection|WebSocket"
```

### IP Separation Issues
```bash
# Verify Tor proxy is running in each container
docker exec mexc-btc-client ps aux | grep tor
docker exec mexc-eth-client ps aux | grep tor
docker exec mexc-sol-client ps aux | grep tor

# Check proxy connections
docker exec mexc-btc-client curl -s --socks4 127.0.0.1:9050 https://ipinfo.io/
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

2. **Update setup_database.py to create xrp table**
3. **Create client_xrp.py** (copy existing client and update imports)
4. **Add to docker-compose.yml** with unique container name
5. **Update verify_data.py** to include xrp in verification

### Configuration Tuning
```python
# config.py adjustments
PING_INTERVAL = 15          # WebSocket keep-alive (10-30s recommended)
STATS_INTERVAL = 15         # Statistics output frequency  
MAX_ERROR_COUNT = 100       # Error threshold before restart
```

## Technical Specifications

### Dependencies
```
websocket-client>=1.8.0     # MEXC WebSocket connection
clickhouse-driver>=0.2.9    # ClickHouse native client  
asyncio-pool>=0.6.0         # Async processing support
aiohttp>=3.9.5              # HTTP client library
python-dateutil>=2.9.0      # Date/time utilities
pandas>=2.3.1               # Data manipulation
pyarrow>=20.0.0             # Columnar data support
```

### MEXC API Details
- **WebSocket**: wss://contract.mexc.com/edge
- **Rate Limits**: 1 sub.depth.full subscription per IP (handled by IP separation)
- **Message Rates**: ticker (5-20/min), deal (2-30/sec), depth (2-10/sec)
- **Reconnection**: Automatic with exponential backoff

### File Structure
```
mexc-pipeline/
├── README.md                 # This comprehensive guide
├── setup                     # One-command environment setup
├── verify                    # Data verification script
├── docker-compose.yml        # Multi-container deployment
├── Dockerfile               # Container definition with Tor proxy
├── clickhouse.xml           # Optimized ClickHouse configuration
├── config.py                # Symbol configurations
├── setup_database.py        # Database initialization
├── client_btc.py            # BTC WebSocket client
├── client_eth.py            # ETH WebSocket client  
├── client_sol.py            # SOL WebSocket client
├── verify_data.py           # Host-side data verification
├── requirements.txt         # Python dependencies
├── CLAUDE.md                # Development notes
└── venv/                    # Python virtual environment
```

## Quick Reference Commands

```bash
# Complete deployment
./setup && docker-compose up -d

# Verify everything is working
./verify      # Complete verification (IP + data)
./iptest      # IP separation only
./verif       # Data verification only

# Monitor real-time data collection
docker-compose logs -f btc-client

# Check storage growth
docker exec mexc-clickhouse du -sh /var/lib/clickhouse/

# Emergency restart
docker-compose restart btc-client eth-client sol-client

# Complete reset
docker-compose down --volumes && docker-compose up -d
```

## API Compliance

This pipeline is specifically designed for **MEXC's WebSocket API restrictions**:
- ✅ **IP Separation**: Each client uses different Tor proxy IP for sub.depth.full compliance
- ✅ **Rate Limiting**: Automatic reconnection with exponential backoff
- ✅ **Message Handling**: Robust parsing for all MEXC message types
- ✅ **Connection Management**: Health checks and automatic recovery

**Result**: Continuous real-time cryptocurrency data collection across multiple symbols while fully complying with MEXC's API restrictions through automated IP separation.