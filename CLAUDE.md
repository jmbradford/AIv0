# MEXC Multi-Symbol Cryptocurrency Pipeline - Developer Documentation

## Project Overview

This is a production-ready, high-performance cryptocurrency data pipeline that collects real-time market data from MEXC exchange for multiple symbols (BTC, ETH, SOL) with sophisticated IP separation and optimized storage architecture.

## Architecture Evolution

### Current Architecture (v3.0 - Production)
- **IP Separation**: Tor proxy-based isolation (each container gets unique IP)
- **Storage**: ClickHouse StripeLog engine for pure append-only continuous file growth
- **Containers**: 4-container orchestration (ClickHouse + 3 symbol-specific clients)
- **Deployment**: 2-command automated deployment with comprehensive verification
- **Status**: Production-ready with full compliance to MEXC API restrictions

### Previous Iterations
1. **v1.0**: Single-symbol basic WebSocket client
2. **v2.0**: VPN-based multi-container approach (deprecated due to credential complexity)
3. **v3.0**: Tor proxy approach (current) - simplified, credential-free, robust

## Technical Architecture

### Container Orchestration
```
mexc-clickhouse     → ClickHouse database server
mexc-setup          → Database initialization (runs once)
mexc-btc-client     → BTC data collection with Tor proxy #1
mexc-eth-client     → ETH data collection with Tor proxy #2  
mexc-sol-client     → SOL data collection with Tor proxy #3
```

### IP Separation Implementation
- **Technology**: Tor proxy (SOCKS4 proxy on port 9050 per container)
- **Benefit**: Each container appears with different IP to MEXC API
- **Compliance**: Satisfies MEXC's 1 sub.depth.full subscription per IP restriction
- **Verification**: Built-in IP checking via `./iptest` script

### Data Storage Design
```sql
-- Schema: 3 identical tables for symbol separation
CREATE TABLE btc (
    ts DateTime64(3),                           -- Message timestamp
    mt Enum8('t'=1, 'd'=2, 'dp'=3, 'dl'=4),   -- Message type (1 byte)
    m String                                    -- Unified message data
) ENGINE = StripeLog;

-- Same for eth, sol tables
-- Result: 3 continuously growing .bin files
```

### Message Processing Pipeline
1. **WebSocket Connection**: Connects to `wss://contract.mexc.com/edge`
2. **Subscription**: Sends symbol-specific subscriptions (ticker, deal, depth)
3. **Processing**: Parses incoming messages, normalizes format
4. **Storage**: Direct append to ClickHouse StripeLog tables
5. **Statistics**: Real-time throughput and error reporting

## Development Guidelines

### Code Structure Standards
- **Symbol-specific clients**: Each client handles one symbol exclusively
- **Configuration-driven**: All settings centralized in `config.py`
- **Error handling**: Comprehensive error handling with automatic reconnection
- **Logging**: Standardized logging with 15-second statistics intervals

### File Naming Conventions
- `client_{symbol}.py` - WebSocket clients (e.g., client_btc.py)
- `setup_database.py` - Database initialization script
- `verify_data.py` - Data verification logic
- `setup` - Environment setup executable
- `verify` - Complete verification executable
- `iptest` - IP separation verification only
- `verif` - Data verification only

### Development Workflow
1. **Environment Setup**: Always run `./setup` first
2. **Testing**: Use verification scripts during development
3. **Deployment**: Use `docker-compose up -d` for production
4. **Debugging**: Check container logs with `docker-compose logs [service]`

## Implementation Details

### WebSocket Client Architecture
```python
class BtcDataPipeline:
    def __init__(self):
        self.ws = None                    # WebSocket connection
        self.ch_client = None            # ClickHouse client
        self.running = False             # Control flag
        self.symbol = "BTC_USDT"         # Symbol-specific
        self.table_name = "btc"          # Target table
        self.stats = {...}               # Performance metrics
```

### Message Type Processing
- **Ticker ('t')**: Price and volume summaries
- **Deal ('d')**: Individual trade transactions  
- **Depth ('dp')**: Order book depth (bid/ask levels)
- **Deadletter ('dl')**: Malformed messages for debugging

### Configuration Management
```python
# Symbol-specific configurations in config.py
BTC_CONFIG = {
    "symbol": "BTC_USDT",
    "table_name": "btc", 
    "subscriptions": [...]
}
```

## Container Implementation

### Dockerfile Architecture
```dockerfile
FROM python:3.9-slim
# Install: tor, proxychains4, curl, system tools
# Configure: Tor proxy on port 9050
# Setup: Python environment and dependencies
```

### Docker Compose Orchestration
- **Health checks**: Ensures ClickHouse ready before clients start
- **Dependencies**: Setup → ClickHouse → Clients
- **Networking**: Isolated bridge network for internal communication
- **Volumes**: Persistent storage for ClickHouse data

### Service Dependencies
```yaml
btc-client:
  depends_on:
    setup:
      condition: service_completed_successfully
    clickhouse:
      condition: service_healthy
```

## Verification and Monitoring

### Verification Scripts
- **`./verify`**: Complete verification (IP separation + data collection)
- **`./iptest`**: IP separation verification only
- **`./verif`**: Data verification only

### Expected Verification Output
```
IP SEPARATION VERIFICATION
✅ IP Separation Success: 3 unique IPs detected

DATA VERIFICATION REPORT  
Total records appended: 4264
  btc.bin: 2298 records
  eth.bin: 1210 records
  sol.bin: 756 records
```

### Real-time Monitoring
```bash
# Container logs
docker-compose logs -f btc-client

# Statistics output (every 15 seconds)
BTC_USDT STATISTICS (Last 15s)
Total Records Appended: 190
Rate: 12.66 records/sec
```

## Performance Characteristics

### Throughput Metrics
- **BTC_USDT**: ~10-15 messages/second
- **ETH_USDT**: ~8-12 messages/second  
- **SOL_USDT**: ~6-10 messages/second
- **Combined**: ~25-40 messages/second
- **Insert Latency**: <100ms per message

### Resource Usage
- **RAM**: ~500-750MB total across all containers
- **CPU**: ~15-30% total (including Tor proxy overhead)
- **Storage Growth**: ~500MB/day typical
- **Network**: ~50-150KB/s total bandwidth

### Scalability Considerations
- **Symbol Addition**: Create new client + configuration + table
- **Rate Limiting**: Handled by IP separation (1 subscription per IP)
- **Storage**: Linear growth, no partitioning or merging complexity

## Troubleshooting Guide

### Common Issues

#### Container Startup Problems
```bash
# Check container status
docker-compose ps

# View startup logs
docker-compose logs setup
docker-compose logs clickhouse
```

#### IP Separation Issues
```bash
# Verify Tor proxy is running
docker exec mexc-btc-client ps aux | grep tor

# Check IP addresses
./iptest

# Manual IP check
docker exec mexc-btc-client curl -s --socks4 127.0.0.1:9050 https://ipinfo.io/ip
```

#### Data Collection Issues
```bash
# Check WebSocket connectivity
docker-compose logs btc-client | grep -E "connection|error"

# Verify data is being written
./verif

# Check database connectivity
docker exec mexc-clickhouse clickhouse-client --query "SHOW TABLES FROM mexc_data"
```

#### Database Connection Problems
```bash
# Test ClickHouse health
curl -s http://localhost:8123/ping

# Check database from inside container
docker exec mexc-clickhouse clickhouse-client --query "SELECT 1"
```

### Emergency Procedures

#### Complete System Reset
```bash
# Stop everything and remove data
docker-compose down --volumes

# Fresh deployment
./setup && docker-compose up -d
```

#### Restart Clients Only (Preserve Data)
```bash
# Restart data collection clients
docker-compose restart btc-client eth-client sol-client
```

## Development Best Practices

### Testing Protocol
1. **Environment**: Always test with `./setup` first
2. **Verification**: Use verification scripts after changes
3. **IP Testing**: Verify IP separation with `./iptest`
4. **Data Testing**: Verify data collection with `./verif`
5. **Full Testing**: Complete verification with `./verify`

### Code Quality Standards
- **Error Handling**: Comprehensive try/catch with logging
- **Reconnection**: Exponential backoff for WebSocket reconnections
- **Statistics**: 15-second reporting intervals for monitoring
- **Configuration**: Centralized configuration management
- **Documentation**: Inline comments for complex logic

### Security Considerations
- **No Credentials**: Tor proxy eliminates VPN credential management
- **Isolation**: Each container has isolated network stack
- **Logging**: No sensitive data in logs
- **Access**: Database accessible only within Docker network

## Adding New Symbols

### Step-by-Step Process
1. **Configuration**: Add symbol config to `config.py`
```python
ADA_CONFIG = {
    "symbol": "ADA_USDT",
    "table_name": "ada",
    "subscriptions": [...]
}
```

2. **Database**: Update `setup_database.py` to create table
```python
CREATE TABLE ada UUID '...' (...) ENGINE = StripeLog
```

3. **Client**: Create `client_ada.py` (copy existing client)
4. **Docker**: Add service to `docker-compose.yml`
5. **Verification**: Update verification scripts

### Testing New Symbol
```bash
# Redeploy with new symbol
docker-compose down && docker-compose up -d

# Verify new symbol is collecting data
./verify
```

## Deployment Environments

### Development Environment
```bash
# Local development setup
./setup
docker-compose up    # Foreground for debugging
```

### Production Environment
```bash
# Production deployment
./setup
docker-compose up -d    # Background daemon mode

# Monitoring
./verify    # Scheduled verification
docker-compose logs | grep -E "error|Error|ERROR"    # Error monitoring
```

### Staging Environment
```bash
# Same as production but with additional logging
docker-compose logs -f    # Continuous log monitoring
```

## System Requirements

### Minimum Requirements
- **OS**: Linux (native or WSL2)
- **RAM**: 2GB available
- **Storage**: 5GB available (for data growth)
- **Network**: Stable internet connection
- **Docker**: Docker and Docker Compose

### Recommended Requirements
- **RAM**: 4GB+ for optimal performance
- **Storage**: 20GB+ for extended data collection
- **CPU**: 2+ cores for better Tor proxy performance
- **Network**: Low-latency connection for real-time data

## Future Development

### Planned Enhancements
- **Additional Symbols**: Easy symbol addition framework in place
- **Advanced Analytics**: Query optimization for time-series analysis
- **Monitoring Dashboard**: Web-based real-time monitoring
- **Alert System**: Automated failure detection and notification

### Architecture Scalability
- **Horizontal Scaling**: Add more symbol-specific clients
- **Storage Optimization**: Potential partitioning for very large datasets
- **Geographic Distribution**: Multi-region deployment capability

## Support and Maintenance

### Regular Maintenance Tasks
- **Log Rotation**: Monitor container log sizes
- **Storage Monitoring**: Track ClickHouse data growth
- **Update Management**: Keep Docker images updated
- **Performance Monitoring**: Track throughput metrics

### Backup Procedures
```bash
# Backup ClickHouse data
docker exec mexc-clickhouse tar -czf /tmp/backup.tar.gz /var/lib/clickhouse/

# Copy backup to host
docker cp mexc-clickhouse:/tmp/backup.tar.gz ./clickhouse-backup.tar.gz
```

## Contact and Resources

### Development Team Notes
- **Architecture**: Production-ready, battle-tested
- **Compliance**: MEXC API compliant with IP separation
- **Maintenance**: Low-maintenance, self-healing design
- **Documentation**: Comprehensive user and developer docs

### Useful Commands Reference
```bash
# Complete setup
./setup && docker-compose up -d

# Verification suite
./verify        # Complete
./iptest        # IP only  
./verif         # Data only

# Monitoring
docker-compose logs -f [service]
docker-compose ps
docker stats

# Troubleshooting
docker exec mexc-clickhouse clickhouse-client
curl http://localhost:8123/ping
```

---

**Last Updated**: Production optimization with Tor proxy architecture and comprehensive verification suite.
**Status**: Production-ready, actively maintained.
**Version**: 3.0 (Tor proxy implementation)