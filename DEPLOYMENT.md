# 2-Command MEXC Multi-Symbol VPN Deployment

Deploy a complete multi-symbol cryptocurrency data pipeline with VPN separation on any fresh Linux machine with just 2 commands.

## Prerequisites

- Fresh Linux machine (Ubuntu/Debian/RHEL/etc.)
- Docker and Docker Compose installed
- NordVPN subscription

## Quick Installation

### Step 1: Environment Setup
```bash
./setup.sh
```

This script will:
- ✅ Validate Docker installation
- ✅ Create Python virtual environment
- ✅ Install all dependencies
- ✅ Validate project files
- ✅ Create configuration templates
- ✅ Pre-download Docker images
- ✅ Prepare everything for deployment

### Step 2: Configure VPN Credentials
```bash
nano .env
```

Edit the file with your NordVPN credentials:
```env
NORDVPN_USER=your_actual_username
NORDVPN_PASS=your_actual_password
```

### Step 3: Deploy Complete System
```bash
docker-compose up -d
```

This single command will:
- 🚀 Start ClickHouse database
- 🛠️ Auto-run database setup (ch_setup.py)
- 🌐 Launch BTC client with US VPN
- 🌐 Launch ETH client with UK VPN  
- 🌐 Launch SOL client with Canadian VPN
- 📊 Begin real-time data collection

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Docker Network                          │
│                                                            │
│  ┌──────────────┐    ┌─────────────────────────────────┐   │
│  │  ClickHouse  │◄───┤         Setup Container         │   │
│  │   Database   │    │     (Runs ch_setup.py once)     │   │
│  │              │    └─────────────────────────────────┘   │
│  └──────┬───────┘                                          │
│         │                                                  │
│         ▼                                                  │
│  ┌─────────────────────────────────────────────────────┐   |
│  │              Data Collection Clients                |   │
│  │                                                     |   │
│  │ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  |   │
│  │ │  BTC Client  │ │  ETH Client  │ │  SOL Client  │  |   │
│  │ │   (US VPN)   │ │   (UK VPN)   │ │  (CA VPN)    │  |   │
│  │ │ btcdat.py    │ │ ethdat.py    │ │ soldat.py    │  |   │
│  │ └──────────────┘ └──────────────┘ └──────────────┘  |   │
│  └─────────────────────────────────────────────────────┘   |
└────────────────────────────────────────────────────────────┘
```

## Data Storage

```
ClickHouse Volume:
/var/lib/clickhouse/store/
├── b7c/...UUID.../data.bin  ← BTC_USDT data 
├── e74/...UUID.../data.bin  ← ETH_USDT data
└── 507/...UUID.../data.bin  ← SOL_USDT data
```

## Monitoring

### Check Deployment Status
```bash
docker-compose ps
```

### Monitor Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f btc-client
docker-compose logs -f eth-client
docker-compose logs -f sol-client
```

### Verify VPN Connections
```bash
# Check each client's external IP (should be different from each other)
echo "BTC Client IP:" && docker-compose exec btc-client curl -s https://ipinfo.io/ip
echo "ETH Client IP:" && docker-compose exec eth-client curl -s https://ipinfo.io/ip  
echo "SOL Client IP:" && docker-compose exec sol-client curl -s https://ipinfo.io/ip

# Detailed VPN verification with country information
docker-compose exec btc-client curl -s https://ipinfo.io/ip | xargs curl -s https://ipinfo.io/ | grep -E "country|region"
docker-compose exec eth-client curl -s https://ipinfo.io/ip | xargs curl -s https://ipinfo.io/ | grep -E "country|region"
docker-compose exec sol-client curl -s https://ipinfo.io/ip | xargs curl -s https://ipinfo.io/ | grep -E "country|region"

# Expected output:
# BTC: "country": "US"
# ETH: "country": "GB" 
# SOL: "country": "CA"
```

### Verify Data Collection
```bash
source venv/bin/activate && python3 verif.py
```

## Management Commands

### Start/Stop
```bash
# Start all services
docker-compose up -d

# Stop all services  
docker-compose down

# Restart specific service
docker-compose restart btc-client
```

### Complete Reset
```bash
# Stop and remove everything including data
docker-compose down --volumes

# Restart fresh
docker-compose up -d
```

## Expected Performance

- **Startup Time**: ~2-3 minutes for complete deployment
- **VPN Connection**: ~30-60 seconds per client
- **Data Collection**: Begins immediately after VPN connection

## Troubleshooting

### VPN Issues
```bash
# Check VPN connection status for all clients
docker-compose logs btc-client | grep -A 5 -B 2 "VPN"
docker-compose logs eth-client | grep -A 5 -B 2 "VPN"  
docker-compose logs sol-client | grep -A 5 -B 2 "VPN"

# Expected successful output:
# ✅ VPN connected successfully
# External IP: [different IP for each client]

# Common failure outputs:
# ❌ VPN authentication failed - check NordVPN credentials
# ⚠️ No VPN credentials provided, running without VPN

# Check detailed OpenVPN logs for authentication debugging
docker-compose exec btc-client cat /tmp/openvpn.log
docker-compose exec eth-client cat /tmp/openvpn.log
docker-compose exec sol-client cat /tmp/openvpn.log

# Verify VPN process status
docker-compose exec btc-client ps aux | grep openvpn
```

**VPN Authentication Troubleshooting:**
- **AUTH_FAILED**: Update credentials in .env file with valid NordVPN account
- **Same IP addresses**: VPN connections failed, restart containers after fixing credentials
- **No VPN logs**: Check .env file exists and contains NORDVPN_USER and NORDVPN_PASS

### Database Issues
```bash
# Check ClickHouse health
docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Check tables
docker-compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM mexc_data"
```

### Data Collection Issues
```bash
# Check WebSocket connections
docker-compose logs btc-client | grep -i websocket

# Verify MEXC connectivity
docker-compose exec btc-client curl -I https://contract.mexc.com
```

## File Structure

```
mexc-pipeline/
├── setup.sh                    # One-command environment setup
├── docker-compose.yml          # Main deployment configuration
├── Dockerfile                  # Multi-service container definition
├── .env                        # VPN credentials (you create this)
├── clickhouse-config-simple.xml # Optimized ClickHouse config
├── config.py                   # Symbol configurations
├── ch_setup.py                 # Database initialization
├── btcdat.py                   # BTC WebSocket client
├── ethdat.py                   # ETH WebSocket client  
├── soldat.py                   # SOL WebSocket client
├── verif.py                    # Data verification tool
├── requirements.txt            # Python dependencies
└── venv/                       # Python virtual environment (created by setup.sh)
```

---

**🎯 Result**: Complete multi-symbol cryptocurrency data pipeline with VPN separation deployed with just 2 commands on any Linux machine!