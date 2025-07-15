# Pipeline

## Definition
A production-ready, high-performance cryptocurrency data pipeline. Connects with automated IP verification and separation per asset to MEXC's WebSocket API. Streams market data in real time into an optimized ClickHouse database using StripeLog. Automates hourly exportation into .parquet files using .bin rotations and enhanced buffer memory with extensively verified zero data loss. Includes comprehensive export logging and individual message storage verification. Used on Windows 11 with WSL2 Ubuntu and Docker Desktop.

## Prerequisites
  Linux Ubuntu (WSL2 used, native compatible)
  Docker and Docker Compose
  Python with pip
  Appropriate CPU, RAM, storage, and network bandwidth

## Production-Ready Capability
Orchestrates a multi-container Docker setup. Creates and formats a Clickhouse database with 3 tables. Creating 3 client containers with distinct, separate IPs. Streams all market data in real-time to said Clickhouse database. Performs automated hourly exportation into .parquet files.

## Files
pipe/

### Docs
├── README.md # This comprehensive guide
├── CLAUDE.md # Developer documentation and architecture notes
└── WIP.md  # Project status, issues, and relevant information for proceeding with development

### Main
├── requirements.txt  # Python dependencies
├── docker-compose.yml  # Multi-container orchestration
├── setup # Environment setup and validation:
      Validates Docker installation and permissions
      Creates Python virtual environment with proper isolation
      Installs all dependencies from requirements.txt
      Validates all project files and configurations
      Handles WSL2 Ubuntu externally managed Python environments
├── Dockerfile  # Container definition with Tor proxy setup
├── clickhouse.xml  # ClickHouse configuration
├── config.py # Symbol configurations and settings
├── setup_database.py # Database initialization and 3 table creation
├── exporter.py # Table rotation and .parquet export service
├── verif-ip.py # IP separation verification (replaces bash iptest)
├── verif-ch.py # Clickhouse data integrity verification
├── verif-parq.py # Enhanced .parquet integrity verification with message samples
├── client-btc.py # BTC WebSocket client
├── client-eth.py # ETH WebSocket client
├── client-sol.py # SOL WebSocket client
└── venv/ # Python virtual environment

### Output
└── exports/  # Hourly Parquet export files
    ├── btcYYYYMMDDHH.parquet
    ├── ethYYYYMMDDHH.parquet
    └── solYYYYMMDDHH.parquet

## Architecture, Schema, Notes

### Data Storage

#### ClickHouse
3 tables: BTC, ETH, and SOL. Each table contains 3 columns: ts, mt, and m
ts (timestamp) is in DateTime64(3) format, with a value extracted from each MEXC message to the highest precision available
mt (messageType) is in String format, determined by message type: ticker='t', deal='d', depth='dp', dead_letter='dl'
m (message) is in String format containing extracted data from each message
StripeLog engine is used for append-only logic.
Each client has its own directory and single .bin in which the table grows.

#### Message Format
**Ticker Messages (mt='t')**:
Format: lastPrice|fairPrice|indexPrice|fundingRate|holdVol
Example: 122122.3|122122.3|122133.7|0.00010000|102648627

**Deal Messages (mt='d')**:
Format: price|volume|direction (1=buy, 2=sell)
Example: 122115.1|12|1

**Depth Messages (mt='dp')**:
Format: bids20|asks20
Example: [122125.8,103673],[122125.7,100518]...|[122125.9,110108],[122126,113191]...

**Deadletter (mt='dl')**:
Format: raw_message_data
Purpose: Captures unknown/malformed/errored messages for debugging
### Data Flow
**Real-time Collection Flow**: WebSocket → message processing → ClickHouse StripeLog → continuous growth of 3 .bin files
**Hourly Export Flow**: Table rotation (documented below) → export `previous` to .parquet files → verify .parquet files → remove `previous` and respective directories from Clickhouse
  **Table Rotation System**:
    1. Buffer Activation: Clients activate memory buffers
    2. Rotation: `current` → `previous`, create new `current`
    3. Buffer Deposit: Clients deposit buffered messages to `current` with batch insertion and validation
    4. Buffer Verification: Integrity checks ensure zero data loss during rotation

### Docker Container/Network Architecture

#### setup-ch Container
  Creates clickhouse Container
  Formats Clickhouse database (after waiting for it to initialize)
  Creates client-btc, client-eth, client-sol containers with separate IPs
  Creates exporter container
  Creates /exports/export_log.txt and verifies writeablity
  Runs once then stops

#### clickhouse Container
  Self-explanatory.
  Linked to ch-mexc volume.
  Does not collect internal metadata.

#### client-btc Container
  client_btc image from client-btc.py
  Tor proxy 0, unique IP 0

#### client-eth Container
  client_eth image from client-eth.py
  Tor proxy 1, unique IP 1

#### client-sol Container
  client_sol image from client-sol.py
  Tor proxy 2, unique IP 2

#### exporter Container
  exporter image from exporter.py
  Coordinates memory buffer and .bin rotation with client containers
  Exports previous hour of data in 3 .bin files to 3 .parquet files, verifies their integrity, and removes old data and directories
  Outputs to /pipe/exports with format: {symbol}YYYYMMDDHH.parquet
  Adds log to /pipe/exports/export_log.txt

### Data Export System
Automated Hourly Exports:
  Schedule: Runs 1 minute after each hour (12:01 for 11:00-12:00 data)
  Format: Compressed .parquet files with preserved schema
  Location: `./exports/` directory
  Naming: `{symbol}YYYYMMDDHH.parquet`
  Verification: Automatic integrity checks

## Reference

### Commands/Usage
```bash
# Create virtual environment, install dependencies
./setup

# Start all services and containers
docker-compose up -d

# Stop all services
docker-compose down

# Stop all and wipe collected data
docker-compose down --remove-orphans --volumes

# IP separation verification of all 3 client containers
python3 ./verif-ip.py

# Clickhouse table query verification for all three assets
python3 ./verif-ch.py

# Force one time export (independent mode)
docker exec exporter python3 exporter.py --once

# Enhanced Parquet file integrity check with message samples
python3 ./verif-parq.py

# Review export history and metadata
cat ./exports/export-log.txt

# Advanced buffer verification (zero message loss)
docker exec exporter python3 /app/buffer-verification.py

# Individual message storage verification
docker exec exporter python3 /app/buffer-individual-message-test.py
```

### Directories
`./venv/` - Python virtual environment
`./exports/` - Hourly Parquet export files and export-log.txt
`/var/lib/docker/volumes/` - ClickHouse data persistence

### URLs
MEXC WebSocket: wss://contract.mexc.com/edge

## Final Verification
Provided:
  WSL2 Ubuntu is installed and running (on Windows)
  Docker Desktop is installed and running
  GitHub is authenticated
  sudo is passwordless
  Enough CPU, RAM, and storage is available

This command:
```bash
git clone https://github.com/jmbradford/AIv0 pipe && cd pipe && ./setup && source venv/bin/activate && docker-compose up -d && sleep 60 && python3 ./verif-ip.py && python3 ./verif-ch.py && docker exec exporter python3 exporter.py --once && python3 ./verif-parq.py
```

Should:
  Clone the pipeline repo
  Setup the environment and install dependencies
  Initialize Clickhouse and create and properly format 3 tables
  Create 3 client containers with separate IPs, then connect them to MEXC for real-time streaming of 3 assets
  Print verification of separate IPs and Clickhouse ingestion for all assets
  Force a manual export of data and rotation of clients
  Print verification of each of the most recent .parquet files
All without error, and from a minimally setup environment.