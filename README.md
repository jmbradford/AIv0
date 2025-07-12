# MEXC Data Pipeline

Real-time cryptocurrency data pipeline that receives data per change from MEXC exchange, extracts all data and values, and saves them into a ClickHouse database.

## Database Structure

The ClickHouse database contains one table with 6 columns:
- **ts**: Timestamp (populated for every entry using MEXC timestamp)
- **ticker**: Ticker data (gaps between entries as other messages are processed)
- **kline**: Kline data (gaps between entries as other messages are processed)  
- **deal**: Deal data (gaps between entries as other messages are processed)
- **depth**: Depth data (gaps between entries as other messages are processed)
- **dl**: Dead letter column for logging errors

The four data columns (ticker, kline, deal, depth) only exist in the same entry if their timestamps are exactly the same. Each column contains structured JSON with extracted values rather than raw messages.

## Project Initialization

### Dependencies (if not present)
```bash
pip install -r requirements.txt
```

### 1. Start ClickHouse
```bash
docker-compose up -d
```

### 2. Setup Database Schema
```bash
python ./ch_setup.py
```

### 3. Start Data Collection
```bash
python ./client.py
```

### 4. Verify Data (Optional)
```bash
python ./verif.py
```

### 5. Stop Everything
```bash
docker-compose down --remove-orphans --volumes
```

## Architecture

- **Single Wide Table**: `mexc_messages` with temporal sequencing design
- **Per-Change Processing**: Each MEXC message creates immediate database entry
- **Sparse Data Storage**: Only relevant column populated per message type
- **Structured Extraction**: All values parsed and stored as structured JSON
- **Error Handling**: Dead letter logging for failed processing
- **Real-time Monitoring**: Health checks and emergency buffering

## Data Extraction

Each message type extracts specific fields from MEXC WebSocket streams:

- **ticker**: `lastPrice` (lp), `change` (c), `riseFallRate` (r), `high24h` (h), `low24h` (l), `volume24h` (v), `amount24h` (a), `fairPrice` (fp), `indexPrice` (ip), `fundingRate` (fr), best bid/ask prices and sizes
- **kline**: `open`, `close`, `high`, `low`, `amount`, `quantity`, `startTime`, `interval`, real OHLC values (`realOpen`, `realClose`, `realHigh`, `realLow`)
- **deal**: `price` (p), `volume` (v), `side` (buy/sell from T), `tradeType` (T), `orderType` (O), `matchType` (M), `tradeTime` (t)
- **depth**: `bestBidPrice`, `bestAskPrice`, `bestBidQty`, `bestAskQty`, full `asks`/`bids` arrays, `bidLevels`, `askLevels`, `version`
- **dl**: Error information with structured error details, original data, and error timestamps

## Files

- `client.py`: WebSocket client for real-time MEXC data collection
- `ch_setup.py`: Database schema and table creation
- `verif.py`: Data verification and validation tools
- `config.py`: Pipeline configuration settings
- `docker-compose.yml`: ClickHouse database container
- `requirements.txt`: Python dependencies