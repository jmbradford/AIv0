# MEXC Data Pipeline

Real-time cryptocurrency data pipeline that receives data per change from MEXC exchange, extracts all data and values, and saves them into an optimized ClickHouse database.

## Database Structure - Optimized Single-Table Design

The ClickHouse database uses a revolutionary single-table approach with 6 columns for maximum storage efficiency:

- **ts**: Timestamp (populated for every entry using MEXC timestamp)
- **ticker**: Multi-line string containing ticker field data (empty for non-ticker messages)
- **kline**: Multi-line string containing kline field data (empty for non-kline messages)
- **deal**: Multi-line string containing deal field data (empty for non-deal messages)
- **depth**: Multi-line string containing depth field data (empty for non-depth messages)
- **dl**: Dead letter column for error logging (empty for successful messages)

### Storage Optimization Benefits

- **20x Storage Reduction**: Eliminates NULL column waste from traditional wide-table designs
- **Superior Compression**: Multi-line string format achieves excellent compression ratios
- **Single Table Architecture**: No complex JOINs needed across message types
- **Efficient Field Storage**: Each message type uses only 2 columns (timestamp + relevant data column)

### Data Format

Each data column stores extracted fields in multi-line format using "field=value" syntax:

```
ticker column example:
symbol=ETH_USDT
lastPrice=2942.43
fairPrice=2942.43
indexPrice=2943.71
volume24=99478195
fundingRate=2.2e-05
...
```

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

- **Single Optimized Table**: `mexc_data` with multi-line string format design
- **Per-Change Processing**: Each MEXC message creates immediate database entry
- **Ultra-Efficient Storage**: Only 2 columns populated per message (timestamp + data type)
- **Multi-line Field Extraction**: All values parsed and stored as "field=value" strings
- **Error Handling**: Dead letter logging for failed processing
- **Real-time Monitoring**: Health checks and emergency buffering
- **Temporal Ordering**: Unified view across all message types via `message_sequence`

## Data Extraction

Each message type extracts specific fields from MEXC WebSocket streams into multi-line format:

### TICKER Messages (20+ fields):
- Financial data: `lastPrice`, `fairPrice`, `indexPrice`, `fundingRate`
- 24h statistics: `high24Price`, `lower24Price`, `volume24`, `amount24`
- Market depth: `bid1`, `ask1`, `maxBidPrice`, `minAskPrice`
- Rate arrays: `riseFallRates`, `riseFallRatesOfTimezone`

### KLINE Messages (13 fields):
- OHLCV data: `open`, `close`, `high`, `low`, `amount`, `quantity`
- Real values: `realOpen`, `realClose`, `realHigh`, `realLow`
- Metadata: `startTime`, `interval`, `symbol`

### DEAL Messages (8 fields):
- Trade execution: `price`, `volume`, `side`, `tradeTime`
- Order details: `tradeType`, `orderType`, `matchType`

### DEPTH Messages (Variable fields):
- Order book: `bestBidPrice`, `bestAskPrice`, `bidLevels`, `askLevels`
- Full depth: `bids` and `asks` arrays with price/quantity/level data
- Versioning: `version`, `begin`, `end`

## Files

- `client.py`: WebSocket client for real-time MEXC data collection
- `ch_setup.py`: Database schema and table creation
- `verif.py`: Data verification and validation tools
- `config.py`: Pipeline configuration settings
- `docker-compose.yml`: ClickHouse database container
- `requirements.txt`: Python dependencies