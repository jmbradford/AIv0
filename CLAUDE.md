# Development Notes and Behaviors

## Project State (Production Ready)
- **Architecture**: Tor proxy-based IP separation for MEXC API compliance
- **Deployment**: 2-command automated deployment (`./setup && docker-compose up -d`)
- **File Naming**: Production-standardized naming conventions
- **Documentation**: Consolidated comprehensive README.md
- **Status**: Ready for production deployment

## Behaviors
- Verifying script and project functionality after any edits are made is paramount
- Asking the user clarifying questions for any ambiguous choices during development is encouraged
- Do not use emojis in code or documentation

## Current Architecture
- **IP Separation**: Tor proxy (not VPN) provides different IPs per container
- **Storage**: ClickHouse StripeLog tables for append-only continuous file growth
- **Containers**: 4 containers (ClickHouse + 3 clients) with automated orchestration
- **Verification**: Host-side verification via `./verify` script

## File Structure (Production)
```
mexc-pipeline/
├── README.md              # Comprehensive documentation
├── setup                  # Environment setup executable
├── verify                 # Data verification executable  
├── docker-compose.yml     # Container orchestration
├── Dockerfile            # Tor proxy container definition
├── clickhouse.xml        # Optimized ClickHouse config
├── config.py             # Symbol configurations
├── setup_database.py     # Database initialization
├── client_btc.py         # BTC WebSocket client
├── client_eth.py         # ETH WebSocket client
├── client_sol.py         # SOL WebSocket client
├── verify_data.py        # Data verification logic
├── requirements.txt      # Python dependencies
├── CLAUDE.md            # This file
└── venv/                # Python virtual environment
```

## Key Implementation Details
- **No VPN credentials required**: System uses Tor proxy for IP separation
- **Automatic deployment**: Complete system starts with single docker-compose command
- **Production naming**: All files use clear, production-appropriate names
- **Consolidated docs**: Single README.md replaces multiple markdown files
- **Verified functionality**: IP separation and data collection working as of latest tests

## Development Guidelines
- Always test verification after changes: `./verify`
- Use production file names in all references
- Maintain 2-command deployment simplicity
- Ensure IP separation verification shows unique IPs per container
- Keep documentation current with actual implementation (Tor proxy, not VPN)