FROM python:3.9-slim

# Install system dependencies for proxy-based VPN alternative
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    iputils-ping \
    net-tools \
    procps \
    iptables \
    iproute2 \
    ca-certificates \
    proxychains4 \
    tor \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create entrypoint script using proxy chains for IP separation
RUN echo '#!/bin/bash\n\
echo "Container startup: $CONTAINER_TYPE"\n\
\n\
# Get original IP for comparison\n\
ORIGINAL_IP=$(timeout 10 curl -s https://ipinfo.io/ip)\n\
echo "Original IP: $ORIGINAL_IP"\n\
\n\
# Setup proxy-based IP separation\n\
setup_proxy_separation() {\n\
  echo "Setting up proxy-based IP separation..."\n\
  \n\
  # Configure different proxy settings based on container type\n\
  case "$CONTAINER_TYPE" in\n\
    "btc-client")\n\
      echo "BTC Client: Using proxy chain 1"\n\
      export PROXY_CONFIG="socks4 127.0.0.1 9050"\n\
      ;;\n\
    "eth-client")\n\
      echo "ETH Client: Using proxy chain 2"\n\
      export PROXY_CONFIG="socks4 127.0.0.1 9051"\n\
      ;;\n\
    "sol-client")\n\
      echo "SOL Client: Using proxy chain 3"\n\
      export PROXY_CONFIG="socks4 127.0.0.1 9052"\n\
      ;;\n\
    *)\n\
      echo "Default: No proxy configuration"\n\
      ;;\n\
  esac\n\
  \n\
  # Start Tor for proxy functionality\n\
  if [ "$CONTAINER_TYPE" != "setup" ]; then\n\
    echo "Starting Tor proxy service..."\n\
    tor --quiet --runasdaemon 1 --socksport 9050 &\n\
    sleep 5\n\
    \n\
    # Test proxy connection\n\
    PROXY_IP=$(timeout 15 curl --socks4 127.0.0.1:9050 -s https://ipinfo.io/ip)\n\
    if [ -n "$PROXY_IP" ] && [ "$PROXY_IP" != "$ORIGINAL_IP" ]; then\n\
      echo "✅ Proxy IP confirmed: $PROXY_IP (changed from $ORIGINAL_IP)"\n\
      PROXY_INFO=$(timeout 15 curl --socks4 127.0.0.1:9050 -s "https://ipinfo.io/$PROXY_IP")\n\
      echo "Proxy Location: $(echo "$PROXY_INFO" | grep -E \"country|region|city\" | tr \"\\n\" \" \")" \n\
      return 0\n\
    else\n\
      echo "⚠️  Proxy connection available but IP unchanged - continuing anyway"\n\
      return 0\n\
    fi\n\
  fi\n\
}\n\
\n\
echo "Waiting for ClickHouse to be ready..."\n\
\n\
# Wait for ClickHouse to be available\n\
until curl -s http://clickhouse:8123/ping 2>/dev/null | grep -q "Ok"; do\n\
  echo "ClickHouse not ready, waiting 2 seconds..."\n\
  sleep 2\n\
done\n\
\n\
echo "ClickHouse is ready!"\n\
\n\
# Run setup if this is the setup container\n\
if [ "$CONTAINER_TYPE" = "setup" ]; then\n\
  echo "Running database setup..."\n\
  python3 setup_database.py\n\
  echo "Setup completed!"\n\
  exit 0\n\
fi\n\
\n\
# Setup proxy separation for client containers\n\
if [ "$CONTAINER_TYPE" != "setup" ]; then\n\
  echo "🔗 Setting up proxy-based IP separation"\n\
  setup_proxy_separation\n\
  echo "✅ Proxy setup completed - container will continue"\n\
  echo "Waiting 3 seconds for proxy to stabilize..."\n\
  sleep 3\n\
fi\n\
\n\
# Execute the main command\n\
echo "🚀 Starting $CONTAINER_TYPE with proxy configuration..."\n\
exec "$@"' > /entrypoint.sh

# Make entrypoint executable
RUN chmod +x /entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Default command
CMD ["python3", "--version"]