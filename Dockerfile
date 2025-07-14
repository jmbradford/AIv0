FROM python:3.9-slim

# Install system dependencies for VPN and networking
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    iputils-ping \
    net-tools \
    procps \
    openvpn \
    iptables \
    iproute2 \
    ca-certificates \
    unzip \
    expect \
    && rm -rf /var/lib/apt/lists/*

# Create directories for VPN configs
RUN mkdir -p /etc/openvpn /app/vpn-configs

# Download NordVPN OpenVPN configs (more reliable than CLI in containers)
RUN cd /app/vpn-configs && \
    wget -q https://downloads.nordcdn.com/configs/archives/servers/ovpn.zip && \
    unzip -q ovpn.zip && \
    rm ovpn.zip && \
    find . -name "*.ovpn" -type f | head -20 | xargs ls -la

# Set working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create entrypoint script for delayed startup and dependency management
RUN echo '#!/bin/bash\n\
echo "Container startup: $CONTAINER_TYPE"\n\
\n\
# Setup VPN connection for client containers using OpenVPN\n\
setup_vpn() {\n\
  if [ -n "$NORDVPN_USER" ] && [ -n "$NORDVPN_PASS" ]; then\n\
    echo "Setting up NordVPN OpenVPN connection..."\n\
    \n\
    # Create auth file\n\
    echo "$NORDVPN_USER" > /tmp/nordvpn-auth.txt\n\
    echo "$NORDVPN_PASS" >> /tmp/nordvpn-auth.txt\n\
    chmod 600 /tmp/nordvpn-auth.txt\n\
    \n\
    # Find appropriate config file based on country\n\
    CONFIG_FILE=""\n\
    case "$NORDVPN_COUNTRY" in\n\
      "United_States"|"US")\n\
        CONFIG_FILE=$(find /app/vpn-configs -name "*us*.ovpn" | head -1)\n\
        ;;\n\
      "United_Kingdom"|"UK")\n\
        CONFIG_FILE=$(find /app/vpn-configs -name "*uk*.ovpn" | head -1)\n\
        ;;\n\
      "Canada"|"CA")\n\
        CONFIG_FILE=$(find /app/vpn-configs -name "*ca*.ovpn" | head -1)\n\
        ;;\n\
      *)\n\
        CONFIG_FILE=$(find /app/vpn-configs -name "*.ovpn" | head -1)\n\
        ;;\n\
    esac\n\
    \n\
    if [ -n "$CONFIG_FILE" ] && [ -f "$CONFIG_FILE" ]; then\n\
      echo "Using config: $CONFIG_FILE"\n\
      echo "Connecting to VPN..."\n\
      \n\
      # Start OpenVPN in background\n\
      openvpn --config "$CONFIG_FILE" --auth-user-pass /tmp/nordvpn-auth.txt --daemon --log /tmp/openvpn.log\n\
      \n\
      # Wait for connection and check logs\n\
      echo "Waiting for VPN connection..."\n\
      sleep 10\n\
      \n\
      # Check for authentication success in logs\n\
      if grep -q "AUTH_FAILED" /tmp/openvpn.log; then\n\
        echo "❌ VPN authentication failed - check NordVPN credentials"\n\
        echo "Log details:"\n\
        grep -A 2 -B 2 "AUTH_FAILED" /tmp/openvpn.log\n\
        echo "Continuing without VPN connection..."\n\
      elif grep -q "Initialization Sequence Completed" /tmp/openvpn.log; then\n\
        echo "✅ VPN connected successfully"\n\
        # Check external IP\n\
        echo "Checking external IP..."\n\
        NEW_IP=$(timeout 10 curl -s https://ipinfo.io/ip)\n\
        if [ -n "$NEW_IP" ]; then\n\
          echo "External IP: $NEW_IP"\n\
          curl -s "https://ipinfo.io/$NEW_IP" | grep -E "country|region|city" || true\n\
        fi\n\
      elif pgrep openvpn > /dev/null; then\n\
        echo "🔄 VPN process running, waiting for connection..."\n\
        sleep 10\n\
        if grep -q "Initialization Sequence Completed" /tmp/openvpn.log; then\n\
          echo "✅ VPN connected after delay"\n\
          NEW_IP=$(timeout 10 curl -s https://ipinfo.io/ip)\n\
          echo "External IP: $NEW_IP"\n\
        else\n\
          echo "⚠️  VPN connection incomplete, continuing without VPN"\n\
        fi\n\
      else\n\
        echo "❌ VPN process failed to start, continuing without VPN"\n\
      fi\n\
    else\n\
      echo "No suitable VPN config found for $NORDVPN_COUNTRY, running without VPN"\n\
    fi\n\
    \n\
    # Clean up auth file\n\
    rm -f /tmp/nordvpn-auth.txt\n\
  else\n\
    echo "No VPN credentials provided, running without VPN"\n\
  fi\n\
}\n\
\n\
echo "Waiting for ClickHouse to be ready..."\n\
\n\
# Wait for ClickHouse to be available (using HTTP interface which is simpler)\n\
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
  python3 ch_setup.py\n\
  echo "Setup completed!"\n\
  exit 0\n\
fi\n\
\n\
# Setup VPN for client containers\n\
if [ "$CONTAINER_TYPE" != "setup" ]; then\n\
  setup_vpn\n\
  echo "Waiting 5 seconds for VPN to stabilize..."\n\
  sleep 5\n\
fi\n\
\n\
# Execute the main command\n\
echo "Starting $CONTAINER_TYPE..."\n\
exec "$@"' > /entrypoint.sh

# Make entrypoint executable
RUN chmod +x /entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Default command (will be overridden in docker-compose)
CMD ["python3", "--version"]