#!/bin/bash
# Google Compute Engine Startup Script for mexc-pipe - v3 (Robust)
# This script automates the entire setup of the data pipeline on a fresh instance.
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# !!! IMPORTANT !!!
# !!! Your repository MUST be PUBLIC for this HTTPS URL to work in an automated script.
# !!! If your repository is private, you must either:
# !!!   1. Use an SSH URL (e.g., git@github.com:user/repo.git) and add a deploy key to your repository.
# !!!   2. Create a GitHub Personal Access Token and embed it in the URL (less secure).
# !!! Change this to your actual repository URL.
REPO_URL="git@github.com:jmbradford/pipe.git"
# Directory where the project will be cloned.
PROJECT_HOME="/opt"
PROJECT_DIR="${PROJECT_HOME}/mexcpipe"

# --- 1. System Update and Prerequisite Installation ---
echo ">>> STEP 1: Updating system packages and installing prerequisites..."
apt-get update
apt-get install -y git python3-venv python3-pip ca-certificates curl gnupg

# --- 2. Install Docker and Docker Compose ---
echo ">>> STEP 2: Installing Docker using the official repository..."
# Add Docker's official GPG key using a more robust method that avoids interactive prompts.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# --- 3. Configure SSH and Clone Project ---
echo ">>> STEP 3: Configuring SSH and cloning project from ${REPO_URL}..."

# Ensure the .ssh directory exists for the root user and has the correct permissions.
mkdir -p /root/.ssh
chmod 700 /root/.ssh

# Fetch the private key from instance metadata and save it.
# The key 'ssh-private-key' MUST be set in the GCE instance metadata for this to work.
echo "Fetching SSH private key from metadata..."
# Use -s for silent, -f to fail on server errors.
KEY_CONTENT=$(curl -s -f -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/ssh-private-key")

# Validate that the fetched key looks like a real SSH key.
if [[ -z "$KEY_CONTENT" || "$KEY_CONTENT" != *"-----BEGIN"* ]]; then
  echo "!!! FATAL: SSH private key not found or is invalid in instance metadata." >&2
  echo "!!! Please ensure a metadata key named 'ssh-private-key' exists and contains the valid private key." >&2
  exit 1
fi

echo "$KEY_CONTENT" > /root/.ssh/id_ed25519
chmod 600 /root/.ssh/id_ed25519

# Automatically add GitHub's host key to prevent interactive prompts.
echo "Scanning and adding GitHub's host key..."
ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

# --- 4. Clone or Update Project Repository ---
echo ">>> STEP 4: Cloning or updating project from ${REPO_URL}..."
if [ -d "$PROJECT_DIR" ]; then
  echo "Project directory exists. Pulling latest changes."
  cd "$PROJECT_DIR"
  git pull
else
  echo "Cloning repository."
  git clone "$REPO_URL" "$PROJECT_DIR"
  cd "$PROJECT_DIR"
fi

# --- 5. Set up Python Virtual Environment ---
echo ">>> STEP 5: Setting up Python virtual environment..."
python3 -m venv venv
# Use the python from the venv to run pip
./venv/bin/pip install -r requirements.txt

# --- 6. Configure Systemd Service for the Client ---
echo ">>> STEP 6: Configuring systemd service for the data client..."
cat > /etc/systemd/system/mexcpipe-client.service <<EOL
[Unit]
Description=MEXC Pipe WebSocket Client
After=docker.service
Requires=docker.service

[Service]
ExecStart=${PROJECT_DIR}/venv/bin/python ${PROJECT_DIR}/client.py
WorkingDirectory=${PROJECT_DIR}
StandardOutput=journal
StandardError=journal
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOL
systemctl daemon-reload
systemctl enable mexcpipe-client.service

# --- 7. Start Docker Services ---
echo ">>> STEP 7: Starting Docker services (Zookeeper, Kafka, ClickHouse)..."
docker compose up -d --build

# --- 8. Application Setup ---
echo ">>> STEP 8: Setting up the application..."
echo "Waiting 30 seconds for Docker services to fully initialize..."
sleep 30
# Run setup script with the virtual environment's python
./venv/bin/python ch_setup.py

# --- 9. Start the Data Ingestion Client ---
echo ">>> STEP 9: Starting the MEXC client via systemd..."
systemctl start mexcpipe-client.service

echo "--- Startup script finished successfully! The pipeline is running. ---"