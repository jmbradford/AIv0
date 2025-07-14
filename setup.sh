#!/bin/bash

# MEXC Multi-Symbol VPN Deployment - Complete Setup Script
# Usage: ./setup.sh
# Requires: Docker, Docker Compose, Linux

set -e  # Exit on any error

echo "🚀 MEXC Multi-Symbol Cryptocurrency Pipeline Setup"
echo "=================================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    print_error "Please don't run this script as root"
    exit 1
fi

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first:"
    echo "  curl -fsSL https://get.docker.com -o get-docker.sh"
    echo "  sudo sh get-docker.sh"
    echo "  sudo usermod -aG docker $USER"
    echo "  newgrp docker"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first"
    exit 1
fi

# Check if user is in docker group
if ! groups | grep -q docker; then
    print_error "User is not in docker group. Please run:"
    echo "  sudo usermod -aG docker $USER"
    echo "  newgrp docker"
    exit 1
fi

# Test Docker
if ! docker ps &> /dev/null; then
    print_error "Cannot connect to Docker daemon. Make sure Docker is running and you have permissions"
    exit 1
fi

print_status "Docker and Docker Compose are available"

# Check Python
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8+"
    exit 1
fi

# Check pip
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 is not installed. Please install python3-pip"
    exit 1
fi

print_status "Python 3 and pip3 are available"

# Create Python virtual environment
echo ""
echo "🐍 Setting up Python virtual environment..."

if [ -d "venv" ]; then
    print_warning "Virtual environment already exists, removing old one"
    rm -rf venv
fi

python3 -m venv venv
print_status "Virtual environment created"

# Activate virtual environment
source venv/bin/activate
print_status "Virtual environment activated"

# Upgrade pip
pip install --upgrade pip
print_status "pip upgraded"

# Install Python dependencies
echo ""
echo "📦 Installing Python dependencies..."

if [ ! -f "requirements.txt" ]; then
    print_error "requirements.txt not found in current directory"
    exit 1
fi

pip install -r requirements.txt
print_status "Python dependencies installed"

# Create .env file if it doesn't exist
echo ""
echo "🔧 Setting up configuration..."

if [ ! -f ".env" ]; then
    print_warning "No .env file found, creating from template"
    if [ -f ".env.example" ]; then
        cp .env.example .env
        print_status ".env file created from template"
        echo ""
        print_warning "IMPORTANT: Edit .env file with your NordVPN credentials before running docker-compose up"
        echo "           Your NordVPN username and password are required for VPN functionality"
    else
        print_error ".env.example not found"
        exit 1
    fi
else
    print_status ".env file already exists"
fi

# Validate required files
echo ""
echo "📋 Validating project files..."

required_files=(
    "docker-compose.yml"
    "Dockerfile"
    "clickhouse-config-simple.xml"
    "config.py"
    "ch_setup.py"
    "btcdat.py"
    "ethdat.py"
    "soldat.py"
    "verif.py"
)

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        print_error "Required file missing: $file"
        exit 1
    fi
done

print_status "All required files present"

# Test ClickHouse config
echo ""
echo "🗄️  Validating ClickHouse configuration..."

if [ -f "clickhouse-config-simple.xml" ]; then
    # Basic XML validation
    if python3 -c "import xml.etree.ElementTree as ET; ET.parse('clickhouse-config-simple.xml')" 2>/dev/null; then
        print_status "ClickHouse configuration is valid XML"
    else
        print_error "ClickHouse configuration has invalid XML syntax"
        exit 1
    fi
else
    print_error "ClickHouse configuration file missing"
    exit 1
fi

# Check if docker-compose file is valid
echo ""
echo "🐳 Validating Docker Compose configuration..."

if docker-compose config >/dev/null 2>&1; then
    print_status "Docker Compose configuration is valid"
else
    print_error "Docker Compose configuration has errors"
    docker-compose config
    exit 1
fi

# Pull base Docker images to save time later
echo ""
echo "🖼️  Pre-downloading Docker images..."

docker pull python:3.9-slim
docker pull clickhouse/clickhouse-server:24.3-alpine
print_status "Base Docker images downloaded"

# Create deployment summary
echo ""
echo "📊 Setup Summary"
echo "================"
echo "✅ Virtual environment: $(pwd)/venv"
echo "✅ Python dependencies: Installed"
echo "✅ Configuration files: Validated"
echo "✅ Docker images: Pre-downloaded"
echo "✅ Project structure: Complete"
echo ""

# Check .env file status
if grep -q "your_nordvpn_username" .env 2>/dev/null; then
    print_warning "⚠️  .env file contains placeholder values"
    echo "     Edit .env with your actual NordVPN credentials before deployment"
    echo ""
fi

# Final instructions
echo "🎯 Next Steps"
echo "============="
echo ""
echo "1. Edit .env file with your NordVPN credentials:"
echo "   nano .env"
echo ""
echo "2. Start the complete multi-symbol VPN deployment:"
echo "   docker-compose up -d"
echo ""
echo "3. Monitor the deployment:"
echo "   docker-compose logs -f"
echo ""
echo "4. Verify data collection:"
echo "   source venv/bin/activate && python3 verif.py"
echo ""

print_status "Setup completed successfully!"
print_status "Run 'docker-compose up -d' to start deployment"

echo ""
echo "📖 For troubleshooting, see VPN-SETUP.md"