#!/usr/bin/env python3
"""
MEXC IP Separation Test Script
Tests that each container uses a different IP address via Tor proxy
"""

import subprocess
import json
import sys
import time
import requests
from typing import Dict, List, Optional, Tuple


def run_docker_command(command: str) -> Tuple[bool, str]:
    """Run a docker command and return success status and output."""
    try:
        result = subprocess.run(
            command.split(),
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def check_container_status() -> List[str]:
    """Check if client containers are running and return their names."""
    print("📋 Checking container status...")
    
    # Get running containers
    success, output = run_docker_command("docker-compose ps -q client-btc client-eth client-sol")
    
    if not success or not output:
        print("❌ No client containers are running. Start deployment first:")
        print("   docker-compose up -d")
        sys.exit(1)
    
    # Get container names that are actually running
    container_names = ["client-btc", "client-eth", "client-sol"]
    running_containers = []
    
    for container in container_names:
        success, _ = run_docker_command(f"docker ps --format {{{{.Names}}}} --filter name={container}")
        if success:
            running_containers.append(container)
    
    return running_containers


def get_container_ip(container_name: str) -> Optional[str]:
    """Get the external IP address of a container via Tor proxy."""
    print(f"Testing {container_name}...")
    
    # Check if container is actually running
    success, output = run_docker_command(f"docker ps --format {{{{.Names}}}} --filter name={container_name}")
    if not success or container_name not in output:
        print(f"  {container_name}: Not running")
        return None
    
    # Get IP via Tor proxy
    try:
        result = subprocess.run([
            "docker", "exec", container_name, "timeout", "10", 
            "curl", "--socks4", "127.0.0.1:9050", "-s", "https://ipinfo.io/ip"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0 and result.stdout.strip():
            ip = result.stdout.strip()
            # Basic IP validation
            if ip.count('.') == 3 and all(part.isdigit() for part in ip.split('.')):
                return ip
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        pass
    
    print(f"  ❌ {container_name}: Failed to get IP via proxy")
    return None


def get_location_info(container_name: str, ip: str) -> str:
    """Get location information for an IP address via container's Tor proxy."""
    try:
        result = subprocess.run([
            "docker", "exec", container_name, "timeout", "10",
            "curl", "--socks4", "127.0.0.1:9050", "-s", f"https://ipinfo.io/{ip}"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0 and result.stdout.strip():
            try:
                location_data = json.loads(result.stdout.strip())
                city = location_data.get('city', 'Unknown')
                country = location_data.get('country', 'Unknown')
                return f"{city}, {country}"
            except json.JSONDecodeError:
                # Fallback to simple text parsing if JSON parsing fails
                output = result.stdout.strip()
                city = "Unknown"
                country = "Unknown"
                
                for line in output.split('\n'):
                    if '"city"' in line:
                        city = line.split('"')[3] if len(line.split('"')) > 3 else "Unknown"
                    elif '"country"' in line:
                        country = line.split('"')[3] if len(line.split('"')) > 3 else "Unknown"
                
                return f"{city}, {country}"
    except Exception:
        pass
    
    return "Unknown"


def analyze_ip_separation(container_ips: Dict[str, str]) -> None:
    """Analyze IP separation and display results."""
    print("📊 IP Separation Analysis:")
    print("=" * 26)
    
    # Check if we got any IPs
    if not container_ips:
        print("❌ No container IPs could be verified")
        print("")
        print("🔧 Troubleshooting:")
        print("  - Check if containers are running: docker-compose ps")
        print("  - Check Tor proxy status: docker-compose logs client-btc | grep -i tor")
        print("  - Restart containers: docker-compose restart")
        sys.exit(1)
    
    # Display all detected IPs
    print("Detected container IPs:")
    for container, ip in container_ips.items():
        symbol = container.split('-')[1].upper()
        print(f"  {symbol}: {ip}")
    print("")
    
    # Check for uniqueness
    unique_ips = list(set(container_ips.values()))
    total_containers = len(container_ips)
    unique_count = len(unique_ips)
    
    if unique_count == total_containers and total_containers >= 2:
        print(f"✅ IP Separation Success: {unique_count} unique IPs detected")
        print("   Each container appears to MEXC with a different IP address")
        print("   ✅ MEXC API compliance: Multiple sub.depth.full subscriptions supported")
    elif unique_count < total_containers:
        print(f"⚠️  Warning: {unique_count} unique IPs from {total_containers} containers")
        print("   Some containers may share IP addresses")
        print("   ⚠️  MEXC API impact: May not support multiple sub.depth.full subscriptions")
    else:
        print(f"ℹ️  Note: {unique_count} container(s) detected with unique IPs")
    
    print("")
    print("🎯 Expected for MEXC compliance:")
    print("  - Each cryptocurrency client should have a different IP")
    print("  - This allows multiple sub.depth.full WebSocket subscriptions")
    print("  - Tor proxy provides automatic IP separation without VPN credentials")


def main():
    """Main function to run IP separation verification."""
    print("🔍 MEXC IP Separation Verification")
    print("=" * 35)
    print("")
    
    # Check container status
    running_containers = check_container_status()
    
    print("🌐 Testing IP separation for each container...")
    print("")
    
    # Test each container
    container_ips = {}
    
    for container in running_containers:
        ip = get_container_ip(container)
        if ip:
            container_ips[container] = ip
            location = get_location_info(container, ip)
            symbol = container.split('-')[1].upper()
            print(f"  ✅ {symbol} Container ({container}):")
            print(f"     IP: {ip}")
            print(f"     Location: {location}")
            print("")
    
    # Analyze results
    analyze_ip_separation(container_ips)


if __name__ == "__main__":
    main()