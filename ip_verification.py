#!/usr/bin/env python3
"""
IP Verification Module for MEXC Client Containers
Provides functionality to verify unique IP addresses via Tor proxy
"""

import subprocess
import json
import time
import os
from typing import Dict, Optional


def get_my_external_ip() -> Optional[str]:
    """Get the external IP address of this container via Tor proxy."""
    try:
        result = subprocess.run([
            "timeout", "10", "curl", "--socks4", "127.0.0.1:9050", "-s", "https://ipinfo.io/ip"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0 and result.stdout.strip():
            ip = result.stdout.strip()
            # Basic IP validation
            if ip.count('.') == 3 and all(part.isdigit() for part in ip.split('.')):
                return ip
    except Exception:
        pass
    
    return None


def get_location_for_ip(ip: str) -> str:
    """Get location information for an IP address via Tor proxy."""
    try:
        result = subprocess.run([
            "timeout", "10", "curl", "--socks4", "127.0.0.1:9050", "-s", f"https://ipinfo.io/{ip}"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0 and result.stdout.strip():
            try:
                location_data = json.loads(result.stdout.strip())
                city = location_data.get('city', 'Unknown')
                country = location_data.get('country', 'Unknown')
                return f"{city}, {country}"
            except json.JSONDecodeError:
                # Fallback to simple text parsing
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


def get_other_container_ips() -> Dict[str, str]:
    """Get IP addresses of other client containers."""
    container_ips = {}
    container_names = ["client-btc", "client-eth", "client-sol"]
    
    for container in container_names:
        try:
            # Check if container is running
            check_result = subprocess.run([
                "docker", "ps", "--format", "{{.Names}}", "--filter", f"name={container}"
            ], capture_output=True, text=True, timeout=10)
            
            if check_result.returncode == 0 and container in check_result.stdout:
                # Get IP from container
                ip_result = subprocess.run([
                    "docker", "exec", container, "timeout", "10", 
                    "curl", "--socks4", "127.0.0.1:9050", "-s", "https://ipinfo.io/ip"
                ], capture_output=True, text=True, timeout=15)
                
                if ip_result.returncode == 0 and ip_result.stdout.strip():
                    ip = ip_result.stdout.strip()
                    if ip.count('.') == 3 and all(part.isdigit() for part in ip.split('.')):
                        container_ips[container] = ip
        except Exception:
            continue
    
    return container_ips


def verify_ip_uniqueness(container_name: str, max_retries: int = 3, retry_delay: int = 10) -> bool:
    """
    Verify that this container has a unique IP address compared to other containers.
    
    Args:
        container_name: Name of this container (e.g., "client-btc")
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        True if IP is unique, False otherwise
    """
    symbol = container_name.split('-')[1].upper()
    
    print(f"🔍 {symbol}: Verifying IP uniqueness before connecting to MEXC...")
    
    for attempt in range(max_retries):
        try:
            # Get our IP
            my_ip = get_my_external_ip()
            if not my_ip:
                print(f"⚠️  {symbol}: Could not determine external IP via Tor proxy (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                continue
            
            location = get_location_for_ip(my_ip)
            print(f"✅ {symbol}: External IP: {my_ip} ({location})")
            
            # Get other container IPs
            other_ips = get_other_container_ips()
            
            # Remove ourselves from the list
            other_ips.pop(container_name, None)
            
            if not other_ips:
                print(f"ℹ️  {symbol}: No other containers running - IP verification passed")
                return True
            
            # Check for IP conflicts
            conflicts = []
            for other_container, other_ip in other_ips.items():
                if other_ip == my_ip:
                    conflicts.append(other_container)
            
            if conflicts:
                print(f"❌ {symbol}: IP conflict detected!")
                print(f"   Our IP: {my_ip}")
                print(f"   Conflicts with: {', '.join(conflicts)}")
                print(f"   This violates MEXC API requirements for multiple depth subscriptions")
                
                if attempt < max_retries - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                continue
            
            # Success - unique IP detected
            print(f"✅ {symbol}: IP uniqueness verified")
            print(f"   Our IP: {my_ip}")
            for other_container, other_ip in other_ips.items():
                other_symbol = other_container.split('-')[1].upper()
                print(f"   {other_symbol} IP: {other_ip}")
            
            return True
            
        except Exception as e:
            print(f"⚠️  {symbol}: IP verification error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    print(f"❌ {symbol}: IP verification failed after {max_retries} attempts")
    return False


def wait_for_tor_proxy(timeout: int = 60) -> bool:
    """
    Wait for Tor proxy to become available.
    
    Args:
        timeout: Maximum time to wait in seconds
    
    Returns:
        True if Tor proxy is available, False otherwise
    """
    print("🔄 Waiting for Tor proxy to become available...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Test Tor proxy connectivity
            result = subprocess.run([
                "timeout", "5", "curl", "--socks4", "127.0.0.1:9050", "-s", "https://ipinfo.io/ip"
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                ip = result.stdout.strip()
                if ip.count('.') == 3 and all(part.isdigit() for part in ip.split('.')):
                    print(f"✅ Tor proxy available - External IP: {ip}")
                    return True
        except Exception:
            pass
        
        time.sleep(2)
    
    print(f"❌ Tor proxy did not become available within {timeout} seconds")
    return False