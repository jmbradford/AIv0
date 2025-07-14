#!/usr/bin/env python3
"""Test script for hourly exporter - exports previous hour immediately for testing."""

import sys
sys.path.append('/app')

from hourly_exporter import HourlyExporter

if __name__ == "__main__":
    print("Testing hourly exporter for previous hour...")
    exporter = HourlyExporter()
    exporter.run_once()
    print("\nTest complete!")