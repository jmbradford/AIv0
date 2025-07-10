#!/usr/bin/env python3
"""
Automated Kafka cleanup service for memory and storage management.
Monitors Kafka topics and forces cleanup when needed without interrupting data flow.
"""

import time
import logging
from datetime import datetime
from kafka import KafkaAdminClient
from kafka.admin.config_resource import ConfigResource, ConfigResourceType
from kafka.admin.client_api import AlterConfigResponse
from kafka.errors import KafkaError
import subprocess
import os
import json
from config import KAFKA_BROKERS, KAFKA_TOPICS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/kafka-cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KafkaCleanupService:
    def __init__(self):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id='kafka-cleanup-service'
        )
        
        # Cleanup thresholds - optimized for memory efficiency
        self.max_size_bytes = 100 * 1024 * 1024  # 100MB per topic (reduced from 500MB)
        self.max_memory_usage = 70  # 70% memory threshold (more aggressive)
        self.retention_ms = 600000  # 10 minute retention (reduced from 1 hour)
        self.cleanup_interval = 120  # 2 minutes (increased frequency)
        
    def get_topic_sizes(self):
        """Get current size of all Kafka topics"""
        try:
            topic_sizes = {}
            for topic in KAFKA_TOPICS:
                # Use kafka-log-dirs tool to get topic sizes
                cmd = [
                    'kafka-log-dirs',
                    '--bootstrap-server', 'kafka:29092',
                    '--describe',
                    '--json'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    size = self._parse_topic_size(data, topic)
                    topic_sizes[topic] = size
                else:
                    logger.warning(f"Could not get size for topic {topic}")
                    topic_sizes[topic] = 0
                    
            return topic_sizes
        except Exception as e:
            logger.error(f"Error getting topic sizes: {e}")
            return {}
    
    def _parse_topic_size(self, log_dirs_data, topic_name):
        """Parse topic size from kafka-log-dirs output"""
        total_size = 0
        try:
            for broker in log_dirs_data.get('brokers', []):
                for log_dir in broker.get('logDirs', []):
                    for partition in log_dir.get('partitions', []):
                        if partition.get('topic') == topic_name:
                            total_size += partition.get('size', 0)
        except Exception as e:
            logger.error(f"Error parsing topic size for {topic_name}: {e}")
        return total_size
    
    def get_kafka_memory_usage(self):
        """Get current Kafka JVM memory usage"""
        try:
            cmd = [
                'docker', 'exec', 'kafka',
                'bash', '-c',
                'jstat -gc $(pgrep -f kafka) | tail -1'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                # Parse jstat output to get heap usage percentage
                parts = result.stdout.strip().split()
                if len(parts) >= 10:
                    used_heap = float(parts[2]) + float(parts[3])  # S0U + S1U + EU + OU
                    total_heap = float(parts[1]) + float(parts[4])  # S0C + S1C + EC + OC
                    return (used_heap / total_heap) * 100 if total_heap > 0 else 0
        except Exception as e:
            logger.error(f"Error getting memory usage: {e}")
        return 0
    
    def force_topic_cleanup(self, topic_name):
        """Force cleanup of a specific topic"""
        try:
            # Update topic configuration to force cleanup
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            config_updates = {
                'cleanup.policy': 'delete',
                'retention.ms': str(self.retention_ms),
                'segment.ms': '300000',  # 5 minutes
                'min.cleanable.dirty.ratio': '0.01'
            }
            
            self.admin_client.alter_configs({resource: config_updates})
            logger.info(f"Forced cleanup configuration for topic {topic_name}")
            
            # Trigger log cleanup by calling kafka-log-dirs
            cmd = [
                'docker', 'exec', 'kafka',
                'kafka-log-dirs',
                '--bootstrap-server', 'kafka:29092',
                '--alter',
                '--add-config', f'cleanup.policy=delete,retention.ms={self.retention_ms}',
                '--topic', topic_name
            ]
            
            subprocess.run(cmd, check=True)
            logger.info(f"Triggered log cleanup for topic {topic_name}")
            
        except Exception as e:
            logger.error(f"Error forcing cleanup for topic {topic_name}: {e}")
    
    def cleanup_oversized_topics(self):
        """Clean up topics that exceed size limits"""
        topic_sizes = self.get_topic_sizes()
        
        for topic, size in topic_sizes.items():
            if size > self.max_size_bytes:
                logger.warning(f"Topic {topic} size {size/1024/1024:.2f}MB exceeds limit")
                self.force_topic_cleanup(topic)
    
    def cleanup_on_memory_pressure(self):
        """Clean up when memory usage is high"""
        memory_usage = self.get_kafka_memory_usage()
        
        if memory_usage > self.max_memory_usage:
            logger.warning(f"Memory usage {memory_usage:.2f}% exceeds threshold")
            for topic in KAFKA_TOPICS:
                self.force_topic_cleanup(topic)
    
    def run_cleanup_cycle(self):
        """Run a complete cleanup cycle"""
        logger.info("Starting Kafka cleanup cycle")
        
        try:
            # Check topic sizes
            self.cleanup_oversized_topics()
            
            # Check memory usage
            self.cleanup_on_memory_pressure()
            
            # Log current status
            topic_sizes = self.get_topic_sizes()
            memory_usage = self.get_kafka_memory_usage()
            
            logger.info(f"Cleanup cycle completed. Memory usage: {memory_usage:.2f}%")
            for topic, size in topic_sizes.items():
                logger.info(f"Topic {topic}: {size/1024/1024:.2f}MB")
                
        except Exception as e:
            logger.error(f"Error in cleanup cycle: {e}")
    
    def run_service(self):
        """Main service loop"""
        logger.info("Starting Kafka cleanup service")
        
        while True:
            try:
                self.run_cleanup_cycle()
                time.sleep(self.cleanup_interval)
            except KeyboardInterrupt:
                logger.info("Cleanup service stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in service loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    service = KafkaCleanupService()
    service.run_service()