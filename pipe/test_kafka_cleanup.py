#!/usr/bin/env python3
"""
Test script to verify Kafka cleanup functionality.
"""

import os
import sys
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_kafka_retention():
    """Test Kafka retention policies"""
    
    # Kafka connection
    bootstrap_servers = ['localhost:9092']
    
    # Create admin client
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    # Create test topic with retention
    test_topic = 'test_retention_topic'
    try:
        topic_config = {
            'retention.ms': '60000',  # 1 minute retention for testing
            'retention.bytes': '1048576',  # 1MB max size
            'segment.bytes': '524288',  # 512KB segments
            'cleanup.policy': 'delete'
        }
        
        topic = NewTopic(
            name=test_topic,
            num_partitions=1,
            replication_factor=1,
            topic_configs=topic_config
        )
        
        admin_client.create_topics([topic])
        logger.info(f"Created test topic: {test_topic}")
        
    except TopicAlreadyExistsError:
        logger.info(f"Test topic {test_topic} already exists")
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Send test messages
    logger.info("Sending test messages...")
    for i in range(1000):
        message = {
            'id': i,
            'data': f'Test message {i}' * 100,  # Make messages larger
            'timestamp': time.time()
        }
        producer.send(test_topic, value=message)
        if i % 100 == 0:
            logger.info(f"Sent {i} messages")
    
    producer.flush()
    logger.info("Finished sending messages")
    
    # Wait for retention to kick in
    logger.info("Waiting for retention policies to take effect...")
    time.sleep(90)  # Wait 90 seconds
    
    # Check if messages were cleaned up
    consumer = KafkaConsumer(
        test_topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000
    )
    
    message_count = 0
    for message in consumer:
        message_count += 1
        if message_count > 100:  # Just count first 100 to avoid timeout
            break
    
    logger.info(f"Messages remaining after cleanup: {message_count}")
    
    # Cleanup
    admin_client.delete_topics([test_topic])
    logger.info("Test completed and cleanup done")

if __name__ == "__main__":
    test_kafka_retention()