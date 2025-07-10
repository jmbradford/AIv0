import asyncio
import json
import logging
import websockets
import config
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global Kafka Producer ---
# Initialized in main()
producer = None

def get_topic_from_channel(channel):
    """Maps a WebSocket channel to a Kafka topic."""
    if 'push.deal' in channel:
        return config.TOPICS["deals"]
    if 'push.kline' in channel:
        return config.TOPICS["klines"]
    if 'push.ticker' in channel:
        return config.TOPICS["tickers"]
    if 'push.depth.full' in channel:
        return config.TOPICS["depth"]
    return None

async def send_ping(ws):
    """Sends a ping to the server every 20 seconds to keep the connection alive."""
    while True:
        try:
            await ws.send(json.dumps({"method": "ping"}))
            await asyncio.sleep(20)
        except websockets.exceptions.ConnectionClosed:
            logging.info("Connection closed, stopping ping task.")
            break

async def subscribe(ws):
    """Subscribes to all channels defined in the config."""
    for sub in config.SUBSCRIPTIONS.values():
        await ws.send(json.dumps(sub))
        logging.info(f"Subscribed to {sub['method']} for {sub['param'].get('symbol')}")
        await asyncio.sleep(0.1) # Avoid rate limiting

async def mexc_websocket_client():
    """The main WebSocket client loop."""
    uri = config.MEXC_WSS_URL
    async for ws in websockets.connect(uri, ping_interval=None):
        try:
            # Start the background ping task
            ping_task = asyncio.create_task(send_ping(ws))
            
            await subscribe(ws)
            while True:
                message = await ws.recv()
                data = json.loads(message)
                
                # Handle PONG response for keep-alive
                if data.get("method") == "PING":
                    await ws.send(json.dumps({"method": "PONG"}))
                    continue

                channel = data.get('channel')
                if not channel:
                    continue

                topic = get_topic_from_channel(channel)
                if topic and producer:
                    try:
                        producer.send(topic, value=data)
                        logging.info(f"Sent message to Kafka topic '{topic}' for channel '{channel}'.")
                    except KafkaError as e:
                        logging.error(f"Failed to send message to Kafka: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"Connection closed: {e}. Reconnecting...")
            ping_task.cancel()  # Stop the ping task when connection is closed
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            ping_task.cancel() # Stop the ping task on other errors
            # In case of a non-connection error, break the inner loop 
            # to allow the main loop to handle reconnection logic.
            break

async def main():
    """Initializes producer and runs the client."""
    global producer
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKERS_HOST,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=60000,
            reconnect_backoff_ms=1000,
            max_block_ms=120000,
            linger_ms=20
        )
        logging.info("✅ Successfully connected to Kafka producer.")
        
        while True:
            await mexc_websocket_client()
            logging.info("Reconnecting after 5 seconds...")
            await asyncio.sleep(5)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("Shutdown signal received, initiating graceful exit...")
    except KafkaError as e:
        logging.error(f"FATAL: Could not initialize Kafka producer: {e}")
    except Exception as e:
        logging.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        if producer:
            logging.info("Flushing final messages and closing Kafka producer...")
            producer.flush(timeout=30)
            producer.close(timeout=30)
            logging.info("Kafka producer closed.")
        logging.info("Client has shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Client shutdown complete.") 