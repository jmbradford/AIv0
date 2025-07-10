import asyncio
import json
import logging
import websockets

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
MEXC_WSS_URL = "wss://contract.mexc.com/edge"
SYMBOL = "ETH_USDT"
OUTPUT_FILE = "websocket_output.log"
# ---------------------

async def send_ping(websocket):
    """Sends a ping every 30 seconds to keep the connection alive."""
    while True:
        try:
            await websocket.send(json.dumps({"method": "ping"}))
            await asyncio.sleep(30)
        except websockets.exceptions.ConnectionClosed:
            logging.warning("Connection closed. Stopping ping task.")
            break

async def subscribe_to_ticker(websocket):
    """Subscribes to the ticker channel for the specified symbol."""
    sub_payload = {
        "method": "sub.ticker",
        "param": {
            "symbol": SYMBOL
        }
    }
    await websocket.send(json.dumps(sub_payload))
    logging.info(f"Sent subscription request for {SYMBOL} ticker.")

async def listen_to_stream():
    """Connects to the WebSocket, subscribes, and prints all incoming messages."""
    logging.info("Attempting to connect to WebSocket...")
    # Clear the output file at the start of the session
    with open(OUTPUT_FILE, "w") as f:
        f.write("")

    async for websocket in websockets.connect(MEXC_WSS_URL, ping_interval=None):
        try:
            logging.info("Successfully connected to MEXC WebSocket.")
            await subscribe_to_ticker(websocket)

            # Start the ping task in the background
            ping_task = asyncio.create_task(send_ping(websocket))

            async for message in websocket:
                # Ensure message is a string before writing
                message_str = message if isinstance(message, str) else message.decode('utf-8')
                
                # Log and write the message
                logging.info(f"Received message: {message_str}")
                with open(OUTPUT_FILE, "a") as f:
                    f.write(message_str + "\n")

        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"Connection closed: {e}. Reconnecting...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            break # Exit on other errors

if __name__ == "__main__":
    try:
        asyncio.run(listen_to_stream())
    except KeyboardInterrupt:
        logging.info("Script stopped by user.") 