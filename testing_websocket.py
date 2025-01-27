import websocket
import json
import time
import threading
import pandas as pd
from datetime import datetime

from kraken_pairs import kraken_pairs

# Divide pairs into chunks of 500
chunk_size = 500
connections = [kraken_pairs[i:i + chunk_size] for i in range(0, len(kraken_pairs), chunk_size)]

# Initialize a global dictionary to store OHLC data for each pair
ohlc_data = {}

# Lock for thread-safe DataFrame updates
data_lock = threading.Lock()

# Function to handle incoming messages
def on_message(ws, message):
    print(f"Raw message received: {message}")
    data = json.loads(message)
    if isinstance(data, list):
        print(f"Message received: {data}")  # Print the raw data for debugging
        print(f"Message type: {data[-2]}")

# Function to handle errors
def on_error(ws, error):
    print(f"WebSocket error: {error}")

# Function to handle the connection closing
def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")

# Function to handle the connection opening and send a subscription message
def on_open(ws):
    # Subscription message to request OHLC data for BTC/USD
    subscription_message = {
        "event": "subscribe",
        "pair": ["BTC/USD"],
        "subscription": {"name": "ohlc", "interval": 1}  # 1-minute OHLC data
    }
    ws.send(json.dumps(subscription_message))
    print(f"Subscribed to OHLC data for BTC/USD")

# Main function to start the WebSocket connection
def start_websocket():
    websocket_url = "wss://ws.kraken.com"
    ws = websocket.WebSocketApp(
        websocket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

def start_websockets():
    threads = []
    for pair_group in connections:
        # print(f"Starting WebSocket for pairs: {pair_group}")  # Log which pairs are being 2subscribed to
        ws = websocket.WebSocketApp(
            "wss://ws.kraken.com/",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.on_open = lambda ws, pg=pair_group: on_open(ws, pg)
        thread = threading.Thread(target=ws.run_forever, daemon=True)  # Use daemon threads
        threads.append(thread)
        thread.start()

    print("All WebSocket connections started.")

# def start_websockets():
#     threads = []
#     for pair_group in connections:
#         ws = websocket.WebSocketApp(
#             "wss://ws.kraken.com/",
#             on_message=on_message,
#             on_error=on_error,
#             on_close=on_close
#         )
#         ws.on_open = lambda ws, pg=pair_group: on_open(ws, pg)
#         thread = threading.Thread(target=lambda: run_websocket(ws))
#         threads.append(thread)
#         thread.start()

#     print("All WebSocket connections started.")

#     # Wait for a certain duration or until a stop condition
#     try:
#         time.sleep(60)  # Run for 60 seconds
#     finally:
#         stop_event.set()  # Signal threads to stop

#     # Wait for all threads to finish
#     for thread in threads:
#         thread.join()
#     print("All WebSocket connections started.")

if __name__ == "__main__":
    print("Starting WebSocket connection...")
    start_websocket()
