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

example_dict = df = {'symbol': 'BTC/USD',
                            'open': 99129.6, 'high': 99249.9, 'low': 99129.6,
                            'close': 99200.6, 'trades': 86, 'volume': 1.52633817,
                            'vwap': 99190.1, 'interval_begin': '2025-01-27T18:30:00.000000000Z',
                            'interval': 1, 'timestamp': '2025-01-27T18:31:00.000000Z'}

df = pd.DataFrame(columns=example_dict.keys())

# Function to handle incoming messages
def on_message(ws, message):
    """
    Handle incoming messages from the WebSocket.
    """

    df = pd.DataFrame(columns=example_dict.keys())
    try:
        data = json.loads(message)
        print(f"Channel: {data['channel']}")  # Example of processing the received data
        if data['channel'] == 'ohlc':
            data_to_parse = data['data']
            # print(data_to_parse)
            # for line in data_to_parse:
            #     new_line = pd.DataFrame(line)
            #     pd.concat(new_line, df)
            # print(f"data keys: {data_to_parse}")
            new_df = pd.DataFrame(data_to_parse)
            print(new_df)
            df = pd.concat([new_df, df])
            print(f"dataframe: {df}")
            # print(f"data: {data['data']}")
    except Exception as e:
        print(f"Error processing message: {e}")
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
        "method": "subscribe",
        "params": {
            "channel": "ohlc",
            "symbol": ['BTC/USD'],
            "interval": 1
        }
    }
    ws.send(json.dumps(subscription_message))
    print(f"Subscribed to OHLC data")

# Main function to start the WebSocket connection
def start_websocket():
    websocket_url = "wss://ws.kraken.com/v2"
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
            "wss://ws.kraken.com/v2",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.on_open = lambda ws, pg=pair_group: on_open(ws, pg)
        thread = threading.Thread(target=ws.run_forever, daemon=True)  # Use daemon threads
        threads.append(thread)
        thread.start()

    print("All WebSocket connections started.")

if __name__ == "__main__":
    print("Starting WebSocket connection...")
    start_websocket()
