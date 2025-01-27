import websocket
import json
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

# WebSocket message handlers
def on_message(ws, message):
    print(f"Raw message received: {message}")  # Log the raw message
    global ohlc_data
    data = json.loads(message)
    print(f"Received message: {data}")  # Debug: Log received data

    if isinstance(data, list) and "ohlc" in data[-1]:
        ohlc = data[1]
        pair = data[-1].split('-')[1]
        ohlc_entry = {
            "timestamp": datetime.utcfromtimestamp(float(ohlc[0])).strftime('%Y-%m-%d %H:%M:%S'),
            "open": float(ohlc[1]),
            "high": float(ohlc[2]),
            "low": float(ohlc[3]),
            "close": float(ohlc[4]),
            "volume": float(ohlc[5]),
            "vwap": float(ohlc[6])
        }

        with data_lock:
            if pair not in ohlc_data:
                ohlc_data[pair] = []
            ohlc_data[pair].append(ohlc_entry)
            if len(ohlc_data[pair]) > 60:
                ohlc_data[pair] = ohlc_data[pair][-60:]

        print(f"Updated OHLC data for {pair}: {ohlc_entry}")  # Debug: Log processed data

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

def on_open(ws, pair_group):
    subscription_message = {
        "event": "subscribe",
        "pair": pair_group,
        "subscription": {"name": "ohlc", "interval": 1}
    }
    ws.send(json.dumps(subscription_message))
    print(f"WebSocket opened and subscribed to pairs: {pair_group}")

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

def convert_to_dataframe():
    with data_lock:
        frames = []
        for pair, ohlc_list in ohlc_data.items():
            print(f"Processing {pair}, number of records: {len(ohlc_list)}")  # Debugging
            df = pd.DataFrame(ohlc_list)  # Ensure pd is recognized here
            df["pair"] = pair
            frames.append(df)
        if not frames:
            print("No data to concatenate")
            return pd.DataFrame()  # Return an empty DataFrame if no data exists
        return pd.concat(frames, ignore_index=False)



# import time

# stop_event = threading.Event()

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

# def run_websocket(ws):
#     while not stop_event.is_set():
#         ws.run_forever()
#     ws.close()

if __name__ == "__main__":
    # Start WebSocket connections
    start_websockets()
    # print(f"ohlc data: {ohlc_data}")
    # Convert to DataFrame after collecting data
    ohlc_dataframe = convert_to_dataframe()
    print(ohlc_dataframe)
    # Save to CSV
    ohlc_dataframe.to_csv("ohlc_data.csv", index=False)
