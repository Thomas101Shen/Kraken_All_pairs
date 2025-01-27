import websocket
import json
import threading
import time

from kraken_pairs import kraken_pairs
# Global stop event to control WebSocket threads
stop_event = threading.Event()

chunk_size = 450
connections = [kraken_pairs[i:i + chunk_size] for i in range(0, len(kraken_pairs), chunk_size)]

# Initialize a global dictionary to store OHLC data for each pair
ohlc_data = {}

# Lock for thread-safe DataFrame updates
data_lock = threading.Lock()

def on_message(ws, message):
    print(f"Raw message received: {message}")  # Log the raw message
    global ohlc_data
    data = json.loads(message)
    print(f"Received message: {data}")  # Debug: Log received data
    # print(isinstance(data, list))
    # print(data[3])
    if isinstance(data, list):
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
    print(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
    if not stop_event.is_set():
        print("Reconnecting WebSocket...")
        reconnect(ws)

def on_open(ws, pair_group):
    subscription_message = {
        "event": "subscribe",
        "pair": pair_group,
        "subscription": {"name": "ohlc", "interval": 1}
    }
    ws.send(json.dumps(subscription_message))
    print(f"Subscribed to pairs: {pair_group}")

def reconnect(ws):
    retry_count = 0
    while not stop_event.is_set() and retry_count < 5:  # Retry up to 5 times
        try:
            wait_time = min(2 ** retry_count, 30)  # Exponential backoff (max 30 seconds)
            print(f"Reconnecting in {wait_time} seconds...")
            time.sleep(wait_time)
            ws.run_forever()
            break  # Exit loop if reconnection is successful
        except Exception as e:
            print(f"Reconnection failed: {e}")
            retry_count += 1

def start_websockets(connections):
    threads = []
    for pair_group in connections:
        ws = websocket.WebSocketApp(
            "wss://ws.kraken.com/",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.on_open = lambda ws, pg=pair_group: on_open(ws, pg)
        
        thread = threading.Thread(target=lambda: run_persistent_websocket(ws), daemon=True)
        threads.append(thread)
        thread.start()

    print("All WebSocket connections started.")
    return threads

def run_persistent_websocket(ws):
    while not stop_event.is_set():
        try:
            ws.run_forever()
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            time.sleep(5)  # Wait before reconnecting

def convert_to_dataframe():
    with data_lock:
        frames = []
        for pair, ohlc_list in ohlc_data.items():
            df = pd.DataFrame(ohlc_list)
            df["pair"] = pair
            frames.append(df)
        return pd.concat(frames, ignore_index=False)

# [4414898189,
# ['1737996675.228397', '1737996720.000000', '99804.50000',
# '99804.60000', '99804.50000', '99804.60000', '99804.59979', '9.83629309', 73], 'ohlc-1', 'XBT/USD']

if __name__ == "__main__":
    # connections = [["BTC/USDT", "ETH/USDT"]]  # Example groups of pairs
    threads = start_websockets(connections)

    try:
        # Keep the main thread alive while WebSockets are running
        while True:
            time.sleep(1)
            print("\n\n\n")
            # ohlc_dataframe = convert_to_dataframe()
            # ohlc_dataframe.to_csv("ohlc_data.csv", index=False)
    except KeyboardInterrupt:
        print("Stopping WebSocket connections...")
        stop_event.set()  # Signal threads to stop
        for thread in threads:
            thread.join()  # Wait for threads to exit
        print("All WebSocket connections stopped.")
