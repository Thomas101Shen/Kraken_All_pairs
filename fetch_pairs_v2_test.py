import websocket
import json
import time
import threading
import pandas as pd
from datetime import datetime
import os
import re

from kraken_pairs import kraken_pairs

class WebSocketHandler:
    def __init__(self, websocket_url="wss://ws.kraken.com/v2", chunk_size=450, interval=1):
        self.websocket_url = websocket_url
        self.chunk_size = chunk_size
        self.interval = interval

        # Divide pairs into chunks
        self.connections = [kraken_pairs[i:i + self.chunk_size] for i in range(0, len(kraken_pairs), self.chunk_size)]

        # Dictionary to store OHLC data for each pair
        self.ohlc_data = pd.DataFrame()

        # Lock for thread-safe updates
        self.data_lock = threading.Lock()
        self.stop_event = threading.Event()

    def convert_to_dataframe(self, data):
        with self.data_lock:
            new_data = pd.DataFrame(data['data'])
            self.ohlc_data = pd.concat([new_data, self.ohlc_data])
            print(self.ohlc_data.head())

    def on_message(self, ws, message):
        """
        Handle incoming messages from the WebSocket.
        """
        try:
            data = json.loads(message)
            # print(f"Channel: {data['channel']}")
            if data['channel'] == 'ohlc':
                self.convert_to_dataframe(data)
        except Exception as e:
            print(f"Error processing message: {e}")


    def on_error(self, ws, error):
        """
        Handle WebSocket errors.
        """
        print(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Handle WebSocket closure.
        """
        print(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")


    def on_open(self, ws, pair_group=None):
        """
        Handle WebSocket connection opening and subscribe to channels.
        """
        subscription_message = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": pair_group if pair_group else ["BTC/USD"],
                "interval": self.interval
            }
        }
        ws.send(json.dumps(subscription_message))
        print(f"Subscribed to OHLC data for pairs: {pair_group if pair_group else ["BTC/USD"]}")

    def start_websocket(self):
        """
        Start a single WebSocket connection.
        """
        ws = websocket.WebSocketApp(
            self.websocket_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.on_open = self.on_open
        ws.run_forever()

    def stop_websockets(self):
        """
        Stop all WebSocket connections gracefully.
        """
        self.stop_event.set()
        print("Stopping all WebSocket connections.")

    # def run_persistent_websocket(self, ws):
    #     while not self.stop_event.is_set():
    #         try:
    #             ws.run_forever()
    #         except Exception as e:
    #             print(f"WebSocket connection error: {e}")
    #             time.sleep(5)  # Wait before reconnecting

    def start_websockets(self):
        """
        Start multiple WebSocket connections in parallel threads.
        """
        threads = []
        # print(f"connections: {self.connections}")
        for pair_group in self.connections:
            ws = websocket.WebSocketApp(
                self.websocket_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            ws.on_open = lambda ws, pg=pair_group: self.on_open(ws, pg)
            thread = threading.Thread(target=ws.run_forever, daemon=True)
            threads.append(thread)
            thread.start()

        print("All WebSocket connections started.")

def convert_to_csv(df, save_path="./crypto_data"):
    """
    Converts a dataframe to CSV files, organized by symbol.
    Creates a directory if it does not exist.
    """
    # Ensure save_path exists
    os.makedirs(save_path, exist_ok=True)

    for symbol, df_grouped in df.groupby("symbol"):
        # Sanitize the symbol name to remove any invalid characters for filenames
        safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)

        # Define the file path
        file_path = os.path.join(save_path, f"{safe_symbol}_data.csv")

        # Save the grouped data
        df_grouped.to_csv(file_path, index=False)
        print(f"Saved {safe_symbol} data to {file_path}")

if __name__ == "__main__":
    print("Initializing WebSocketHandler...")
    handler = WebSocketHandler()
    try:
        handler.start_websockets()
        while True:
            # ohlc_data = handler.convert_to_dataframe()
            # ohlc_data.to_csv('./ohlc_data.csv')
            # print("ohlc data:", handler.ohlc_data.head())
            if len(handler.ohlc_data) > 500:
                convert_to_csv(handler.ohlc_data)
                handler.ohlc_data = pd.DataFrame()
            time.sleep(1)  # Keep the main thread alive

    except KeyboardInterrupt:
        print("Keyboard interrupt received. Shutting down...")
        handler.stop_websockets()