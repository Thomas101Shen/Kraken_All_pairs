# import time
# from fetch_pairs import WebSocketHandler
# from data_operations import convert_to_csv, limit_symbol_instances
# import pandas as pd
# from kraken_pairs import kraken_pairs

# if __name__ == "__main__":
#     num_pairs = len(kraken_pairs[0]) + len(kraken_pairs[1])
#     print("Initializing WebSocketHandler...")
#     handler = WebSocketHandler()
#     try:
#         handler.start_websockets()
#         while True:
#             if len(handler.ohlc_data) * 60 >= num_pairs:
#                 handler.ohlc_data = limit_symbol_instances(handler.ohlc_data)
#                 convert_to_csv(handler.ohlc_data)

#             time.sleep(1)  # Keep the main thread alive

#     except KeyboardInterrupt:
#         print("Keyboard interrupt received. Shutting down...")
#         handler.stop_websockets()


import time
import logging
from fetch_pairs import WebSocketHandler
from data_operations import convert_to_csv, limit_symbol_instances
import pandas as pd
from kraken_pairs import kraken_pairs

# Configure logging
logging.basicConfig(filename="error_logs.log", 
                    level=logging.ERROR, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    num_pairs = len(kraken_pairs[0]) + len(kraken_pairs[1])
    print("Initializing WebSocketHandler...")
    handler = WebSocketHandler()
    
    try:
        handler.start_websockets()
        while True:
            if len(handler.ohlc_data) * 60 >= num_pairs:
                handler.ohlc_data = limit_symbol_instances(handler.ohlc_data)
                convert_to_csv(handler.ohlc_data)
                # print("updating the csv data")

            time.sleep(1)  # Keep the main thread alive

    except KeyboardInterrupt:
        print("Keyboard interrupt received. Shutting down...")
        handler.stop_websockets()

    except Exception as e:
        logging.error("An error occurred", exc_info=True)
