# Kraken OHLC Data Fetcher

This script fetches OHLC (Open, High, Low, Close) data for a all cryptocurrency trading pairs from Kraken's API in real-time. The data is updated at a user-defined interval and stored in memory for further analysis.

---

## Features

- Connects to Kraken's API using Kraken Websocket.
- Retrieves OHLC data for the specified trading pair and interval.
- Handles API rate limits gracefully.
- Logs errors to a file (`error_log.txt`) for debugging. (Only errors in main.py errors in the websocket connection still need some work to log)

---

## Requirements

- Python 3.8 or higher
(Note all the dependencies are included in the requirements.txt file)

---

## Setup

1. Clone this repository or copy the script to your local environment.
2. Install the required Python packages:
   `pip install -r requirements.txt`
It is reccomended to use a venv
```
Python -m venv ./venv
source ./venv/bin/activate
pip install -r requirements.txt
```

(Note: Kraken uses UTC for timestamps)
Have fun!
