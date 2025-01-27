# Kraken OHLC Data Fetcher

This script fetches OHLC (Open, High, Low, Close) data for a specified cryptocurrency trading pair from Kraken's API in real-time. The data is updated at a user-defined interval and stored in memory for further analysis.

---

## Features

- Connects to Kraken's API using user-provided API keys.
- Retrieves OHLC data for the specified trading pair and interval.
- Handles API rate limits gracefully.
- Logs errors to a file (`error_log.txt`) for debugging.

---

## Requirements

- Python 3.8 or higher
- A Kraken API key and secret, setup instructions [here](https://support.kraken.com/hc/en-us/articles/360000919966-How-to-create-an-API-key)
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
## Usage

create a .env file and set the following:

```
api_key={your_api_key}
api_secret={your_api_secret}
```

pair: Put the name of the pair you want to trade (BTCUSD default)
interval: Specify the time interval you want to update the information on (measured in minutes)
error_log.txt: Use this to debug while the API is running
ohlc.csv: ohlc data (will exist after first run)

(Note: Kraken uses UTC for timestamps)
Have fun!
