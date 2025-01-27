import requests
import json

def fetch_kraken_pairs():
    # Kraken REST API endpoint to fetch asset pairs
    url = "https://api.kraken.com/0/public/AssetPairs"
    response = requests.get(url)

    if response.status_code == 200:
        asset_pairs = response.json().get("result", {})

        # Extract and format pairs for WebSocket
        websocket_pairs = []
        for pair_name, pair_data in asset_pairs.items():
            # Use the `wsname` field for WebSocket-compatible pair format
            wsname = pair_data.get("wsname")
            if wsname:  # Only include pairs with WebSocket names
                websocket_pairs.append(wsname)

        return websocket_pairs
    else:
        print(f"Error fetching asset pairs: {response.status_code}, {response.text}")
        return []

# Get and save pairs to a file
kraken_pairs = fetch_kraken_pairs()
print(f"Found {len(kraken_pairs)} pairs:")
print(kraken_pairs)

# Save pairs to a Python file for reuse
with open("kraken_pairs.py", "w") as f:
    f.write(f"kraken_pairs = {json.dumps(kraken_pairs, indent=4)}")
