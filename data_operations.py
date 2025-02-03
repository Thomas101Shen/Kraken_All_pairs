import os
import re
import pandas as pd

def convert_to_csv(df, save_path="./crypto_data"):
    """
    Converts a dataframe to CSV files, organized by symbol.
    Creates a directory if it does not exist.
    Removes rows that are not needed
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

def limit_symbol_instances(df, max_instances=60):
    """
    Ensures that the DataFrame contains at most 'max_instances' per symbol.
    Keeps the most recent instances based on the timestamp.
    
    Args:
    - df (pd.DataFrame): Input DataFrame containing a 'symbol' column.
    - max_instances (int): Maximum number of rows to keep per symbol.

    Returns:
    - pd.DataFrame: Cleaned DataFrame with no more than 'max_instances' per symbol.
    """
    df = df.sort_values(by=["symbol", "timestamp"], ascending=[True, False])  # Sort by symbol, then timestamp (latest first)
    
    # Use .groupby() and head() to keep the latest `max_instances` per symbol
    df_limited = df.groupby("symbol").head(max_instances).reset_index(drop=True)

    return df_limited
