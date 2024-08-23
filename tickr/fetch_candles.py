"""
fetch_candles.py

This script fetches the latest 1-minute cryptocurrency candle data using the CCXT library and resamples it
to generate candles for larger timeframes. It then updates the local data files in the repository.

License: MIT
"""

import ccxt
import json
import os
from datetime import datetime
from github import Github
import pandas as pd

# Configuration
EXCHANGE_ID = 'binance'
SYMBOLS = ['BTC/USDT']  # Add more symbols if needed
TIMEFRAMES = ['5m', '15m', '1h', '6h', '12h', '1d', '1w']
DATA_DIR = 'data'

# Initialize exchange
exchange = getattr(ccxt, EXCHANGE_ID)()

# Load your GitHub token
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
g = Github(GITHUB_TOKEN)
repo = g.get_repo('your_username/tickr')  # Replace 'your_username/tickr' with your actual repo path

def fetch_and_save_candles(symbol):
    """
    Fetches 1m candles from the exchange and resamples them to various timeframes.
    """
    print(f"Fetching 1m candles for {symbol}...")
    
    # Fetch 1-minute candles from the exchange
    since = exchange.parse8601('2021-01-01T00:00:00Z')  # Start date for fetching candles (adjust as needed)
    candles = exchange.fetch_ohlcv(symbol, '1m', since=since)

    # Convert to DataFrame for resampling
    df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)

    # Save and resample for each timeframe
    for timeframe in TIMEFRAMES:
        resampled = df.resample(timeframe).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Save and update data
        file_path = os.path.join(DATA_DIR, f"{symbol.replace('/', '-')}_{timeframe}.json")
        save_and_update_github(file_path, resampled, symbol, timeframe)

def save_and_update_github(file_path, resampled, symbol, timeframe):
    """
    Saves resampled candles to a file and updates the GitHub repository.
    """
    # Load existing data if file exists
    existing_candles = []
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_candles = json.load(f)
    
    # Combine existing and new candles, ensuring no duplicates
    existing_df = pd.DataFrame(existing_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    if not existing_df.empty:
        existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], unit='ms')
        existing_df.set_index('timestamp', inplace=True)
        combined_df = pd.concat([existing_df, resampled])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    else:
        combined_df = resampled

    # Save updated data back to the file
    combined_df.reset_index(inplace=True)
    combined_candles = combined_df.to_dict('records')
    with open(file_path, 'w') as f:
        json.dump(combined_candles, f)
    
    # Update the file in the GitHub repository
    update_github_file(file_path, symbol, timeframe)

def update_github_file(file_path, symbol, timeframe):
    """
    Updates the file in the GitHub repository with the latest candle data.
    """
    with open(file_path, 'r') as f:
        content = f.read()

    repo_file_path = f"data/{symbol.replace('/', '-')}_{timeframe}.json"
    try:
        # Update existing file
        contents = repo.get_contents(repo_file_path)
        repo.update_file(contents.path, f"Update {symbol} {timeframe} candles", content, contents.sha)
    except Exception as e:
        # Create new file if it doesn't exist
        repo.create_file(repo_file_path, f"Add {symbol} {timeframe} candles", content)
        print(f"Created new file for {symbol} {timeframe} candles in GitHub repo.")

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for symbol in SYMBOLS:
        fetch_and_save_candles(symbol)

if __name__ == "__main__":
    main()
