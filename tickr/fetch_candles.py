"""
fetch_candles.py

This script fetches the latest cryptocurrency candle data using the CCXT library and updates
the local data files in the repository. It is intended to be run manually or via a scheduled GitHub Actions workflow.

License: MIT
"""

import ccxt
import json
import os
from datetime import datetime
from github import Github
from github import GithubException
import pandas as pd

# Configuration
EXCHANGE_ID = 'kraken'  # TODO: parameterize this
SYMBOLS = ['BTC/USD']  # TODO: Add more symbols or paramaterize if needed
TIMEFRAMES = ['1m', '5m', '15m', '1h', '6h', '12h', '1d', '1w']
DATA_DIR = 'data'

# Initialize exchange
exchange = getattr(ccxt, EXCHANGE_ID)()

def get_github_repo():
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo('syncsoftco/tickr')  # TODO: parameterize this
    return repo

def fetch_and_save_candles(symbol):
    print(f"Fetching 1m candles for {symbol} on {EXCHANGE_ID}...")

    try:
        # Fetch 1-minute candles from the exchange
        since = exchange.parse8601('2021-01-01T00:00:00Z')
        candles = exchange.fetch_ohlcv(symbol, '1m', since=since)

        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        for timeframe in TIMEFRAMES:
            resampled = df.resample(timeframe).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna(how='all')  # Drop rows where all elements are NaN
            
            if resampled.empty:
                continue  # Skip if the resampled DataFrame is empty

            for name, group in resampled.groupby([resampled.index.year, resampled.index.month]):
                year, month = name
                shard_filename = f"{EXCHANGE_ID}_{symbol.replace('/', '-')}_{timeframe}_{year}-{month:02d}.json"
                file_path = os.path.join(DATA_DIR, symbol.replace('/', '-'), timeframe, str(year), f"{month:02d}", shard_filename)

                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))
                
                save_and_update_github(file_path, group, symbol, timeframe, year, month)

    except ccxt.NetworkError as e:
        print(f"Network error: {e}")
    except ccxt.ExchangeError as e:
        print(f"Exchange error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def save_and_update_github(file_path, resampled, symbol, timeframe, year, month):
    # Load existing data if file exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_candles = json.load(f)
        existing_df = pd.DataFrame(existing_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], unit='ms')
        existing_df.set_index('timestamp', inplace=True)
        combined_df = pd.concat([existing_df, resampled])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    else:
        combined_df = resampled

    # Convert the DataFrame index (Timestamp) to milliseconds since epoch
    combined_df.reset_index(inplace=True)
    combined_df['timestamp'] = combined_df['timestamp'].apply(lambda x: int(x.timestamp() * 1000))
    combined_candles = combined_df.to_dict('records')

    # Save updated data back to the file
    with open(file_path, 'w') as f:
        json.dump(combined_candles, f)

    repo = get_github_repo()
    update_github_file(repo, file_path, symbol, timeframe, year, month)

def update_github_file(repo, file_path, symbol, timeframe, year, month):
    with open(file_path, 'r') as f:
        content = f.read()

    # Try to get the file contents to check if it exists
    try:
        contents = repo.get_contents(file_path)
        repo.update_file(contents.path, f"Update {symbol} {timeframe} candles for {year}-{month:02d}", content, contents.sha)
    except GithubException as e:
        if e.status != 404:
            raise

        # If the file does not exist, create it
        repo.create_file(file_path, f"Add {symbol} {timeframe} candles for {year}-{month:02d}", content)

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for symbol in SYMBOLS:
        fetch_and_save_candles(symbol)

if __name__ == "__main__":
    main()
