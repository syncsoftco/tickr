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
from absl import app, flags

# Configuration
FLAGS = flags.FLAGS

flags.DEFINE_string('exchange', 'kraken', 'Exchange ID (e.g., kraken, binance)')
flags.DEFINE_string('symbol', 'BTC/USD', 'Symbol to fetch data for (e.g., BTC/USD, ETH/USD)')
flags.DEFINE_string('timeframe', '1m', 'Timeframe (e.g., 1m, 5m, 1h, 1d)')
flags.DEFINE_string('data_dir', 'data', 'Directory to store the candle data')
flags.DEFINE_string('repo_name', 'syncsoftco/tickr', 'GitHub repository name')

def get_github_repo(repo_name):
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(repo_name)
    return repo

def fetch_and_save_candles(exchange, symbol, timeframe, data_dir, repo_name):
    print(f"Fetching {timeframe} candles for {symbol} on {exchange.id}...")

    try:
        # Fetch candles from the exchange
        since = exchange.parse8601('2021-01-01T00:00:00Z')
        candles = exchange.fetch_ohlcv(symbol, timeframe, since=since)

        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        for name, group in df.groupby([df.index.year, df.index.month]):
            year, month = name
            shard_filename = f"{exchange.id}_{symbol.replace('/', '-')}_{timeframe}_{year}-{month:02d}.json"
            file_path = os.path.join(data_dir, exchange.id, symbol.replace('/', '-'), timeframe, str(year), f"{month:02d}", shard_filename)

            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)

    except ccxt.NetworkError as e:
        print(f"Network error: {e}")
    except ccxt.ExchangeError as e:
        print(f"Exchange error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name):
    # Load existing data if file exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_candles = json.load(f)
        existing_df = pd.DataFrame(existing_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], unit='ms')
        existing_df.set_index('timestamp', inplace=True)
        combined_df = pd.concat([existing_df, group])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    else:
        combined_df = group

    # Convert the DataFrame index (Timestamp) to milliseconds since epoch
    combined_df.reset_index(inplace=True)
    combined_df['timestamp'] = combined_df['timestamp'].apply(lambda x: int(x.timestamp() * 1000))
    combined_candles = combined_df.to_dict('records')

    # Save updated data back to the file with pretty printing
    with open(file_path, 'w') as f:
        json.dump(combined_candles, f, indent=4)  # Added indent=4 for pretty printing

    repo = get_github_repo(repo_name)
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

def main(argv):
    exchange = getattr(ccxt, FLAGS.exchange)()
    symbol = FLAGS.symbol
    timeframe = FLAGS.timeframe
    data_dir = FLAGS.data_dir
    repo_name = FLAGS.repo_name

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    fetch_and_save_candles(exchange, symbol, timeframe, data_dir, repo_name)

if __name__ == "__main__":
    app.run(main)
