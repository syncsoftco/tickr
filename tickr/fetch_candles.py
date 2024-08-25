"""
fetch_candles.py

This script fetches the latest cryptocurrency candle data using the CCXT library and updates
the local data files in the repository. It is intended to be run manually or via a scheduled GitHub Actions workflow.

License: MIT
"""

import backoff
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
    if timeframe not in exchange.timeframes:
        raise ValueError(f"Unsupported timeframe: {timeframe} Supported timeframes: {exchange.timeframes}")
    
    print(f"Fetching {timeframe} candles for {symbol} on {exchange.id}...")
     
    # Prepare directories for saving data
    shard_dir = os.path.join(data_dir, exchange.id, symbol.replace('/', '-'), timeframe)
    if not os.path.exists(shard_dir):
        os.makedirs(shard_dir)

    # Determine the last recorded candle's timestamp
    last_timestamp = None
    latest_file = None
    if os.path.exists(shard_dir):
        for root, dirs, files in os.walk(shard_dir):
            for file in sorted(files, reverse=True):
                if file.endswith('.json'):
                    latest_file = os.path.join(root, file)
                    with open(latest_file, 'r') as f:
                        existing_candles = json.load(f)
                    if existing_candles:
                        last_timestamp = existing_candles[-1]['timestamp']
                    break
            if last_timestamp:
                break

    if last_timestamp:
        print(f"Last recorded candle timestamp: {last_timestamp} (ms)")

    # Fetch candles from the exchange
    since = last_timestamp + 1 if last_timestamp else exchange.parse8601('2021-01-01T00:00:00Z')
    candles = exchange.fetch_ohlcv(symbol, timeframe, since=since)

    if candles:
        # Remove the current candle (if any)
        current_time = int(datetime.now().timestamp() * 1000)
        candles = [candle for candle in candles if candle[0] < current_time]

        if not candles:
            print("No new candles to record.")
            return

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

def extract_timestamp(x):
    if isinstance(x, int):
        return x

    return x.timestamp()

@backoff.on_exception(backoff.expo, GithubException, max_tries=3, giveup=lambda e: e.status != 409)
def save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name):
    combined_df = group

    # Convert the DataFrame index (Timestamp) to milliseconds since epoch
    combined_df.reset_index(inplace=True)
    combined_df['timestamp'] = combined_df['timestamp'].apply(lambda x: int(extract_timestamp(x) * 1000))
    combined_candles = combined_df.to_dict('records')

    # Save the data to a temporary location first
    temp_file_path = f"{file_path}.temp"
    with open(temp_file_path, 'w') as f:
        json.dump(combined_candles, f, indent=4)  # Added indent=4 for pretty printing

    repo = get_github_repo(repo_name)

    # Check if the file exists on GitHub and compare the content
    try:
        contents = repo.get_contents(file_path)
        existing_content = contents.decoded_content.decode('utf-8')

        with open(temp_file_path, 'r') as f:
            new_content = f.read()

        if existing_content == new_content:
            print(f"No changes detected for {file_path}. Skipping update.")
            os.remove(temp_file_path)
            return

        repo.update_file(contents.path, f"Update {symbol} {timeframe} candles for {year}-{month:02d}", new_content, contents.sha)
        print(f"Updated {file_path} on GitHub.")
    except GithubException as e:
        if e.status == 404:
            with open(temp_file_path, 'r') as f:
                new_content = f.read()
            repo.create_file(file_path, f"Add {symbol} {timeframe} candles for {year}-{month:02d}", new_content)
            print(f"Created {file_path} on GitHub.")
        else:
            raise

    # Save the new content locally
    os.rename(temp_file_path, file_path)

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
