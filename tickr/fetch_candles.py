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
from datetime import datetime, timezone
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
    if not GITHUB_TOKEN:
        raise EnvironmentError("Please set the GITHUB_TOKEN environment variable.")
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(repo_name)
    return repo

def get_last_timestamp(shard_dir):
    last_timestamp = None
    if os.path.exists(shard_dir):
        json_files = []
        for root, dirs, files in os.walk(shard_dir):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    json_files.append(file_path)
        if json_files:
            # Sort files by their paths (assumes filenames are ordered by date)
            json_files.sort(reverse=True)
            for file_path in json_files:
                with open(file_path, 'r') as f:
                    existing_candles = json.load(f)
                if existing_candles:
                    file_last_timestamp = existing_candles[-1]['timestamp']
                    if last_timestamp is None or file_last_timestamp > last_timestamp:
                        last_timestamp = file_last_timestamp
                        break  # Found the latest timestamp
    return last_timestamp

def fetch_and_save_candles(exchange, symbol, timeframe, data_dir, repo_name):
    if timeframe not in exchange.timeframes:
        raise ValueError(f"Unsupported timeframe: {timeframe} Supported timeframes: {exchange.timeframes}")
    
    print(f"Fetching {timeframe} candles for {symbol} on {exchange.id}...")
     
    # Prepare directories for saving data
    shard_dir = os.path.join(data_dir, exchange.id, symbol.replace('/', '-'), timeframe)
    if not os.path.exists(shard_dir):
        os.makedirs(shard_dir)

    # Determine the last recorded candle's timestamp
    last_timestamp = get_last_timestamp(shard_dir)

    if last_timestamp:
        print(f"Last recorded candle timestamp: {last_timestamp} (ms)")

    # Fetch candles from the exchange
    since = last_timestamp + 1 if last_timestamp else exchange.parse8601('2021-01-01T00:00:00Z')
    candles = exchange.fetch_ohlcv(symbol, timeframe, since=since)

    if candles:
        # Remove the current candle (if any)
        current_time = int(datetime.utcnow().timestamp() * 1000)
        candles = [candle for candle in candles if candle[0] < current_time]

        if not candles:
            print("No new candles to record.")
            return

        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)

        for name, group in df.groupby([df.index.year, df.index.month]):
            year, month = name
            shard_filename = f"{exchange.id}_{symbol.replace('/', '-')}_{timeframe}_{year}-{month:02d}.json"
            file_path = os.path.join(
                data_dir,
                exchange.id,
                symbol.replace('/', '-'),
                timeframe,
                str(year),
                f"{month:02d}",
                shard_filename
            )

            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)

def extract_timestamp(x):
    if isinstance(x, int):
        return x
    return x.timestamp()

@backoff.on_exception(backoff.expo, GithubException, max_tries=7, giveup=lambda e: e.status != 409)
def save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name):
    # Convert the group DataFrame to a list of dicts
    new_candles_df = group
    new_candles_df.reset_index(inplace=True)
    new_candles_df['timestamp'] = new_candles_df['timestamp'].apply(lambda x: int(extract_timestamp(x) * 1000))
    new_candles = new_candles_df.to_dict('records')

    # Read existing candles from the local file if it exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_candles = json.load(f)
    else:
        existing_candles = []

    # Merge existing candles with new candles
    combined_candles = existing_candles + new_candles

    # Remove duplicates based on timestamp
    combined_candles = {candle['timestamp']: candle for candle in combined_candles}
    combined_candles = [combined_candles[timestamp] for timestamp in sorted(combined_candles)]

    # Convert combined candles to JSON string
    new_content = json.dumps(combined_candles, indent=4)

    repo = get_github_repo(repo_name)

    # Check if the file exists on GitHub and compare the content
    try:
        contents = repo.get_contents(file_path)
        existing_content = contents.decoded_content.decode('utf-8')

        # Load existing content as JSON
        existing_candles_remote = json.loads(existing_content)

        # If contents are the same, skip the update
        if existing_candles_remote == combined_candles:
            print(f"No changes detected for {file_path}. Skipping update.")
            return

        # Update the file on GitHub
        repo.update_file(
            contents.path,
            f"Update {symbol} {timeframe} candles for {year}-{month:02d}",
            new_content,
            contents.sha
        )
        print(f"Updated {file_path} on GitHub.")

    except GithubException as e:
        if e.status == 404:
            # File does not exist on GitHub; create it
            repo.create_file(
                file_path,
                f"Add {symbol} {timeframe} candles for {year}-{month:02d}",
                new_content
            )
            print(f"Created {file_path} on GitHub.")
        else:
            raise

    # Save the new content locally
    with open(file_path, 'w') as f:
        f.write(new_content)

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
