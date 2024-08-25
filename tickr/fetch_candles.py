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
        raise ValueError(f"Unsupported timeframe: {timeframe}. Supported timeframes: {exchange.timeframes}")
    
    print(f"Fetching {timeframe} candles for {symbol} on {exchange.id}...")
    
    # Prepare the file path
    filename = f"{exchange.id}_{symbol.replace('/', '-')}_{timeframe}_{datetime.now().strftime('%Y-%m')}.json"
    file_path = os.path.join(data_dir, exchange.id, filename)

    # Check if the file already exists
    last_timestamp = None
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_candles = json.load(f)
        if existing_candles:
            last_timestamp = existing_candles[-1]['timestamp']

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

        save_and_update_github(file_path, df, symbol, timeframe, repo_name)

@backoff.on_exception(backoff.expo, GithubException, max_tries=7, giveup=lambda e: e.status != 409)
def save_and_update_github(file_path, df, symbol, timeframe, repo_name):
    combined_df = df

    # Convert the DataFrame index (Timestamp) to milliseconds since epoch
    combined_df.reset_index(inplace=True)
    combined_df['timestamp'] = combined_df['timestamp'].apply(lambda x: int(x.timestamp() * 1000))
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

        repo.update_file(contents.path, f"Update {symbol} {timeframe} candles for {datetime.now().strftime('%Y-%m')}", new_content, contents.sha)
        print(f"Updated {file_path} on GitHub.")
    except GithubException as e:
        if e.status == 404:
            with open(temp_file_path, 'r') as f:
                new_content = f.read()
            repo.create_file(file_path, f"Add {symbol} {timeframe} candles for {datetime.now().strftime('%Y-%m')}", new_content)
            print(f"Created {file_path} on GitHub.")
        else:
            raise

    # Save the new content locally
    os.rename(temp_file_path, file_path)

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
