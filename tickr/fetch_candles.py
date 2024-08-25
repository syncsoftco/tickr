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
import subprocess
import pandas as pd
from absl import app, flags

# Configuration
FLAGS = flags.FLAGS

flags.DEFINE_string('exchange', 'kraken', 'Exchange ID (e.g., kraken, binance)')
flags.DEFINE_string('symbol', 'BTC/USD', 'Symbol to fetch data for (e.g., BTC/USD, ETH/USD)')
flags.DEFINE_string('timeframe', '1m', 'Timeframe (e.g., 1m, 5m, 1h, 1d)')
flags.DEFINE_string('data_dir', 'data', 'Directory to store the candle data')
flags.DEFINE_string('repo_name', 'syncsoftco/tickr', 'GitHub repository name')

def fetch_and_save_candles(exchange, symbol, timeframe, data_dir):
    if timeframe not in exchange.timeframes:
        raise ValueError(f"Unsupported timeframe: {timeframe}. Supported timeframes: {exchange.timeframes}")
    
    print(f"Fetching {timeframe} candles for {symbol} on {exchange.id}...")

    # Determine the directory and base filename
    base_filename = f"{exchange.id}_{symbol.replace('/', '-')}_{timeframe}"
    os.makedirs(os.path.join(data_dir, exchange.id), exist_ok=True)

    # Determine the last recorded candle's timestamp
    last_timestamp = None
    file_path = os.path.join(data_dir, exchange.id, f"{base_filename}.json")
    
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_candles = json.load(f)
            if existing_candles:
                last_timestamp = existing_candles[-1]['timestamp']

    if last_timestamp:
        print(f"Last recorded candle timestamp: {last_timestamp} (ms)")
    else:
        last_timestamp = exchange.parse8601('2021-01-01T00:00:00Z')
    
    # Fetch candles from the exchange
    since = last_timestamp + 1
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

        # Calculate filename based on the first candle's timestamp
        first_timestamp = df.index[0]
        year = first_timestamp.year
        month = first_timestamp.month
        file_path = os.path.join(data_dir, exchange.id, f"{base_filename}_{year}-{month:02d}.json")

        save_and_commit_changes(file_path, df)

def save_and_commit_changes(file_path, df):
    # Convert the DataFrame index (Timestamp) to milliseconds since epoch
    df.reset_index(inplace=True)
    df['timestamp'] = df['timestamp'].apply(lambda x: int(x.timestamp() * 1000))
    combined_candles = df.to_dict('records')

    # Write the data directly to the file
    with open(file_path, 'w') as f:
        json.dump(combined_candles, f, indent=4)

    # Check if there are any changes in the file using git diff
    result = subprocess.run(['git', 'diff', '--exit-code', file_path], capture_output=True)
    if result.returncode != 0:
        # There are changes, so commit and push
        subprocess.run(['git', 'add', file_path])
        subprocess.run(['git', 'commit', '-m', f"Update {os.path.basename(file_path)}"])
        subprocess.run(['git', 'push'])
        print(f"Changes detected and committed for {file_path}.")
    else:
        print(f"No changes detected for {file_path}. Skipping commit.")

def main(argv):
    exchange = getattr(ccxt, FLAGS.exchange)()
    symbol = FLAGS.symbol
    timeframe = FLAGS.timeframe
    data_dir = FLAGS.data_dir

    fetch_and_save_candles(exchange, symbol, timeframe, data_dir)

if __name__ == "__main__":
    app.run(main)
