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
import logging
from filelock import FileLock

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    return last_timestamp

def fetch_and_save_candles(exchange, symbol, timeframe, data_dir, repo_name):
    if timeframe not in exchange.timeframes:
        raise ValueError(f"Unsupported timeframe: {timeframe} Supported timeframes: {exchange.timeframes}")
    
    logger.info(f"Fetching {timeframe} candles for {symbol} on {exchange.id}...")
    
    # Prepare directories for saving data
    symbol_dir = symbol.replace('/', '-')
    shard_dir = os.path.join(data_dir, exchange.id, symbol_dir, timeframe)
    if not os.path.exists(shard_dir):
        os.makedirs(shard_dir)

    # Determine the last recorded candle's timestamp
    last_timestamp = get_last_timestamp(shard_dir)

    if last_timestamp:
        logger.info(f"Last recorded candle timestamp: {last_timestamp} (ms)")
    
    # Fetch candles from the exchange
    since = last_timestamp if last_timestamp else exchange.parse8601('2021-01-01T00:00:00Z')
    now = exchange.milliseconds()
    all_candles = []

    while since < now:
        try:
            logger.info(f"Fetching candles since {exchange.iso8601(since)}")
            candles = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
            if not candles:
                break
            all_candles.extend(candles)
            # Move 'since' forward to the timestamp of the last candle plus the timeframe
            since = candles[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            if len(candles) < 1000:
                break  # No more candles available
        except Exception as e:
            logger.error(f"Error fetching candles: {e}")
            break

    if all_candles:
        # Remove the current candle (if any)
        current_time = int(datetime.utcnow().timestamp() * 1000)
        all_candles = [candle for candle in all_candles if candle[0] < current_time]

        if not all_candles:
            logger.info("No new candles to record after removing current incomplete candle.")
            return

        df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)

        # Group candles by year and month
        df['year'] = df.index.year
        df['month'] = df.index.month

        for (year, month), group in df.groupby(['year', 'month']):
            shard_filename = f"{exchange.id}_{symbol_dir}_{timeframe}_{year}-{month:02d}.json"
            file_path = os.path.join(
                data_dir,
                exchange.id,
                symbol_dir,
                timeframe,
                str(year),
                f"{month:02d}",
                shard_filename
            )

            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name, exchange)

    else:
        logger.info("No new candles fetched.")

def extract_timestamp(x):
    if isinstance(x, int):
        return x
    return int(x.timestamp() * 1000)

@backoff.on_exception(backoff.expo, GithubException, max_tries=7, giveup=lambda e: e.status != 409)
def save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name, exchange):
    # Convert the group DataFrame to a list of dicts
    new_candles_df = group
    new_candles_df.reset_index(inplace=True)
    new_candles_df['timestamp'] = new_candles_df['timestamp'].apply(lambda x: extract_timestamp(x))
    new_candles = new_candles_df.to_dict('records')

    # Read existing candles from the local file if it exists
    if os.path.exists(file_path):
        with FileLock(f"{file_path}.lock"):
            with open(file_path, 'r') as f:
                existing_candles = json.load(f)
    else:
        existing_candles = []

    # Merge existing candles with new candles
    combined_candles = existing_candles + new_candles

    # Remove duplicates based on timestamp and sort
    combined_df = pd.DataFrame(combined_candles)
    combined_df.drop_duplicates(subset=['timestamp'], inplace=True)
    combined_df.sort_values(by='timestamp', inplace=True)
    combined_candles = combined_df.to_dict('records')

    # Validate candles
    validate_candles(combined_candles, timeframe, exchange)

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
            logger.info(f"No changes detected for {file_path}. Skipping update.")
            return

        # Update the file on GitHub
        repo.update_file(
            contents.path,
            f"Update {symbol} {timeframe} candles for {year}-{month:02d}",
            new_content,
            contents.sha
        )
        logger.info(f"Updated {file_path} on GitHub.")

    except GithubException as e:
        if e.status == 404:
            # File does not exist on GitHub; create it
            repo.create_file(
                file_path,
                f"Add {symbol} {timeframe} candles for {year}-{month:02d}",
                new_content
            )
            logger.info(f"Created {file_path} on GitHub.")
        else:
            raise

    # Save the new content locally
    with FileLock(f"{file_path}.lock"):
        with open(file_path, 'w') as f:
            f.write(new_content)

def validate_candles(candles, timeframe, exchange):
    expected_interval = exchange.parse_timeframe(timeframe) * 1000  # in milliseconds
    timestamps = [candle['timestamp'] for candle in candles]
    for i in range(1, len(timestamps)):
        delta = timestamps[i] - timestamps[i - 1]
        if delta != expected_interval:
            logger.warning(f"Gap or overlap detected between {timestamps[i - 1]} and {timestamps[i]}")
            # Decide whether to raise an exception or handle gaps
            # For now, we'll just log the warning

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
