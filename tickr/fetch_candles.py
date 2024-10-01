"""
fetch_candles.py

This script fetches the latest cryptocurrency candle data using the CCXT library and updates
the local data files in the repository. It is intended to be run manually or via a scheduled GitHub Actions workflow.

License: MIT
"""

from absl import app, flags
import ccxt
import os
import datetime
import json
from typing import List

FLAGS = flags.FLAGS

flags.DEFINE_string('exchange', "kraken", 'Exchange name (e.g., binance)')
flags.DEFINE_string('symbol', "BTC/USD", 'Trading symbol (e.g., BTC/USDT)')
flags.DEFINE_string('data_directory', None, 'Directory to store data files')

def main(_):
    exchange_name = FLAGS.exchange
    symbol = FLAGS.symbol
    data_directory = FLAGS.data_directory

    # Initialize exchange
    exchange = initialize_exchange(exchange_name)

    # Ensure data directory exists
    os.makedirs(data_directory, exist_ok=True)

    # Create CandleFetcher instance
    candle_fetcher = CandleFetcher(exchange, symbol, data_directory)
    candle_fetcher.fetch_and_save_candles()

def initialize_exchange(exchange_name: str) -> ccxt.Exchange:
    try:
        return getattr(ccxt, exchange_name)
    except AttributeError:
        raise ValueError(f'Exchange "{exchange_name}" not found in ccxt library.')

class CandleFetcher:
    def __init__(self, exchange: ccxt.Exchange, symbol: str, data_directory: str, timeframe: str = '1m'):
        self.exchange = exchange
        self.symbol = symbol
        self.data_directory = data_directory
        self.timeframe = timeframe  # Using 1-minute candles
        self.limit = self.exchange.options.get('fetchOHLCVLimit', 1000)
        self.exchange.load_markets()

    def fetch_and_save_candles(self):
        since = self.get_since_timestamp()
        while True:
            candles = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, since, self.limit)
            if not candles:
                break
            self.process_candles(candles)
            since = candles[-1][0] + 1  # Move to next timestamp
            if len(candles) < self.limit:
                break

    def get_since_timestamp(self) -> int:
        # Filter files for the current symbol
        prefix = f"{self.symbol.replace('/', '_')}_"
        existing_files = sorted(
            f for f in os.listdir(self.data_directory) if f.startswith(prefix)
        )
        if not existing_files:
            # Start from the earliest timestamp available
            return 0

        latest_file = existing_files[-1]
        latest_file_path = os.path.join(self.data_directory, latest_file)
        # Read the last timestamp from the latest file
        with open(latest_file_path, 'r') as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1]
                last_candle = json.loads(last_line)
                return last_candle[0] + 1  # Continue from the last timestamp
            
            # Empty file, use the date from filename
            latest_date_str = latest_file[len(prefix):-5]  # Remove prefix and '.json'
            latest_date = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d')
            return int(latest_date.timestamp() * 1000)

    def process_candles(self, candles: List[List[int]]):
        daily_candles = {}
        for candle in candles:
            timestamp = candle[0]
            date_str = datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
            if date_str not in daily_candles:
                daily_candles[date_str] = []
            daily_candles[date_str].append(candle)

        for date_str, candles in daily_candles.items():
            file_name = f"{self.symbol.replace('/', '_')}_{date_str}.json"
            file_path = os.path.join(self.data_directory, file_name)
            with open(file_path, 'a') as f:
                for candle in candles:
                    f.write(json.dumps(candle) + '\n')

if __name__ == '__main__':
    app.run(main)
