"""
tickr_client.py

A high-level API client for accessing cryptocurrency candle data stored in a GitHub repository.
The TickrClient class provides an easy interface to fetch the most recent candles or specify a date range.

License: MIT
"""

from github import Github
import json
from datetime import datetime, timedelta
import time
import calendar

class TickrClient:
    def __init__(self, token=None):
        """
        Initializes the TickrClient with an optional GitHub token.
        
        :param token: (Optional) GitHub token for authenticated API requests.
        """
        self.github = Github(token)
        self.repo_name = "syncsoftco/tickr"
        self.repo = self.github.get_repo(self.repo_name)
    
    def get_candles(self, symbol, timeframe, start_date=None, end_date=None):
        """
        Fetches candle data for a given symbol and timeframe. If start_date and end_date are not provided,
        it defaults to fetching the most recent 100 candles.

        :param symbol: The trading symbol (e.g., 'BTC/USDT').
        :param timeframe: The timeframe (e.g., '1min', '5min', '1h', '1d').
        :param start_date: (Optional) The start date for the data (datetime or epoch timestamp).
        :param end_date: (Optional) The end date for the data (datetime or epoch timestamp).
        :return: A list of candles within the specified time range.
        """
        if timeframe == '1M':
            raise ValueError("1 month time frame is not supported. Maximum is 1 week.")

        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = self._calculate_default_start_date(end_date, timeframe)
        
        # Get list of file paths to check
        file_paths = self._get_sharded_file_paths(symbol, timeframe, start_date, end_date)
        candles = []

        # Load candles from each relevant file
        for file_path in file_paths:
            try:
                file_content = self.repo.get_contents(file_path)
                file_candles = json.loads(file_content.decoded_content.decode())
                candles.extend(file_candles)
            except Exception as e:
                print(f"Error fetching data from {file_path}: {e}")
                continue

        # Convert date range to timestamps
        start_timestamp = self._convert_to_timestamp(start_date)
        end_timestamp = self._convert_to_timestamp(end_date)

        # Filter candles within the specified range
        candles = [candle for candle in candles if self._is_within_range(candle[0], start_timestamp, end_timestamp)]

        return candles

    def _get_sharded_file_paths(self, symbol, timeframe, start_date, end_date):
        """
        Constructs the list of file paths that may contain data within the given date range.

        :param symbol: The trading symbol (e.g., 'BTC/USDT').
        :param timeframe: The timeframe (e.g., '1min', '5min', '1h', '1d').
        :param start_date: The start date for the data (datetime).
        :param end_date: The end date for the data (datetime).
        :return: A list of file paths to check in the repository.
        """
        file_paths = []
        current_date = start_date

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            file_name = f"{symbol.replace('/', '-')}_{timeframe}_{year}-{month:02d}.json"
            file_path = f"data/{symbol.replace('/', '-')}/{timeframe}/{year}/{month:02d}/{file_name}"
            file_paths.append(file_path)

            # Move to the next month
            next_month = month % 12 + 1
            next_year = year + (1 if next_month == 1 else 0)
            current_date = datetime(next_year, next_month, 1)

        return file_paths

    def _calculate_default_start_date(self, end_date, timeframe):
        """
        Calculates the default start date based on the given timeframe, aiming to fetch the last 100 candles.

        :param end_date: The end date for the data (datetime).
        :param timeframe: The timeframe (e.g., '1min', '5min', '1h', '1d').
        :return: The calculated start date (datetime).
        """
        timeframe_seconds = {
            '1min': 60,
            '5min': 300,
            '15min': 900,
            '1H': 3600,
            '6H': 21600,
            '12H': 43200,
            '1D': 86400,
            '1W': 604800,
        }

        if timeframe not in timeframe_seconds:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        time_delta = timedelta(seconds=timeframe_seconds[timeframe] * 100)
        return end_date - time_delta

    def _convert_to_timestamp(self, date):
        """
        Converts a datetime or epoch timestamp to milliseconds since the epoch.

        :param date: The date to convert (datetime or epoch timestamp).
        :return: The timestamp in milliseconds.
        """
        if isinstance(date, datetime):
            return int(time.mktime(date.timetuple()) * 1000)
        elif isinstance(date, (int, float)):
            return int(date * 1000 if date < 1e12 else date)
        else:
            raise ValueError("Unsupported date format. Please use a datetime object or an epoch timestamp.")

    def _is_within_range(self, timestamp, start, end):
        """
        Checks if a given timestamp is within the specified start and end range.

        :param timestamp: The timestamp to check (in milliseconds).
        :param start: The start timestamp (in milliseconds).
        :param end: The end timestamp (in milliseconds).
        :return: True if within range, False otherwise.
        """
        if start and timestamp < start:
            return False
        if end and timestamp > end:
            return False
        return True
