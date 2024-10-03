"""
tickr_client.py

A high-level API client for accessing cryptocurrency candle data stored in a GitHub repository.
The TickrClient class provides an easy interface to fetch the most recent candles or specify a date range.

License: MIT
"""

import datetime
import json
import pandas as pd
from typing import Optional, List
from github import Github, Repository, ContentFile
import base64
import io


class TickerClient:
    def __init__(self, github_token: str, repo_name: str, data_directory: str, symbol: str):
        self.github = Github(github_token)
        self.repo_name = repo_name
        self.data_directory = data_directory
        self.symbol = symbol
        self.repo = self._get_repo()

    def _get_repo(self) -> Repository.Repository:
        try:
            repo = self.github.get_repo(self.repo_name)
            return repo
        except Exception as e:
            raise ValueError(f"Unable to access repository '{self.repo_name}': {e}")

    def get_candles(
        self,
        start_timestamp: int,
        end_timestamp: int,
        timeframe: str = '1m'
    ) -> pd.DataFrame:
        if start_timestamp is None:
            raise ValueError("start_timestamp must be specified")
        if end_timestamp is None:
            raise ValueError("end_timestamp must be specified")
        if end_timestamp <= start_timestamp:
            raise ValueError("end_timestamp must be greater than start_timestamp")

        # Calculate start and end dates based on provided timestamps
        start_date = datetime.datetime.fromtimestamp(
            start_timestamp / 1000, tz=datetime.timezone.utc
        ).date()
        end_date = datetime.datetime.fromtimestamp(
            end_timestamp / 1000, tz=datetime.timezone.utc
        ).date()

        # Generate list of dates between start_date and end_date inclusive
        delta = end_date - start_date
        dates = [
            start_date + datetime.timedelta(days=i) for i in range(delta.days + 1)
        ]

        # Collect file paths
        prefix = f"{self.symbol.replace('/', '_')}_"
        file_paths = []
        for date in dates:
            date_str = date.strftime('%Y-%m-%d')
            file_path = f"{self.data_directory}/{prefix}{date_str}.jsonl"
            try:
                self.repo.get_contents(file_path)
                file_paths.append(file_path)
            except Exception:
                raise ValueError(f"Data file for date {date_str} is missing.")

        # Read candles from files
        candles = []
        for file_path in file_paths:
            file_content = self.repo.get_contents(file_path)
            decoded_content = base64.b64decode(file_content.content).decode('utf-8')
            lines = decoded_content.strip().split('\n')
            for line in lines:
                candle = json.loads(line)
                candles.append(candle)

        # Convert to DataFrame
        df = pd.DataFrame(
            candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('datetime', inplace=True)

        # Filter candles between start_timestamp and end_timestamp
        df = df[
            (df['timestamp'] >= start_timestamp) & (df['timestamp'] <= end_timestamp)
        ]

        if df.empty:
            raise ValueError("No data available in the specified time range.")

        # Check for missing data
        expected_timestamps = pd.date_range(
            start=datetime.datetime.fromtimestamp(
                start_timestamp / 1000, tz=datetime.timezone.utc
            ),
            end=datetime.datetime.fromtimestamp(
                end_timestamp / 1000, tz=datetime.timezone.utc
            ),
            freq='1min',
        )
        missing_timestamps = expected_timestamps.difference(df.index)

        if not missing_timestamps.empty:
            raise ValueError(
                "Data is insufficient to cover the requested time range due to missing candles."
            )

        # Resample data to the specified timeframe
        if timeframe != '1m':
            df = self.resample_candles(df, timeframe)

        return df

    def resample_candles(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        # Map timeframe to pandas offset alias
        freq = self.timeframe_to_pandas_freq(timeframe)
        if freq is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'timestamp': 'first',  # Use the timestamp of the first candle in the resampled period
        }
        resampled_df = df.resample(freq).agg(ohlc_dict).dropna()

        return resampled_df

    def timeframe_to_pandas_freq(self, timeframe: str) -> Optional[str]:
        # Map timeframe strings to pandas frequency strings
        mapping = {
            '1m': '1T',
            '3m': '3T',
            '5m': '5T',
            '15m': '15T',
            '30m': '30T',
            '1h': '1H',
            '2h': '2H',
            '4h': '4H',
            '1d': '1D',
        }
        return mapping.get(timeframe)
