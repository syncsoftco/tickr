"""
tickr_client.py

A high-level API client for accessing cryptocurrency candle data stored in a GitHub repository.
The TickrClient class provides an easy interface to fetch the most recent candles or specify a date range.

This script can also be run as a standalone program to fetch and display candle data
using command-line flags.

License: MIT
"""

import datetime
import json
import pandas as pd
from typing import Optional
from github import Github, Repository
import base64
from absl import app
from absl import flags

# Define command-line flags
FLAGS = flags.FLAGS

flags.DEFINE_string('github_token', None, 'GitHub token for authentication.')
flags.DEFINE_string('repo_name', None, 'Name of the GitHub repository.')
flags.DEFINE_string('data_directory', 'data', 'Directory where data files are stored in the repo.')
flags.DEFINE_string('symbol', None, 'Symbol of the cryptocurrency, e.g., "BTC/USD".')
flags.DEFINE_integer('start_timestamp', None, 'Start timestamp in milliseconds since epoch.')
flags.DEFINE_integer('end_timestamp', None, 'End timestamp in milliseconds since epoch.')
flags.DEFINE_string('timeframe', '1m', 'Timeframe for candle data, e.g., "1m", "5m", "1h".')

# Mark required flags
flags.mark_flag_as_required('github_token')
flags.mark_flag_as_required('repo_name')
flags.mark_flag_as_required('symbol')
flags.mark_flag_as_required('start_timestamp')
flags.mark_flag_as_required('end_timestamp')


class TickrClient:
    """Client for fetching cryptocurrency candle data from a GitHub repository.

    The TickrClient class provides methods to fetch candle data for a specified
    cryptocurrency symbol and time range from data files stored in a GitHub repository.

    Attributes:
        github: An instance of the PyGithub Github class for GitHub API interaction.
        repo_name: The name of the GitHub repository containing the data.
        data_directory: The directory in the repository where data files are stored.
        symbol: The symbol of the cryptocurrency, e.g., "BTC/USD".
        repo: The Repository object representing the GitHub repository.
    """

    def __init__(self, github_token: str, repo_name: str, data_directory: str, symbol: str):
        """Initializes the TickrClient with GitHub authentication and repository details.

        Args:
            github_token: GitHub token for authentication.
            repo_name: Name of the GitHub repository.
            data_directory: Directory where data files are stored in the repo.
            symbol: Symbol of the cryptocurrency, e.g., "BTC/USD".
        """
        self.github = Github(github_token)
        self.repo_name = repo_name
        self.data_directory = data_directory
        self.symbol = symbol
        self.repo = self._get_repo()

    def _get_repo(self) -> Repository.Repository:
        """Retrieves the GitHub repository object.

        Returns:
            The Repository object corresponding to repo_name.

        Raises:
            ValueError: If the repository cannot be accessed.
        """
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
        """Fetches candle data between the specified timestamps.

        Args:
            start_timestamp: Start timestamp in milliseconds since epoch.
            end_timestamp: End timestamp in milliseconds since epoch.
            timeframe: Timeframe for candle data, e.g., '1m', '5m', '1h'.

        Returns:
            A pandas DataFrame containing the candle data.

        Raises:
            ValueError: If input parameters are invalid or data is insufficient.
        """
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
        # Convert timestamp to datetime and set as index
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
        """Resamples the candle data to a different timeframe.

        Args:
            df: DataFrame containing the original candle data.
            timeframe: The new timeframe to resample to.

        Returns:
            A pandas DataFrame with resampled candle data.

        Raises:
            ValueError: If the timeframe is unsupported.
        """
        # Map timeframe to pandas offset alias
        freq = self.timeframe_to_pandas_freq(timeframe)
        if freq is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        # Define how to aggregate the data
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'timestamp': 'first',  # Use the timestamp of the first candle in the resampled period
        }
        # Resample and aggregate the data
        resampled_df = df.resample(freq).agg(ohlc_dict).dropna()

        return resampled_df

    def timeframe_to_pandas_freq(self, timeframe: str) -> Optional[str]:
        """Converts a timeframe string to a pandas frequency string.

        Args:
            timeframe: Timeframe string, e.g., '1m', '5m', '1h'.

        Returns:
            The corresponding pandas frequency string, or None if unsupported.
        """
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


def main(argv):
    """Main method to fetch cryptocurrency candle data and print it.

    Parses command-line flags to create a TickrClient instance, fetches candle data,
    and prints the resulting DataFrame.

    Args:
        argv: Command-line arguments (unused).
    """
    del argv  # Unused.

    # Construct TickrClient instance
    client = TickrClient(
        github_token=FLAGS.github_token,
        repo_name=FLAGS.repo_name,
        data_directory=FLAGS.data_directory,
        symbol=FLAGS.symbol,
    )

    # Fetch candles
    try:
        df = client.get_candles(
            start_timestamp=FLAGS.start_timestamp,
            end_timestamp=FLAGS.end_timestamp,
            timeframe=FLAGS.timeframe,
        )
        print(df)
    except Exception as e:
        print(f"Error fetching candles: {e}")


if __name__ == '__main__':
    app.run(main)
