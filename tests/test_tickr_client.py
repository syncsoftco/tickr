"""
test_tickr_client.py

Unit tests for the TickrClient class in tickr/tickr_client.py.

License: MIT
"""

import unittest
from unittest.mock import patch, MagicMock
import base64
import pandas as pd
import datetime
import json
from tickr.tickr_client import TickrClient, main
from absl.testing import absltest
from absl import app


class TestTickrClient(unittest.TestCase):
    """Test suite for the TickrClient class."""

    def setUp(self):
        """Set up test fixtures."""
        # Arrange
        self.github_token = 'fake_token'
        self.repo_name = 'syncsoftco/tickr'  # Default repo name
        self.data_directory = 'data'
        self.exchange = 'binance'
        self.symbol = 'BTC/USD'
        self.prefix = f"{self.exchange}/{self.symbol.replace('/', '_')}_"

        # Create sample candle data
        self.sample_dates = [
            datetime.date(2023, 3, 1),
            datetime.date(2023, 3, 2),
        ]
        self.sample_files = {}
        for date in self.sample_dates:
            date_str = date.strftime('%Y-%m-%d')
            file_name = f"{self.data_directory}/{self.prefix}{date_str}.jsonl"
            candles = []
            timestamp_start = int(
                datetime.datetime.combine(
                    date, datetime.time.min, tzinfo=datetime.timezone.utc
                ).timestamp()
                * 1000
            )
            timestamp_end = (
                timestamp_start + 24 * 60 * 60 * 1000 - 60000
            )  # up to 23:59
            for ts in range(timestamp_start, timestamp_end + 1, 60000):
                candles.append([ts, 1.0, 1.0, 1.0, 1.0, 1.0])
            content = '\n'.join([json.dumps(c) for c in candles])
            self.sample_files[file_name] = base64.b64encode(
                content.encode('utf-8')
            ).decode('utf-8')

    @patch('tickr.tickr_client.Github')
    def test_start_timestamp_not_specified(self, mock_github):
        """Test that ValueError is raised when start_timestamp is None."""
        # Arrange
        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        start_timestamp = None
        end_timestamp = 1677715200000  # March 2, 2023 00:00:00 UTC in milliseconds

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            sut.get_candles(start_timestamp, end_timestamp)

        self.assertEqual(str(context.exception), "start_timestamp must be specified")

    @patch('tickr.tickr_client.Github')
    def test_end_timestamp_not_specified(self, mock_github):
        """Test that ValueError is raised when end_timestamp is None."""
        # Arrange
        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        start_timestamp = 1677628800000  # March 1, 2023 00:00:00 UTC
        end_timestamp = None

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            sut.get_candles(start_timestamp, end_timestamp)

        self.assertEqual(str(context.exception), "end_timestamp must be specified")

    @patch('tickr.tickr_client.Github')
    def test_data_insufficient_missing_file(self, mock_github):
        """Test that ValueError is raised when data files are missing."""
        # Arrange
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo

        # Simulate missing file for 2023-02-28
        def get_contents_side_effect(file_path):
            if '2023-02-28' in file_path:
                raise Exception("File not found")
            else:
                content_file = MagicMock()
                content_file.content = self.sample_files.get(file_path, '')
                if not content_file.content:
                    raise Exception("File not found")
                return content_file

        mock_repo.get_contents.side_effect = get_contents_side_effect

        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        # Request data from Feb 28, 2023, which we don't have
        start_timestamp = 1677542400000  # Feb 28, 2023 00:00:00 UTC
        end_timestamp = 1677628799000  # Feb 28, 2023 23:59:59 UTC

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            sut.get_candles(start_timestamp, end_timestamp)

        self.assertEqual(str(context.exception), "Data file for date 2023-02-28 is missing.")

    @patch('tickr.tickr_client.Github')
    def test_data_sufficient(self, mock_github):
        """Test that data is fetched correctly when sufficient data is available."""
        # Arrange
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo

        def get_contents_side_effect(file_path):
            content_file = MagicMock()
            content_file.content = self.sample_files.get(file_path, '')
            if not content_file.content:
                raise Exception("File not found")
            return content_file

        mock_repo.get_contents.side_effect = get_contents_side_effect

        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        start_timestamp = 1677628800000  # March 1, 2023 00:00:00 UTC
        end_timestamp = 1677715199000  # March 1, 2023 23:59:59 UTC

        # Act
        df = sut.get_candles(start_timestamp, end_timestamp)

        # Assert
        expected_rows = 1440  # 24 hours * 60 minutes
        self.assertEqual(len(df), expected_rows)

    @patch('tickr.tickr_client.Github')
    def test_resampling(self, mock_github):
        """Test that data is resampled correctly for different timeframes."""
        # Arrange
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo

        def get_contents_side_effect(file_path):
            content_file = MagicMock()
            content_file.content = self.sample_files.get(file_path, '')
            if not content_file.content:
                raise Exception("File not found")
            return content_file

        mock_repo.get_contents.side_effect = get_contents_side_effect

        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        start_timestamp = 1677628800000  # March 1, 2023 00:00:00 UTC
        end_timestamp = 1677715199000  # March 1, 2023 23:59:59 UTC

        # Act
        df = sut.get_candles(start_timestamp, end_timestamp, timeframe='1h')

        # Assert
        expected_rows = 24  # 24 hours
        self.assertEqual(len(df), expected_rows)

    @patch('tickr.tickr_client.Github')
    def test_missing_candles(self, mock_github):
        """Test that ValueError is raised when there are missing candles."""
        # Arrange
        # Simulate missing candles in the data
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo

        # Remove candles from March 1, 2023 12:00 to 12:59
        def get_contents_side_effect(file_path):
            content_file = MagicMock()
            decoded_content = base64.b64decode(self.sample_files.get(file_path, '')).decode('utf-8')
            lines = decoded_content.strip().split('\n')
            filtered_lines = []
            for line in lines:
                candle = json.loads(line)
                ts = candle[0]
                if not (1677672000000 <= ts <= 1677675599000):  # Exclude 12:00 to 12:59
                    filtered_lines.append(line)
            new_content = '\n'.join(filtered_lines)
            if not new_content:
                raise Exception("File not found")
            content_file.content = base64.b64encode(new_content.encode('utf-8')).decode('utf-8')
            return content_file

        mock_repo.get_contents.side_effect = get_contents_side_effect

        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        start_timestamp = 1677628800000  # March 1, 2023 00:00:00 UTC
        end_timestamp = 1677715199000  # March 1, 2023 23:59:59 UTC

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            sut.get_candles(start_timestamp, end_timestamp)

        self.assertEqual(
            str(context.exception),
            "Data is insufficient to cover the requested time range due to missing candles.",
        )

    @patch('tickr.tickr_client.Github')
    def test_unsupported_timeframe(self, mock_github):
        """Test that ValueError is raised for unsupported timeframes."""
        # Arrange
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo

        def get_contents_side_effect(file_path):
            content_file = MagicMock()
            content_file.content = self.sample_files.get(file_path, '')
            if not content_file.content:
                raise Exception("File not found")
            return content_file

        mock_repo.get_contents.side_effect = get_contents_side_effect

        sut = TickrClient(
            self.github_token, self.repo_name, self.data_directory, self.exchange, self.symbol
        )
        start_timestamp = 1677628800000  # March 1, 2023 00:00:00 UTC
        end_timestamp = 1677715199000  # March 1, 2023 23:59:59 UTC

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            sut.get_candles(start_timestamp, end_timestamp, timeframe='7m')

        self.assertEqual(str(context.exception), "Unsupported timeframe: 7m")


class TestTickrClientMain(absltest.TestCase):
    """Unit test for the main method in tickr_client.py."""

    @patch('tickr.tickr_client.TickrClient')
    @patch('sys.exit')
    def test_main_with_specified_timestamps(self, mock_sys_exit, mock_tickr_client):
        """Test the main method with specified timestamps."""
        # Arrange
        # Mock the TickrClient instance and its methods
        mock_client_instance = mock_tickr_client.return_value
        mock_client_instance.get_candles.return_value = pd.DataFrame()

        # Prepare argv with flags including start_timestamp and end_timestamp
        argv = [
            'tickr_client.py',
            '--github_token=fake_token',
            '--exchange_name=binance',
            '--trade_symbol=BTC/USD',
            '--start_timestamp=1677628800000',
            '--end_timestamp=1677715199000',
            '--timeframe=1m',
        ]

        # Act
        # Run the main function with mocked sys.exit to prevent exiting
        app.run(main, argv=argv)

        # Assert
        # Verify that TickrClient was called with correct parameters
        mock_tickr_client.assert_called_with(
            github_token='fake_token',
            repo_name='syncsoftco/tickr',
            data_directory='data',
            exchange='binance',
            symbol='BTC/USD',
        )

        # Verify that get_candles was called with specified timestamps
        mock_client_instance.get_candles.assert_called_with(
            start_timestamp=1677628800000,
            end_timestamp=1677715199000,
            timeframe='1m',
        )

if __name__ == '__main__':
    absltest.main()
