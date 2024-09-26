import unittest
from unittest.mock import patch, MagicMock, mock_open, ANY
import pandas as pd
import json
from datetime import datetime, timezone
from tickr.fetch_candles import (
    fetch_and_save_candles,
    get_last_timestamp,
    save_and_update_github,
    get_github_repo
)

class TestFetchCandles(unittest.TestCase):

    # Helper method to create a mock exchange
    def create_mock_exchange(self, id='kraken', timeframes=None):
        mock_exchange = MagicMock()
        mock_exchange.id = id
        mock_exchange.timeframes = timeframes or {'1m': '1 minute', '1h': '1 hour'}
        return mock_exchange

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.save_and_update_github')
    @patch('tickr.fetch_candles.ccxt.kraken')
    def test_fetch_and_save_candles_with_new_data(self, mock_exchange_class, mock_save_and_update, mock_get_repo):
        """Test fetching and saving candles when new data is available."""
        # Arrange
        mock_exchange = mock_exchange_class.return_value
        mock_exchange.id = 'kraken'
        mock_exchange.timeframes = {'1m': '1 minute'}
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],
            [1609545600000, 29400, 29700, 29200, 29500, 1200]
        ]
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo

        # Act
        fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')

        # Assert
        mock_save_and_update.assert_called_once()
        args, kwargs = mock_save_and_update.call_args
        self.assertIn('kraken_BTC-USD_1m_2021-01.json', args[0])

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.save_and_update_github')
    @patch('tickr.fetch_candles.ccxt.kraken')
    def test_fetch_and_save_candles_no_new_data(self, mock_exchange_class, mock_save_and_update, mock_get_repo):
        """Test fetching and saving candles when no new data is available."""
        # Arrange
        mock_exchange = mock_exchange_class.return_value
        mock_exchange.id = 'kraken'
        mock_exchange.timeframes = {'1m': '1 minute'}
        mock_exchange.fetch_ohlcv.return_value = []  # No new candles
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo

        # Act
        fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')

        # Assert
        mock_save_and_update.assert_not_called()

    def test_fetch_candles_unsupported_timeframe(self):
        """Test that an unsupported timeframe raises a ValueError."""
        # Arrange
        mock_exchange = self.create_mock_exchange()
        # Act & Assert
        with self.assertRaises(ValueError) as context:
            fetch_and_save_candles(mock_exchange, 'BTC/USD', '6h', 'data', 'syncsoftco/tickr')
        self.assertIn("Unsupported timeframe", str(context.exception))

    @patch('os.path.exists', return_value=False)
    def test_get_last_timestamp_no_files(self, mock_exists):
        """Test get_last_timestamp when no files are present."""
        # Arrange
        shard_dir = 'data/kraken/BTC-USD/1m'
        # Act
        last_timestamp = get_last_timestamp(shard_dir)
        # Assert
        self.assertIsNone(last_timestamp)

    @patch('os.path.exists', return_value=True)
    @patch('os.walk')
    @patch('builtins.open', new_callable=mock_open, read_data='[{"timestamp": 1609459200000}]')
    def test_get_last_timestamp_with_files(self, mock_file, mock_walk, mock_exists):
        """Test get_last_timestamp when files are present."""
        # Arrange
        mock_walk.return_value = [
            ('data/kraken/BTC-USD/1m/2021/01', [], ['file1.json']),
        ]
        shard_dir = 'data/kraken/BTC-USD/1m'
        # Act
        last_timestamp = get_last_timestamp(shard_dir)
        # Assert
        self.assertEqual(last_timestamp, 1609459200000)

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('os.path.exists', return_value=False)
    @patch('builtins.open', new_callable=mock_open)
    def test_save_and_update_github_file_creation(self, mock_open_file, mock_exists, mock_get_repo):
        """Test that save_and_update_github creates a new file when none exists."""
        # Arrange
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        group = pd.DataFrame([{
            'timestamp': pd.Timestamp('2021-01-01T00:00:00Z', tz=timezone.utc),
            'open': 29000,
            'high': 29500,
            'low': 28800,
            'close': 29400,
            'volume': 1000
        }])
        symbol = 'BTC/USD'
        timeframe = '1m'
        year = 2021
        month = 1
        repo_name = 'syncsoftco/tickr'
        # Act
        with patch('json.dumps', return_value='{}'):
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)
        # Assert
        mock_repo.create_file.assert_called_once()
        args, kwargs = mock_repo.create_file.call_args
        self.assertIn(file_path, args)

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open, read_data='[]')
    def test_save_and_update_github_file_update(self, mock_open_file, mock_exists, mock_get_repo):
        """Test that save_and_update_github updates the file when it exists with new data."""
        # Arrange
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_contents = MagicMock()
        mock_contents.decoded_content = b'[]'
        mock_repo.get_contents.return_value = mock_contents

        file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        group = pd.DataFrame([{
            'timestamp': pd.Timestamp('2021-01-02T00:00:00Z', tz=timezone.utc),
            'open': 29500,
            'high': 30000,
            'low': 29400,
            'close': 29900,
            'volume': 1100
        }])
        symbol = 'BTC/USD'
        timeframe = '1m'
        year = 2021
        month = 1
        repo_name = 'syncsoftco/tickr'
        # Act
        with patch('json.dumps', return_value='[{"timestamp":1609545600000}]'):
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)
        # Assert
        mock_repo.update_file.assert_called_once()
        args, kwargs = mock_repo.update_file.call_args
        self.assertIn(file_path, args)

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open, read_data='[{"timestamp":1609459200000}]')
    def test_save_and_update_github_no_changes(self, mock_open_file, mock_exists, mock_get_repo):
        """Test that save_and_update_github does not update the file when there are no changes."""
        # Arrange
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_contents = MagicMock()
        mock_contents.decoded_content = b'[{"timestamp":1609459200000}]'
        mock_repo.get_contents.return_value = mock_contents

        file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        group = pd.DataFrame([{
            'timestamp': pd.Timestamp('2021-01-01T00:00:00Z', tz=timezone.utc),
            'open': 29000,
            'high': 29500,
            'low': 28800,
            'close': 29400,
            'volume': 1000
        }])
        symbol = 'BTC/USD'
        timeframe = '1m'
        year = 2021
        month = 1
        repo_name = 'syncsoftco/tickr'
        # Act
        with patch('json.dumps', return_value='[{"timestamp":1609459200000}]'):
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)
        # Assert
        mock_repo.update_file.assert_not_called()
        mock_repo.create_file.assert_not_called()

    def test_extract_timestamp_with_int(self):
        """Test extract_timestamp with integer input."""
        # Arrange
        from tickr.fetch_candles import extract_timestamp
        timestamp_int = 1609459200000
        # Act
        result = extract_timestamp(timestamp_int)
        # Assert
        self.assertEqual(result, 1609459200000)

    def test_extract_timestamp_with_datetime(self):
        """Test extract_timestamp with datetime input."""
        # Arrange
        from tickr.fetch_candles import extract_timestamp
        timestamp_dt = datetime(2021, 1, 1, tzinfo=timezone.utc)
        # Act
        result = extract_timestamp(timestamp_dt)
        # Assert
        self.assertEqual(result, timestamp_dt.timestamp())

    @patch('tickr.fetch_candles.os.getenv', return_value=None)
    def test_get_github_repo_no_token(self, mock_getenv):
        """Test get_github_repo raises EnvironmentError when GITHUB_TOKEN is not set."""
        # Arrange
        # Act & Assert
        with self.assertRaises(EnvironmentError):
            get_github_repo('syncsoftco/tickr')

    @patch('tickr.fetch_candles.Github')
    @patch('tickr.fetch_candles.os.getenv', return_value='fake_token')
    def test_get_github_repo_success(self, mock_getenv, mock_github):
        """Test get_github_repo returns a repository object when token is set."""
        # Arrange
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo
        # Act
        repo = get_github_repo('syncsoftco/tickr')
        # Assert
        self.assertEqual(repo, mock_repo)

    @patch('tickr.fetch_candles.datetime')
    def test_fetch_and_save_candles_current_time_handling(self, mock_datetime):
        """Test that current time is handled correctly to exclude current candle."""
        # Arrange
        mock_datetime.utcnow.return_value = datetime(2021, 1, 3, tzinfo=timezone.utc)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        mock_exchange = self.create_mock_exchange()
        mock_exchange.fetch_ohlcv.return_value = [
            [1609632000000, 30000, 30500, 29900, 30400, 1300],  # 2021-01-03 00:00:00
            [1609718400000, 30400, 31000, 30300, 30900, 1400]   # 2021-01-04 00:00:00 (current time)
        ]
        # Act
        with patch('tickr.fetch_candles.save_and_update_github'):
            fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')
        # Assert
        mock_exchange.fetch_ohlcv.assert_called_once()
        args, kwargs = mock_exchange.fetch_ohlcv.call_args
        candles = mock_exchange.fetch_ohlcv.return_value
        # Ensure that only past candles are processed
        self.assertLess(candles[-1][0], int(datetime.utcnow().timestamp() * 1000))

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open, read_data='[{"timestamp":1609459200000}]')
    def test_merge_candles_remove_duplicates(self, mock_open_file, mock_exists, mock_get_repo):
        """Test that duplicate candles are removed when merging."""
        # Arrange
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_contents = MagicMock()
        mock_contents.decoded_content = b'[{"timestamp":1609459200000,"open":29000}]'
        mock_repo.get_contents.return_value = mock_contents

        file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        group = pd.DataFrame([
            {
                'timestamp': pd.Timestamp('2021-01-01T00:00:00Z', tz=timezone.utc),
                'open': 29000,
                'high': 29500,
                'low': 28800,
                'close': 29400,
                'volume': 1000
            },
            {
                'timestamp': pd.Timestamp('2021-01-02T00:00:00Z', tz=timezone.utc),
                'open': 29500,
                'high': 30000,
                'low': 29400,
                'close': 29900,
                'volume': 1100
            }
        ])
        symbol = 'BTC/USD'
        timeframe = '1m'
        year = 2021
        month = 1
        repo_name = 'syncsoftco/tickr'

        # Act
        with patch('json.dumps') as mock_json_dumps:
            mock_json_dumps.return_value = '[{"timestamp":1609459200000,"open":29000},{"timestamp":1609545600000,"open":29500}]'
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)

        # Assert
        mock_repo.update_file.assert_called_once()
        args, kwargs = mock_repo.update_file.call_args
        updated_content = args[2]
        # Ensure that there are no duplicate timestamps
        candles = json.loads(updated_content)
        timestamps = [candle['timestamp'] for candle in candles]
        self.assertEqual(len(timestamps), len(set(timestamps)))

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open, read_data='[{"timestamp":1609459200000}]')
    def test_handle_github_exception(self, mock_open_file, mock_exists, mock_get_repo):
        """Test that a GithubException is raised when an unexpected error occurs."""
        # Arrange
        from github import GithubException
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_repo.get_contents.side_effect = GithubException(500, 'Server Error')
        file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        group = pd.DataFrame([{
            'timestamp': pd.Timestamp('2021-01-01T00:00:00Z', tz=timezone.utc),
            'open': 29000,
            'high': 29500,
            'low': 28800,
            'close': 29400,
            'volume': 1000
        }])
        symbol = 'BTC/USD'
        timeframe = '1m'
        year = 2021
        month = 1
        repo_name = 'syncsoftco/tickr'
        # Act & Assert
        with self.assertRaises(GithubException):
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open, read_data='[]')
    def test_save_and_update_github_handles_empty_existing_file(self, mock_open_file, mock_exists, mock_get_repo):
        """Test that save_and_update_github handles empty existing files correctly."""
        # Arrange
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_contents = MagicMock()
        mock_contents.decoded_content = b'[]'  # Empty existing content
        mock_repo.get_contents.return_value = mock_contents

        file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        group = pd.DataFrame([{
            'timestamp': pd.Timestamp('2021-01-02T00:00:00Z', tz=timezone.utc),
            'open': 29500,
            'high': 30000,
            'low': 29400,
            'close': 29900,
            'volume': 1100
        }])
        symbol = 'BTC/USD'
        timeframe = '1m'
        year = 2021
        month = 1
        repo_name = 'syncsoftco/tickr'

        # Act
        with patch('json.dumps', return_value='[{"timestamp":1609545600000}]'):
            save_and_update_github(file_path, group, symbol, timeframe, year, month, repo_name)

        # Assert
        mock_repo.update_file.assert_called_once()
        args, kwargs = mock_repo.update_file.call_args
        updated_content = args[2]
        self.assertIn('1609545600000', updated_content)

    @patch('tickr.fetch_candles.ccxt.kraken')
    def test_fetch_and_save_candles_since_parameter(self, mock_exchange_class):
        """Test that fetch_ohlcv is called with correct 'since' parameter."""
        # Arrange
        mock_exchange = mock_exchange_class.return_value
        mock_exchange.id = 'kraken'
        mock_exchange.timeframes = {'1m': '1 minute'}
        mock_exchange.parse8601.return_value = 1609459200000  # 2021-01-01T00:00:00Z
        mock_exchange.fetch_ohlcv.return_value = []

        # Mock get_last_timestamp to return a specific timestamp
        with patch('tickr.fetch_candles.get_last_timestamp', return_value=1609545600000):
            # Act
            with patch('tickr.fetch_candles.save_and_update_github'):
                fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')

        # Assert
        expected_since = 1609545600001  # last_timestamp + 1
        mock_exchange.fetch_ohlcv.assert_called_once_with('BTC/USD', '1m', since=expected_since)

    @patch('tickr.fetch_candles.datetime')
    def test_fetch_and_save_candles_timezone_aware(self, mock_datetime):
        """Test that timestamps are correctly handled with timezones."""
        # Arrange
        mock_datetime.utcnow.return_value = datetime(2021, 1, 2, tzinfo=timezone.utc)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        mock_exchange = self.create_mock_exchange()
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # 2021-01-01 00:00:00 UTC
            [1609545600000, 29400, 29700, 29200, 29500, 1200]   # 2021-01-02 00:00:00 UTC
        ]
        # Act
        with patch('tickr.fetch_candles.save_and_update_github'):
            fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')
        # Assert
        mock_exchange.fetch_ohlcv.assert_called_once()
        df_call_args = mock_exchange.fetch_ohlcv.return_value
        timestamps = [candle[0] for candle in df_call_args]
        # Ensure that timestamps are timezone-aware and in UTC
        self.assertTrue(all(datetime.utcfromtimestamp(ts / 1000).tzinfo is None for ts in timestamps))

    # Add more test cases as needed to cover other aspects

if __name__ == '__main__':
    unittest.main()
