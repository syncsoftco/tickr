import unittest
from unittest.mock import patch, MagicMock
from tickr.fetch_candles import fetch_and_save_candles

class TestFetchCandles(unittest.TestCase):

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.save_and_update_github')
    @patch('tickr.fetch_candles.exchange')
    def test_fetch_and_save_candles(self, mock_exchange, mock_save_and_update, mock_get_repo):
        # Setup mock return values
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # Mocked candle data
            [1609545600000, 29400, 29700, 29200, 29500, 1200]
        ]
        
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo

        # Run the function
        fetch_and_save_candles('BTC/USDT')

        # Assertions
        mock_exchange.fetch_ohlcv.assert_called_once_with('BTC/USDT', '1m', since=mock_exchange.parse8601('2021-01-01T00:00:00Z'))
        mock_save_and_update.assert_called()

        # Check that the correct sharded file paths were used in the save_and_update_github call
        expected_file_path = 'data/BTC-USDT/1min/2021/01/BTC-USDT_1min_2021-01.json'
        called_file_path = mock_save_and_update.call_args[0][0]  # First positional argument is the file path
        self.assertEqual(expected_file_path, called_file_path)

if __name__ == '__main__':
    unittest.main()
