import unittest
from unittest.mock import patch, MagicMock
from tickr.fetch_candles import fetch_and_save_candles

class TestFetchCandles(unittest.TestCase):

    @patch('tickr.fetch_candles.exchange')
    @patch('tickr.fetch_candles.update_github_file')
    def test_fetch_and_save_candles(self, mock_update_github, mock_exchange):
        # Setup mock return values
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # Mocked candle data
            [1609545600000, 29400, 29700, 29200, 29500, 1200]
        ]
        mock_update_github.return_value = None

        # Run the function
        fetch_and_save_candles('BTC/USDT')

        # Assertions
        mock_exchange.fetch_ohlcv.assert_called_once_with('BTC/USDT', '1m', since=mock_exchange.parse8601('2021-01-01T00:00:00Z'))
        mock_update_github.assert_called()  # Check if the GitHub update function was called

if __name__ == '__main__':
    unittest.main()
