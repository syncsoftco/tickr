import unittest
from unittest.mock import patch, MagicMock
from tickr.fetch_candles import fetch_and_save_candles

class TestFetchCandles(unittest.TestCase):

    @patch('tickr.fetch_candles.exchange')
    @patch('tickr.fetch_candles.repo')
    def test_fetch_and_save_candles(self, mock_repo, mock_exchange):
        # Setup mock return values
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # Mocked candle data
            [1609545600000, 29400, 29700, 29200, 29500, 1200]
        ]
        
        mock_repo.get_contents.return_value = MagicMock()
        mock_repo.update_file.return_value = MagicMock()
        mock_repo.create_file.return_value = MagicMock()

        # Run the function
        fetch_and_save_candles('BTC/USDT')

        # Assertions
        mock_exchange.fetch_ohlcv.assert_called_once_with('BTC/USDT', '1m', since=mock_exchange.parse8601('2021-01-01T00:00:00Z'))
        mock_repo.get_contents.assert_called()
        mock_repo.update_file.assert_called()

if __name__ == '__main__':
    unittest.main()
