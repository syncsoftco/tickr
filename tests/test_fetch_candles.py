import unittest
from unittest.mock import patch, MagicMock
from tickr.fetch_candles import fetch_and_save_candles

class TestFetchCandles(unittest.TestCase):

    @patch('tickr.fetch_candles.get_github_repo')
    @patch('tickr.fetch_candles.save_and_update_github')
    @patch('tickr.fetch_candles.ccxt.kraken')  # Mock the exchange setup
    def test_fetch_and_save_candles(self, mock_exchange_class, mock_save_and_update, mock_get_repo):
        # Setup mock return values
        mock_exchange = mock_exchange_class.return_value
        mock_exchange.id = 'kraken'  # Explicitly set the exchange ID
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # Mocked candle data
            [1609545600000, 29400, 29700, 29200, 29500, 1200]
        ]
        
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo

        # Call the function with required parameters
        fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')

        # Assertions
        mock_exchange.fetch_ohlcv.assert_called_once_with('BTC/USD', '1m', since=mock_exchange.parse8601('2021-01-01T00:00:00Z'))
        
        # Check that save_and_update_github was called
        expected_file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        mock_save_and_update.assert_called_once_with(expected_file_path, unittest.mock.ANY, 'BTC/USD', '1m', 2021, 1, 'syncsoftco/tickr')

if __name__ == '__main__':
    unittest.main()
