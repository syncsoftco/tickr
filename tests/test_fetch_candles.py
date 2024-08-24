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
        mock_exchange.timeframes = {'1m': '1 minute', '1h': '1 hour'}  # Mock supported timeframes
        mock_exchange.fetch_ohlcv.return_value = [
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # Mocked candle data
            [1609545600000, 29400, 29700, 29200, 29500, 1200]
        ]
        
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo

        # Test with a supported timeframe
        fetch_and_save_candles(mock_exchange, 'BTC/USD', '1m', 'data', 'syncsoftco/tickr')

        # Assertions for supported timeframe
        mock_exchange.fetch_ohlcv.assert_called_once_with('BTC/USD', '1m', since=1724528100001)
        
        # Check that save_and_update_github was called
        expected_file_path = 'data/kraken/BTC-USD/1m/2021/01/kraken_BTC-USD_1m_2021-01.json'
        mock_save_and_update.assert_called_once_with(expected_file_path, unittest.mock.ANY, 'BTC/USD', '1m', 2021, 1, 'syncsoftco/tickr')

    def test_fetch_candles_unsupported_timeframe(self):
        # Setup a mock exchange with limited timeframes
        mock_exchange = MagicMock()
        mock_exchange.id = 'kraken'
        mock_exchange.timeframes = {'1m': '1 minute', '1h': '1 hour'}

        # Attempt to call with an unsupported timeframe
        with self.assertRaises(ValueError) as context:
            fetch_and_save_candles(mock_exchange, 'BTC/USD', '6h', 'data', 'syncsoftco/tickr')

        # Verify the error message
        self.assertIn("Unsupported timeframe", str(context.exception))
        self.assertIn("6h", str(context.exception))
        self.assertIn("Supported timeframes", str(context.exception))

if __name__ == '__main__':
    unittest.main()
