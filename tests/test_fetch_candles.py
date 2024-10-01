import unittest
from unittest.mock import MagicMock, patch
import tempfile
import os
from tickr.fetch_candles import initialize_exchange, CandleFetcher
import datetime
import json

class TestFetchCandles(unittest.TestCase):

    @patch('tickr.fetch_candles.ccxt')
    def test_initialize_exchange_success(self, mock_ccxt):
        mock_exchange_class = MagicMock()
        mock_exchange = MagicMock()
        mock_exchange.has = {'fetchOHLCV': True}
        mock_exchange_class.return_value = mock_exchange
        setattr(mock_ccxt, 'binance', mock_exchange_class)

        exchange = initialize_exchange('binance')
        self.assertEqual(exchange, mock_exchange)

    def test_initialize_exchange_failure(self):
        with self.assertRaises(ValueError):
            initialize_exchange('nonexistent_exchange')

    @patch('tickr.fetch_candles.ccxt.Exchange')
    def test_candle_fetcher_fetch_and_save_candles(self, mock_exchange_class):
        # Set up mock exchange
        mock_exchange = MagicMock()
        mock_exchange.has = {'fetchOHLCV': True}
        # Mock fetch_ohlcv to return 1-minute candles
        mock_exchange.fetch_ohlcv = MagicMock(return_value=[
            [1609459200000, 29000, 29500, 28900, 29400, 100],  # Jan 1, 2021 00:00
            [1609459260000, 29400, 29600, 29300, 29500, 110],  # Jan 1, 2021 00:01
            [1609545600000, 29500, 30000, 29400, 29900, 150],  # Jan 2, 2021 00:00
        ])
        mock_exchange.parse_timeframe = MagicMock(return_value=1)
        mock_exchange.options = {'fetchOHLCVLimit': 1000}
        mock_exchange.milliseconds = MagicMock(return_value=1609549200000)
        mock_exchange.load_markets = MagicMock()
        mock_exchange_class.return_value = mock_exchange

        with tempfile.TemporaryDirectory() as data_directory:
            candle_fetcher = CandleFetcher(mock_exchange, 'BTC/USDT', data_directory)
            candle_fetcher.fetch_and_save_candles()

            # Check if files are created
            files = os.listdir(data_directory)
            self.assertTrue(any('2021-01-01' in file for file in files))
            self.assertTrue(any('2021-01-02' in file for file in files))

            # Verify content of one of the files
            file_path = os.path.join(data_directory, 'BTC_USDT_2021-01-01.json')
            with open(file_path, 'r') as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 2)  # Two candles for Jan 1, 2021
                # Parse the JSON and check content
                candle_data = [json.loads(line) for line in lines]
                self.assertEqual(candle_data[0][0], 1609459200000)

    def test_candle_fetcher_get_since_timestamp_no_files(self):
        mock_exchange = MagicMock()
        mock_exchange.parse_timeframe = MagicMock(return_value=1)
        mock_exchange.options = {'fetchOHLCVLimit': 1000}
        mock_exchange.milliseconds = MagicMock(return_value=1609549200000)

        with tempfile.TemporaryDirectory() as data_directory:
            candle_fetcher = CandleFetcher(mock_exchange, 'BTC/USDT', data_directory)
            since = candle_fetcher.get_since_timestamp()
            self.assertEqual(since, 0)

    def test_candle_fetcher_get_since_timestamp_with_existing_file(self):
        mock_exchange = MagicMock()

        with tempfile.TemporaryDirectory() as data_directory:
            # Create a mock file for Jan 1, 2021 with existing data
            file_name = os.path.join(data_directory, 'BTC_USDT_2021-01-01.json')
            existing_candle = [1609459200000, 29000, 29500, 28900, 29400, 100]
            with open(file_name, 'w') as f:
                f.write(json.dumps(existing_candle) + '\n')

            candle_fetcher = CandleFetcher(mock_exchange, 'BTC/USDT', data_directory)
            since = candle_fetcher.get_since_timestamp()
            expected_since = existing_candle[0] + 1
            self.assertEqual(since, expected_since)

    def test_candle_fetcher_get_since_timestamp_with_empty_file(self):
        mock_exchange = MagicMock()

        with tempfile.TemporaryDirectory() as data_directory:
            # Create an empty file for Jan 1, 2021
            file_name = os.path.join(data_directory, 'BTC_USDT_2021-01-01.json')
            with open(file_name, 'w') as f:
                pass  # Create an empty file

            candle_fetcher = CandleFetcher(mock_exchange, 'BTC/USDT', data_directory)
            since = candle_fetcher.get_since_timestamp()
            expected_since = int(datetime.datetime(2021, 1, 1).timestamp() * 1000)
            self.assertEqual(since, expected_since)

    def test_get_since_timestamp_with_multiple_symbols(self):
        mock_exchange = MagicMock()

        with tempfile.TemporaryDirectory() as data_directory:
            # Create files for different symbols
            with open(os.path.join(data_directory, 'BTC_USDT_2021-01-01.json'), 'w') as f:
                existing_candle = [1609459200000, 29000, 29500, 28900, 29400, 100]
                f.write(json.dumps(existing_candle) + '\n')
            with open(os.path.join(data_directory, 'ETH_USDT_2021-01-02.json'), 'w') as f:
                existing_candle_eth = [1609545600000, 730, 750, 720, 740, 200]
                f.write(json.dumps(existing_candle_eth) + '\n')

            candle_fetcher = CandleFetcher(mock_exchange, 'BTC/USDT', data_directory)
            since = candle_fetcher.get_since_timestamp()
            expected_since = existing_candle[0] + 1
            self.assertEqual(since, expected_since)

if __name__ == '__main__':
    unittest.main()
