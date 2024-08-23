"""
test_tickr_client.py

Unit tests for the TickrClient class in tickr/tickr_client.py.

License: MIT
"""

import unittest
from unittest.mock import patch, MagicMock
from tickr.tickr_client import TickrClient
from datetime import datetime, timedelta
import json

class TestTickrClient(unittest.TestCase):
    @patch('tickr.tickr_client.Github')
    def setUp(self, MockGithub):
        """
        Set up the TickrClient for testing with mocked GitHub API responses.
        """
        mock_repo = MockGithub().get_repo.return_value
        mock_file_content = MagicMock()
        # Use fixed timestamps that should clearly fall within any reasonable date range
        mock_file_content.decoded_content = json.dumps([
            [1609459200000, 29000, 29500, 28800, 29400, 1000],  # 2021-01-01 00:00:00 UTC
            [1609545600000, 29400, 29700, 29200, 29500, 1200]   # 2021-01-02 00:00:00 UTC
        ]).encode('utf-8')  # Ensure this is in bytes as expected from GitHub API
        mock_repo.get_contents.return_value = mock_file_content
        
        self.client = TickrClient()

    def test_default_date_range(self):
        """
        Test the default start and end date calculation for fetching the last 100 candles.
        """
        symbol = "BTC/USDT"
        timeframe = "1h"
        candles = self.client.get_candles(symbol, timeframe)
        
        print(f"Mocked candles: {candles}")

        self.assertIsNotNone(candles, "Candles should not be None")
        self.assertTrue(len(candles) > 0, "Candles should contain data")

    def test_custom_date_range(self):
        """
        Test fetching candles with a custom date range.
        """
        symbol = "BTC/USDT"
        timeframe = "1h"
        start_date = datetime(2021, 1, 1)
        end_date = datetime(2021, 1, 3)

        candles = self.client.get_candles(symbol, timeframe, start_date=start_date, end_date=end_date)
        
        print(f"Custom range candles: {candles}")

        self.assertIsNotNone(candles, "Candles should not be None")
        self.assertTrue(len(candles) > 0, "Candles should contain data")
        self.assertTrue(all(start_date <= datetime.fromtimestamp(candle[0] / 1000) <= end_date for candle in candles), 
                        "All candles should be within the specified date range")

    def test_unsupported_timeframe(self):
        """
        Test that an unsupported timeframe raises an appropriate error.
        """
        symbol = "BTC/USDT"
        timeframe = "1M"

        with self.assertRaises(ValueError) as context:
            self.client.get_candles(symbol, timeframe)

        self.assertTrue("1 month time frame is not supported" in str(context.exception))

if __name__ == "__main__":
    unittest.main()
