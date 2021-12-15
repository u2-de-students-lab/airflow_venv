import os
import unittest
from unittest.mock import Mock, patch

from scripts.extract import extract


class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.api_key = "RANDOM"
        os.environ["API_KEY"] = cls.api_key
        return super().setUpClass()

    def setUp(self) -> None:
        self.ticker = 'AAPL'
        self.return_json = {'quoteResponse': {
            'result': [{'k': 'v'}],
            'error': None}
        }
        self.get_method = "GET"
        self.api_url = "https://yfapi.net/v6/finance/quote"
        self.request_kwargs = {
            'headers': {'x-api-key': self.api_key},
            'params': {"symbols": self.ticker}
        }
        self.key = 'ticker_data'
        return super().setUp()

    @patch('scripts.extract.requests')
    def test_request_was_made(self, requests_mock) -> None:
        ti = Mock()

        response_mock = Mock()
        response_mock.json.return_value = self.return_json
        requests_mock.request.return_value = response_mock

        extract(self.ticker, ti=ti)
        requests_mock.request.assert_called_with(
            self.get_method,
            self.api_url,
            **self.request_kwargs
        )

    @patch('scripts.extract.requests')
    def test_xcom_push_value(self, requests_mock):
        ti = Mock()

        response_mock = Mock()
        response_mock.json.return_value = self.return_json
        requests_mock.request.return_value = response_mock

        extract(self.ticker, ti=ti)

        ticker_data = {}
        ticker_data[self.ticker] = self.return_json

        ti.xcom_push.assert_called_with(
            key=self.key,
            value=ticker_data
        )
