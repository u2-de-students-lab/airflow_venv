from datetime import datetime
import unittest
from unittest.mock import Mock, patch

from scripts.transform import find_result_data


class TestTransformation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ticker = 'AAPL'
        cls.income_dict = {
            'item': {
                'quoteResponse': {
                    'result': [{
                        'symbol': cls.ticker,
                        'ask': 2.2,
                        'bid': 3.3
                        }],
                    'error': None
                }
            }
        }

    def setUp(self):
        self.key = 'ticker_data'
        self.push_key = 'transformed_data'
        self.task_ids = f'extract_{self.ticker}'
        self.time_now = datetime.strptime('2021-12-07', '%Y-%m-%d')
        self.transformed_data = {
                'symbol': self.ticker,
                'ask': 2.2,
                'bid': 3.3,
                'date': str(self.time_now)
        }

    def test_data_transformation(self):
        ti = Mock()

        ti.xcom_pull.return_value = self.income_dict
        find_result_data(self.ticker, ti=ti)

        ti.xcom_pull.assert_called_with(
            key=self.key,
            task_ids=self.task_ids
        )

    @patch('scripts.transform.datetime.datetime')
    def test_xcom_push_transsformed_data(self, datetime_mock):
        ti = Mock()

        datetime_mock.now.return_value = self.time_now
        ti.xcom_pull.return_value = self.income_dict

        find_result_data(self.ticker, ti=ti)

        data = {}
        data[self.ticker] = self.transformed_data

        ti.xcom_push.assert_called_with(
            key=self.push_key,
            value=data
        )
