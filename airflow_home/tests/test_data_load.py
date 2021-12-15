import os
import unittest
from unittest.mock import Mock, patch
from datetime import datetime

from scripts.load import data_load


class TestDataLoad(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ticker = 'AAPL'
        cls.db = 'RANDOM_DB'
        cls.user = 'RANDOM_USER'
        cls.password = 'RANDOM_PASSWORD'
        cls.host = 'RANDOM_HOST'
        cls.port = '1111'
        os.environ['DATABASE'] = cls.db
        os.environ['USER'] = cls.user
        os.environ['PASSWORD'] = cls.password
        os.environ['HOST'] = cls.host
        os.environ['PORT'] = cls.port

    def setUp(self):
        self.key = 'transformed_data'
        self.task_ids = f'transform_{self.ticker}'
        self.time_now = datetime.strptime('2021-12-07', '%Y-%m-%d')
        self.income_data = {
            self.ticker: {
                'symbol': self.ticker,
                'ask': 2.2,
                'bid': 3.3,
                'date': str(self.time_now)
            }
        }
        self.values = [self.ticker, 2.2, 3.3, '2021-12-07 00:00:00']
        self.data = ', '.join(
            [f'{float(t)}'
             if isinstance(t, float) else f"'{t}'"
             for t in self.values]
        )

    def test_xcom_pull(self):
        ti = Mock()
        with patch('scripts.load.psycopg2.connect') as _:
            ti.xcom_pull.return_value = self.income_data
            data_load(self.ticker, ti=ti)

        ti.xcom_pull.assert_called_with(
            key=self.key,
            task_ids=self.task_ids
        )

    @patch('scripts.load.psycopg2')
    def test_connection_to_db(self, psycopg_mock):
        ti = Mock()
        psycopg_mock.connect = Mock()

        ti.xcom_pull.return_value = self.income_data

        data_load(self.ticker, ti=ti)
        psycopg_mock.connect.assert_called_with(
            database=self.db,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def test_execute_insert_string(self):
        ti = Mock()
        expected_query = 'insert into ticker_info (TICKER, ASK, BID, ' \
            f'DATETIME_GATHERED) values ({self.data})'

        ti.xcom_pull.return_value = self.income_data

        with patch('scripts.load.psycopg2.connect') as conn:
            connection = Mock()
            cursor = Mock()

            conn.return_value = connection
            connection.cursor.return_value = cursor

            data_load(item=self.ticker, ti=ti)

            cursor.execute.assert_called_with(expected_query)

    def test_connection_closed(self):
        ti = Mock()
        ti.xcom_pull.return_value = self.income_data

        with patch('scripts.load.psycopg2.connect') as conn:
            conn.return_value = Mock()
            data_load(self.ticker, ti=ti)
            conn.return_value.close.assert_called()

    def test_connection_commited(self):
        ti = Mock()
        ti.xcom_pull.return_value = self.income_data

        with patch('scripts.load.psycopg2.connect') as conn:
            conn.return_value = Mock()
            data_load(self.ticker, ti=ti)
            conn.return_value.commit.assert_called()
