import os

import requests
from dotenv import load_dotenv


API_URL = "https://yfapi.net/v6/finance/quote"


def extract(symbol: str, **kwargs) -> None:
    ti = kwargs['ti']  # need for xcom push and pull commands

    load_dotenv()

    headers = {
        'x-api-key': os.environ.get('API_KEY')
    }

    ticker_data = dict()

    querystring = {"symbols": symbol}

    response = requests.request("GET", API_URL, headers=headers,
                                params=querystring)

    ticker_data[symbol] = response.json()

    ti.xcom_push(key='ticker_data', value=ticker_data)
