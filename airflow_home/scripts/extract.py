import os
import requests
from dotenv import load_dotenv


API_URL = "https://yfapi.net/v6/finance/quote"


def extract(symbol: str, **kwargs):
    ti = kwargs['ti']

    load_dotenv()

    headers = {
        'x-api-key': os.environ.get('API_KEY')
    }

    ticker_data = dict()

    querystring = {"symbols": symbol}

    response = requests.request("GET", API_URL, headers=headers, params=querystring)

    ticker_data[symbol] = response.json()
    print(ticker_data)

    ti.xcom_push(key='ticker_data', value=ticker_data)
