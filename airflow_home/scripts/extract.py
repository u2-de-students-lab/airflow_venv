import os
import requests
import yaml

API_URL = "https://yfapi.net/v6/finance/quote"


def extract() -> dict:
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    headers = {
        'x-api-key': os.environ.get('API_KEY')
    }

    ticker_data = dict()

    for item in config['symbols']:
        querystring = {"symbols": item}

        response = requests.request("GET", API_URL, headers=headers, params=querystring)

        ticker_data[item] = response.json()

    return ticker_data
