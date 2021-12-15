import datetime


def find_result_data(item: str, **kwargs) -> None:
    ti = kwargs['ti']  # need for xcom push and pull commands
    ticker_info_from_api = ti.xcom_pull(
        key='ticker_data', task_ids=f'extract_{item}'
    )
    needed_data = dict()

    for element in ticker_info_from_api[item]['quoteResponse']['result']:
        ticker_data = dict()
        symbol_value = element['symbol']

        ticker_data['symbol'] = symbol_value
        ticker_data['ask'] = element.get('ask')
        ticker_data['bid'] = element.get('bid')
        ticker_data['date'] = str(datetime.datetime.now())

        needed_data[symbol_value] = ticker_data

    ti.xcom_push(key='transformed_data', value=needed_data)
