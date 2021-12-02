import datetime


def find_result_data(item: str, **kwargs):
    ti = kwargs['ti']
    data_in_json = ti.xcom_pull(key='ticker_data', task_ids=f'extract_{item}')
    print(data_in_json)
    data = dict()

    for k, v in data_in_json.items():
        for key, value in v.items():
            for element in value['result']:
                element_dict = dict()
                symbol_value = element['symbol']

                element_dict['symbol'] = element.get('symbol')
                element_dict['ask'] = element.get('ask')
                element_dict['bid'] = element.get('bid')
                element_dict['date'] = str(datetime.datetime.now())

                data[symbol_value] = element_dict

    ti.xcom_push(key='transformed_data', value=data)
