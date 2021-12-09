import os

import psycopg2
from dotenv import load_dotenv


def data_load(item: str, **kwargs) -> None:
    ti = kwargs['ti']  # need for xcom push and pull commands
    ticker_data = ti.xcom_pull(
        key='transformed_data',
        task_ids=f'transform_{item}'
    )

    load_dotenv()

    db = os.environ.get('DATABASE')
    user = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('PORT')

    conn = psycopg2.connect(database=db, user=user, password=password,
                            host=host, port=port)

    cursor = conn.cursor()

    for key, value in ticker_data.items():
        values = ', '.join(
            [f'{float(t)}'
             if isinstance(t, float)
             else f"'{t}'"
             for t in value.values()]
        )
        insert = 'insert into ticker_info (TICKER, ASK, BID, ' \
            + f'DATETIME_GATHERED) values ({values})'
        cursor.execute(insert)

    conn.commit()
    conn.close()
