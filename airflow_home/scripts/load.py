import os

import psycopg2

from dotenv import load_dotenv


def data_load(item: str, **kwargs) -> None:
    ti = kwargs['ti']
    ticker_data = ti.xcom_pull(key='transformed_data', task_ids=f'transform_{item}')

    print(ticker_data)

    load_dotenv()

    db = os.environ.get('DATABASE')
    user = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('PORT')

    conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)

    cursor = conn.cursor()

    sql = """create table if not exists ticker_info (TICKER character varying (4) not null,\
             ASK money not null, BID money not null, DATETIME_GATHERED timestamp not null) ;"""

    cursor.execute(sql)

    for key, value in ticker_data.items():
        values = ', '.join([f'{float(t)}' if isinstance(t, float) else f"'{t}'" for t in value.values()])
        insert = f'insert into ticker_info (TICKER, ASK, BID, DATETIME_GATHERED) values ({values})'
        cursor.execute(insert)

    conn.commit()
    conn.close()
