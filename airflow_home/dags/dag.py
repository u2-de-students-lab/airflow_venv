import os
import sys
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from typing import Dict, List

#  Need to make scripts package visible
sys.path.append(os.environ.get('AIRFLOW_HOME'))

from scripts.extract import extract
from scripts.load import data_load
from scripts.transform import find_result_data


def load_config_from_yaml(file_path: str) -> Dict[str, List[str]]:
    with open(file_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    return config


default_args = {
    'depends_on_past': False
}


def create_etl_tasks(
    dag: DAG,
    ticker: str,
    task_before: PostgresOperator
) -> None:
    task_1 = PythonOperator(
        task_id=f'extract_{ticker}',
        python_callable=extract,
        provide_context=True,
        op_args=(ticker, ),
    )

    task_2 = PythonOperator(
        task_id=f'transform_{ticker}',
        python_callable=find_result_data,
        provide_context=True,
        op_args=(ticker, ),
    )

    task_3 = PythonOperator(
        task_id=f'load_{ticker}',
        python_callable=data_load,
        provide_context=True,
        op_args=(ticker, ),
    )

    task_before.set_downstream(task_1)
    task_1.set_downstream(task_2)
    task_2.set_downstream(task_3)


with DAG(
    'tickers_to_postgres',
    default_args=default_args,
    description='Tickers from API loads into PostgreSQL',
    schedule_interval=timedelta(hours=2),
    start_date=datetime(2021, 12, 2, 0, 0, 0),
    catchup=False
) as dag:
    task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""create table if not exists ticker_info (TICKER character \
               varying (4) not null, ASK money not null, BID money not null, \
               DATETIME_GATHERED timestamp not null) ;""",
    )

    config_data = load_config_from_yaml(os.environ.get('CONFIG_PATH'))

    for ticker in config_data['symbols']:
        create_etl_tasks(dag=dag, ticker=ticker, task_before=task)
