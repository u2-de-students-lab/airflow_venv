import os
import sys
import yaml

from datetime import datetime, timedelta


from airflow import DAG

from airflow.operators.python import PythonOperator
sys.path.append('/home/aleh/work/airflow_2/airflow_venv/airflow_home')
from scripts.extract import extract
from scripts.transform import find_result_data
from scripts.load import data_load

#


with open('/home/aleh/work/airflow_2/airflow_venv/airflow_home/dags/configuration/config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

default_args = {
    'depends_on_past': False
}

with DAG(
        'tickers_to_postgres',
        default_args=default_args,
        description='Tickers from API loads into PostgreSQL',
        schedule_interval=timedelta(hours=2),
        start_date=datetime(2021, 12, 2, 0, 0, 0),
        catchup=False
) as dag:
    for item in config['symbols']:
        task_1 = PythonOperator(
            task_id=f'extract_{item}',
            python_callable=extract,
            provide_context=True,
            op_args=(item, ),
        )

        task_2 = PythonOperator(
            task_id=f'transform_{item}',
            python_callable=find_result_data,
            provide_context=True,
            op_args=(item, ),
        )

        task_3 = PythonOperator(
            task_id=f'load_{item}',
            python_callable=data_load,
            provide_context=True,
            op_args=(item, ),
        )

        task_1 >> task_2 >> task_3
