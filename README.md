# airflow_venv
This is the simple airflow app which takes ticker data from API (https://www.yahoofinanceapi.com/) and load it into PostgreSQL

This app contains of 3 parts:
>- Extract script which takes data from API lays in scripts package
>- Transform script which find in API data useful for us information (in our case in takes info about Apple, Microsoft and Amazon tickers)
>- Load scripts load data into PostgreSQL database

All this scripts are orchestrated by dag.py file, with which airflow working

Before you clone this repository, you nedd to know and do some things:
>- First of all, this is venv project for local usage, this means you have to create *.env* file 
   for your local variables
>- Before loading airflow web ui you should create enviroment veruable AIRFLOW_HOME in your virtual 
   envitoment and do this all the time when you againts activate venv
>- To communicate with your local postgresql in PostgresOperator you need to create anothe enviroment 
   veruable AIRFLOW_CONN_POSTGRES_DEFAULT=‘postgresql://postgres_user:password@host:port/databse’
>- Airflow needs a lot of envitoment variables in this app, but exporting them all the time very 
   uncomfortable, that's why to load everything from your *.env* file you need to use next bash script:
   set -a \ source .env \ set +a
 
