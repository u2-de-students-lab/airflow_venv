# airflow_venv
This is the simple airflow app which takes ticker data from API (https://www.yahoofinanceapi.com/) and load it into PostgreSQL

This app contains of 3 parts:
> Extract script which takes data from API lays in scripts package
> Transform script which find in API data useful for us information (in our case in takes info about Apple, Microsoft and Amazon tickers)
> Load scripts load data into PostgreSQL database

All this scripts are orchestrated by dag.py file, with which airflow working

