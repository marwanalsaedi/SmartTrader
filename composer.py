from datetime import timedelta
import os
import airflow
from airflow.decorators import dag, task

START_DATE = airflow.utils.dates.days_ago(1)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "##########")

default_args = {
    'owner': 'airflow',
}
@dag(default_args=default_args, schedule_interval="@daily", start_date=START_DATE, tags=['daily'])


def fetch_crypto():
    @task()
    def update_data(symbol,date="2017-01-01"):
        import requests
        address="https://###########.a.run.app/"
        
        j={'symbol':symbol,'start_date':date}

        r = requests.post(address, json=j,headers={"TOKEN": "hunter2"},)
        print(r.status_code)
        print(r.text)

    [update_data("BINANCE:BTCUSDT"),update_data("BINANCE:ETHUSDT")]
    

taskflowfetch_crypto_dag = fetch_crypto()





