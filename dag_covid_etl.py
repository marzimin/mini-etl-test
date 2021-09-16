from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from covid_etl import run_covid_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 15), # hardcode the startdate (a past date)
    'email': ['dontwant@zoho.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covid_etl_dag',
    default_args=default_args,
    description='First DAG with ETL process',
    schedule_interval=timedelta(days=1), # run daily
)

def tester_function():
    print("Check to see if this works.")

run_etl = PythonOperator(
    task_id='daily_covid_etl',
    python_callable=tester_function, # add function here
    dag=dag,
)

run_etl