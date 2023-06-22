from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.web_scraping.amazon_web_scarping import amazon_scrape_main
from scripts.spark.process_amazon_data import process_data_main
from scripts.spark.query_parquet import main as query_main

# Define the default arguments.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['phutom0412panda@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG('amazon_scraper_dag',
          default_args=default_args,
          description='An Amazon Scraper DAG',
          schedule_interval=timedelta(days=3),
          start_date=datetime(2023, 6, 22),
          catchup=False)

# Define the tasks. These call the appropriate Python functions.
t1 = PythonOperator(
    task_id='amazon_scrape',
    python_callable=amazon_scrape_main,
    dag=dag)

t2 = PythonOperator(
    task_id='process_data',
    python_callable=process_data_main,
    dag=dag)

t3 = PythonOperator(
    task_id='query_data',
    python_callable=query_main,
    dag=dag)

# Define the task dependencies.
t1 >> t2 >> t3
