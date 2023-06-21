# process_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.web_scraping.amazon_web_scraping.py import main as scrape_main

# Define a new DAG
dag = DAG(
    'amazon_processing',
    start_date=datetime(2023, 6, 21),
    schedule_interval='@every 3 days'
)

# Define a new PythonOperator task for web scraping
scrape_task = PythonOperator(
    task_id='scrape_amazon_data',
    python_callable=scrape_main,
    dag=dag,
)
