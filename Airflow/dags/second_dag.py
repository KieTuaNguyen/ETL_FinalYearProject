from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

from function import etl_process

def start_pipeline():
    print("Pipeline started")
        
def end_pipeline():
    print("Pipeline ended")
    
with DAG(
    dag_id="second_DAG",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2024, 1, 1)
    },
    catchup=False
) as f:
  
  start_job = PythonOperator(
    task_id="start_pipeline",
    python_callable=start_pipeline
  )
  
  etl_task = PythonOperator(
    task_id="etl_process",
    python_callable=etl_process
  )
  
  end_job = PythonOperator(
    task_id="end_pipeline",
    python_callable=end_pipeline
  )
  
  start_job >> etl_task >> end_job