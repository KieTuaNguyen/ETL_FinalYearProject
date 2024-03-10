try:
  from datetime import timedelta, datetime
  from airflow import DAG
  from airflow.operators.python import PythonOperator
  
  print("All Dag modules are ok ......")
except Exception as e:
  print("Error  {} ".format(e))


def first_function():
  print("Hello world!")
  return "Hello world!"


with DAG(
  dag_id = "first_dag",
  schedule_interval = "@daily",
  default_args={
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1)
  },
  catchup= False
) as f:
  first_function_execute = PythonOperator(
    task_id = "first_function_execute",
    python_callable = first_function
  )