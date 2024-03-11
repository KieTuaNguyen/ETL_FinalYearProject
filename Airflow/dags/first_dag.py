try:
  from datetime import timedelta, datetime
  from airflow import DAG
  from airflow.operators.python import PythonOperator
  
  print("All Dag modules are ok ......")
except Exception as e:
  print("Error  {} ".format(e))


def first_function(**context):
  print("First function executed")
  context['ti'].xcom_push(key='mykey', value="First function execution done")

def second_function(**context):
  instance = context['ti'].xcom_pull(key='mykey')
  print("This is the second function being executed, which is using the value {} from the first function: ".format(instance))

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
    python_callable = first_function,
    provide_context=True,
    op_kwargs={"name":"Kiet Nguyen"},
  )
  
  second_function_execute = PythonOperator(
    task_id = "second_function_execute",
    python_callable = second_function,
    provide_context=True
  )
    
first_function_execute >> second_function_execute