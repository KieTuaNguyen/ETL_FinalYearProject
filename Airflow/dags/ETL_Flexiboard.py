try:
  from airflow import DAG
  from datetime import timedelta, datetime
  from airflow.operators.python import PythonOperator
  
  print("Modules were imported successfully")
except Exception as e:
  print("Error {} ".format(e))
  
# Define necessary functions
def extract_sub_category_id_func():
  print("Starting the ETL process")
  print("This is task 1")
def extract_all_product_id_func():
  print("This is task 2")
def extract_specify_product_id_func():
  print("This is task 3")
def extract_product_data_func():
  print("This is task 4")
def extract_feedback_data_func():
  print("This is task 5")
def transform_df_to_dataframes_func():
  print("This is task 6")
def load_data_func():
  print("This is task 7")
  print("ETL process completed")

# Define default arguments
default_args = {
  "owner": "airflow",
  "retries": 1,
  "retry_delay": timedelta(minutes=5),
  "start_date": datetime(2024, 5, 1)
}

# Define the DAG
with DAG(dag_id="ETL_Flexiboard", 
         default_args=default_args, 
         schedule_interval="@daily", 
         catchup=False) as f:
  
  # Define Tasks
  # Task 1: Extract sub-category IDs
  extract_sub_category_id = PythonOperator(
    task_id='extract_sub_category_id',
    python_callable=extract_sub_category_id_func
  )
  
  # Task 2: Extract all products IDs
  extract_all_product_id = PythonOperator(
    task_id='extract_all_product_id',
    python_callable=extract_all_product_id_func
  )
  
  # Task 3: List of necessary brands
  list_of_brands = ['Apple', 'HP', 'Asus', 'Samsung']
  
  # Complex tasks for each brand
  extract_specify_product_id_tasks = []
  extract_product_data_tasks = []
  extract_feedback_data_tasks = []

  for brand in list_of_brands:
    # Task 4: Extract specific product IDs for each brand
    extract_specify_product_id_task = PythonOperator(
        task_id=f'extract_{brand.lower()}_product_id',
        python_callable=extract_specify_product_id_func,
        op_kwargs={'brand_name': brand}
    )
    extract_specify_product_id_tasks.append(extract_specify_product_id_task)
    
    # Task 5: Extract product data for each brand
    extract_product_data_task = PythonOperator(
        task_id=f'extract_{brand.lower()}_product_data',
        python_callable=extract_product_data_func,
        op_kwargs={'brand_name': brand}
    )
    extract_product_data_tasks.append(extract_product_data_task)

    # Task 6: Extract feedback data for each brand
    extract_feedback_data_task = PythonOperator(
        task_id=f'extract_{brand.lower()}_feedback_data',
        python_callable=extract_feedback_data_func,
        op_kwargs={'brand_name': brand}
    )
    extract_feedback_data_tasks.append(extract_feedback_data_task)
      
  # Task7: Transform data into dataframes
  transform_df_to_dataframes = PythonOperator(
      task_id='transform_df_to_dataframes',
      python_callable=transform_df_to_dataframes_func
  )

  # Task 8: Load data
  load_data = PythonOperator(
      task_id='load_data',
      python_callable=load_data_func
  )
  
  # Define the workflow
  extract_sub_category_id >> extract_all_product_id
  extract_all_product_id >> extract_specify_product_id_tasks

  for task in extract_specify_product_id_tasks:
      task >> extract_product_data_tasks[extract_specify_product_id_tasks.index(task)]

  for task in extract_product_data_tasks:
      task >> extract_feedback_data_tasks[extract_product_data_tasks.index(task)]

  for task in extract_feedback_data_tasks:
      task >> transform_df_to_dataframes

  transform_df_to_dataframes >> load_data