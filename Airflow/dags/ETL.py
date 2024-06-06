try:
    from airflow import DAG
    from datetime import timedelta, datetime
    from airflow.operators.python import PythonOperator
    from airflow.models.xcom_arg import XComArg
    
    print("Modules were imported successfully")
except Exception as e:
    print("Error {} ".format(e))
    
# Define default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 3, 1)
}

def extract_sub_category_id_func():
    return 0
def transform_data_func():
    return 0
def load_data_func():
    return 0
def extract_all_product_id_func():
    return 0
def extract_specify_product_id_func():
    return 0
def extract_product_data_func():
    return 0
def extract_feedback_data_func():
    return 0

# Define the DAG
with DAG(dag_id="ETL", 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False) as f:
    
    # Define Tasks
    extract_sub_category_id = PythonOperator(
        task_id='extract_sub_category_id',
        python_callable=extract_sub_category_id_func
    )

    transform_data_sub_category = PythonOperator(
        task_id='transform_data_sub_category',
        python_callable=transform_data_func
    )

    load_data_sub_category = PythonOperator(
        task_id='load_data_sub_category',
        python_callable=load_data_func
    )

    extract_all_product_id = PythonOperator(
        task_id='extract_all_product_id',
        python_callable=extract_all_product_id_func,
        op_kwargs={'sub_category_df': XComArg(extract_sub_category_id)}
    )

    transform_data_all_product = PythonOperator(
        task_id='transform_data_all_product',
        python_callable=transform_data_func
    )

    load_data_all_product = PythonOperator(
        task_id='load_data_all_product',
        python_callable=load_data_func
    )

    list_of_brands = ['Apple', 'HP', 'Asus', 'Samsung']
    
    # Complex tasks for each brand
    extract_specify_product_id_tasks = {}
    extract_product_data_tasks = {}
    extract_feedback_data_tasks = {}
    transform_data_product_tasks = {}
    load_data_product_tasks = {}
    transform_data_feedback_tasks = {}
    load_data_feedback_tasks = {}

    for brand in list_of_brands:
        extract_specify_product_id_task = PythonOperator(
            task_id=f'extract_{brand.lower()}_product_id',
            python_callable=extract_specify_product_id_func,
            op_kwargs={'product_ids_df': XComArg(extract_all_product_id), 'brands': [brand]}
        )
        extract_specify_product_id_tasks[brand] = extract_specify_product_id_task

        extract_product_data_task = PythonOperator(
            task_id=f'extract_{brand.lower()}_product_data',
            python_callable=extract_product_data_func,
            op_kwargs={'brand': brand}
        )
        extract_product_data_tasks[brand] = extract_product_data_task

        extract_feedback_data_task = PythonOperator(
            task_id=f'extract_{brand.lower()}_feedback_data',
            python_callable=extract_feedback_data_func,
            op_kwargs={'brand': brand}
        )
        extract_feedback_data_tasks[brand] = extract_feedback_data_task

        transform_data_product_task = PythonOperator(
            task_id=f'transform_data_product_{brand.lower()}',
            python_callable=transform_data_func
        )
        transform_data_product_tasks[brand] = transform_data_product_task

        load_data_product_task = PythonOperator(
            task_id=f'load_data_product_{brand.lower()}',
            python_callable=load_data_func
        )
        load_data_product_tasks[brand] = load_data_product_task

        transform_data_feedback_task = PythonOperator(
            task_id=f'transform_data_feedback_{brand.lower()}',
            python_callable=transform_data_func
        )
        transform_data_feedback_tasks[brand] = transform_data_feedback_task

        load_data_feedback_task = PythonOperator(
            task_id=f'load_data_feedback_{brand.lower()}',
            python_callable=load_data_func
        )
        load_data_feedback_tasks[brand] = load_data_feedback_task

    # Define the workflow
    extract_sub_category_id >> transform_data_sub_category >> load_data_sub_category
    extract_sub_category_id >> extract_all_product_id >> transform_data_all_product >> load_data_all_product

    for brand in list_of_brands:
        extract_all_product_id >> extract_specify_product_id_tasks[brand]
        extract_specify_product_id_tasks[brand] >> extract_product_data_tasks[brand]
        extract_product_data_tasks[brand] >> transform_data_product_tasks[brand] >> load_data_product_tasks[brand]
        extract_product_data_tasks[brand] >> extract_feedback_data_tasks[brand]
        extract_feedback_data_tasks[brand] >> transform_data_feedback_tasks[brand] >> load_data_feedback_tasks[brand]