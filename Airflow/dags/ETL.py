try:
    from airflow import DAG
    from datetime import timedelta, datetime
    from airflow.operators.python import PythonOperator
    from airflow.models.xcom_arg import XComArg
    
    print("Modules were imported successfully")
except Exception as e:
    print("Error {} ".format(e))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
from datetime import datetime, timedelta, date  
from dependencies import *
from configs.config_manager import get_config
# Set up header
HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
  "Accept-Language": 'en-US,en;q=0.9',
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Referer": "https://tiki.vn/",
  "From": "",
  "af-ac-enc-dat": "",
  "x-api-source": "pc"
}
# Set up GroupID
GroupID = ['1846', '1789']

# Define default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 3, 1)
}

# Default functions
def remove_single_category(df):
  category_counts = df.groupby('MasterCategoryID')['CategoryID'].nunique()
  single_category_mask = category_counts == 1
  df.loc[df['MasterCategoryID'].isin(single_category_mask[single_category_mask].index), ['CategoryID', 'CategoryName']] = [None, None]
  return df

def transform_category(row):
  if pd.isna(row['CategoryID']):
    master_category_id = row['MasterCategoryID']
    master_category_name = row['MasterCategoryName']
    category_id = row['MasterCategoryID']
    category_name = row['MasterCategoryName']
    is_category = 0
  else:
    master_category_id = row['MasterCategoryID']
    master_category_name = row['MasterCategoryName']
    category_id = row['CategoryID']
    category_name = row['CategoryName']
    is_category = 1

  if pd.isna(row['SubCategoryID']):
    sub_category_id = category_id
    sub_category_name = category_name
    is_sub_category = 0
  else:
    sub_category_id = row['SubCategoryID']
    sub_category_name = row['SubCategoryName']
    is_sub_category = 1

  return pd.Series([
    master_category_id, master_category_name, category_id, category_name,
    is_category, sub_category_id, sub_category_name, is_sub_category
  ])

# Get the current date
current_date = date.today()
# Get the day of the month
day = current_date.day

def extract_sub_category_id_func(**context):
    print("Today is: ", current_date)
    if day == 1:
        print("[NOTICE] Extracting sub-category IDs from TIKI")
        URL = "https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"
        response = requests.get(URL, headers=HEADERS)
        time.sleep(random.uniform(3.2, 8.7))

        if response.status_code == 200:
            data = response.json()

            group_list = []
            group = data["menu_block"]["items"]
            for group in group:
                link = group["link"]
                group_id = link.split("/")[-1][1:]
                text = group["text"]

                if group_id in GroupID:
                    group_list.append([group_id, text])

        # group df    
        group_df = pd.DataFrame(group_list, columns=["GroupID", "Name"])
        # EXTRACT categories
        category_list = []
        for group_id, group_name in zip(group_df["GroupID"], group_df["Name"]):
            parent_url = f"https://tiki.vn/api/v2/categories?parent_id={group_id}"
            parent_response = requests.get(parent_url, headers=HEADERS)
            time.sleep(random.uniform(3.2, 8.7))
            if parent_response.status_code == 200:
                parent_data = parent_response.json()
                if not parent_data["data"]:
                    category_list.append([group_id, group_name, None, None, None, None, None, None])
                else:
                    for parent_category in parent_data["data"]:
                        parent_id = parent_category["id"]
                        parent_name = parent_category["name"]

                        child_url = f"https://tiki.vn/api/v2/categories?parent_id={parent_id}"
                        child_response = requests.get(child_url, headers=HEADERS)
                        time.sleep(random.uniform(3.2, 8.7))

                        if child_response.status_code == 200:
                            child_data = child_response.json()
                            if not child_data["data"]:
                                category_list.append([group_id, group_name, parent_id, parent_name, None, None, None, None])
                            else:
                                for child_category in child_data["data"]:
                                    child_id = child_category["id"]
                                    child_name = child_category["name"]

                                    type_url = f"https://tiki.vn/api/v2/categories?parent_id={child_id}"
                                    type_response = requests.get(type_url, headers=HEADERS)
                                    time.sleep(random.uniform(3.2, 8.7))

                                    if type_response.status_code == 200:
                                        type_data = type_response.json()
                                        if type_data["data"]:
                                            for type_item in type_data["data"]:
                                                type_id = type_item.get("id")
                                                type_name = type_item.get("name")
                                                category_list.append([group_id, group_name, parent_id, parent_name, child_id, child_name, type_id, type_name])
                                        else:
                                            category_list.append([group_id, group_name, parent_id, parent_name, child_id, child_name, None, None])
        category = pd.DataFrame(category_list, columns=["GroupID", "GroupName", "MasterCategoryID", "MasterCategoryName", "CategoryID", "CategoryName", "SubCategoryID", "SubCategoryName"])
        category = remove_single_category(category)
        category[['MasterCategoryID', 'MasterCategoryName', 'CategoryID', 'CategoryName', 'isCategory', 'SubCategoryID', 'SubCategoryName', 'isSubCategory']] = category.apply(transform_category, axis=1, result_type='expand')
        # Cast to int
        category["GroupID"] = category["GroupID"].astype(int)
        category["MasterCategoryID"] = category["MasterCategoryID"].astype(int)
        category["CategoryID"] = category["CategoryID"].astype(int)
        category["SubCategoryID"] = category["SubCategoryID"].astype(int)
        # Separate df
        master_category_df = category[["MasterCategoryID", "GroupID", "MasterCategoryName"]].drop_duplicates()
        category_df = category[["CategoryID", "MasterCategoryID", "CategoryName", "isCategory"]].drop_duplicates()
        sub_category_df = category[["SubCategoryID", "CategoryID", "SubCategoryName", "isSubCategory"]].drop_duplicates()
        # Rename columns
        master_category_df = master_category_df.rename(columns={"MasterCategoryName": "Name"})
        category_df = category_df.rename(columns={"CategoryName": "Name"})
        sub_category_df = sub_category_df.rename(columns={"SubCategoryName": "Name"})
        # Print out notification
        print(f"[SUCCESS] Extracted {len(master_category_df)} master categories records.")
        print(f"[SUCCESS] Extracted {len(category_df)} categories records.")
        print(f"[SUCCESS] Extracted {len(sub_category_df)} sub-categories records.")
        # Serialize the DataFrames to CSV strings
        master_category_csv = master_category_df.to_csv(index=False)
        category_csv = category_df.to_csv(index=False)
        sub_category_csv = sub_category_df.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
        context['task_instance'].xcom_push(key='category_df', value=category_csv)
        context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)
    else:
        print("[NOTICE] Extracting sub-category IDs from Azure")
        config = get_config('DevDB')
        print("[SUCCESS] Server, Database, Username, Password, Driver are loaded")
        # Configuration for the SQL Server connection
        server = config['server']
        database = config['database']
        username = config['username']
        password = config['password']
        driver = config['driver']
        # Connection string
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Timeout=60"
        )
        # Establish the connection
        conn = pyodbc.connect(conn_str)
        print("[SUCCESS] Connection is established")
        # Execute the queries
        master_category_df = pd.read_sql("SELECT * FROM MasterCategory", conn)
        category_df = pd.read_sql("SELECT * FROM Category", conn)
        sub_category_df = pd.read_sql("SELECT * FROM SubCategory", conn)
        # Cast to df
        master_category_df = pd.DataFrame(master_category_df)
        category_df = pd.DataFrame(category_df)
        sub_category_df = pd.DataFrame(sub_category_df)
        # Print out notification
        print(f"[SUCCESS] Extracted {len(master_category_df)} master categories records")
        print(f"[SUCCESS] Extracted {len(category_df)} categories records")
        print(f"[SUCCESS] Extracted {len(sub_category_df)} sub-categories records")
        # Serialize the DataFrames to CSV strings
        master_category_csv = master_category_df.to_csv(index=False)
        category_csv = category_df.to_csv(index=False)
        sub_category_csv = sub_category_df.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
        context['task_instance'].xcom_push(key='category_df', value=category_csv)
        context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)
    return None

def extract_all_product_id_func():
    if day == 1 or day == 15:
        print("Handle logic code")
    else:
        print("Retrieve from Azure database")
    return 0

def extract_specify_product_id_func():
    # Daily
    print("Handle logic code")
    return 0

def extract_product_data_func():
    # Daily
    print("Handle logic code")
    return 0

def extract_feedback_data_func():
    # Daily
    print("Handle logic code")
    return 0

def transform_data_func():
    # Daily
    print("Handle logic code")
    return 0

def load_data_func():
    # Daily
    print("Handle logic code")
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