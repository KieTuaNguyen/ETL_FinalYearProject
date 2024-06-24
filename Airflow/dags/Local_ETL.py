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
from function import upsert_data

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

# Construct the full paths
base_dir = os.path.join(os.getcwd(), 'dags')

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

def retrieve_product_ids(id):
    base_url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    PARAMS = {"category": id, "page": 1}
    response = requests.get(base_url, headers=HEADERS, params=PARAMS)
    time.sleep(random.uniform(3.2, 8.7))
    data = response.json()
    total_page = data["paging"]["last_page"]
    product_data = []
    for page in range(1, total_page + 1):
        PARAMS = {"category": id, "page": page}
        response = requests.get(base_url, headers=HEADERS, params=PARAMS)
        time.sleep(random.uniform(3.2, 8.7))
        data = response.json()
        for item in data["data"]:
            product_id = item["id"]
            brand_name = item.get("brand_name", None)
            product_data.append({"sub_category_id": id, "product_id": product_id, "brand_name": brand_name})
    return product_data

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
        group_df = group_df.drop_duplicates()
        master_category_df = category[["MasterCategoryID", "GroupID", "MasterCategoryName"]].drop_duplicates()
        category_df = category[["CategoryID", "MasterCategoryID", "CategoryName", "isCategory"]].drop_duplicates()
        sub_category_df = category[["SubCategoryID", "CategoryID", "SubCategoryName", "isSubCategory"]].drop_duplicates()
        # Rename columns
        master_category_df = master_category_df.rename(columns={"MasterCategoryName": "Name"})
        category_df = category_df.rename(columns={"CategoryName": "Name"})
        sub_category_df = sub_category_df.rename(columns={"SubCategoryName": "Name"})
        # Print out notification
        print(f"[SUCCESS] Extracted {len(group_df)} group records.")
        print(f"[SUCCESS] Extracted {len(master_category_df)} master categories records.")
        print(f"[SUCCESS] Extracted {len(category_df)} categories records.")
        print(f"[SUCCESS] Extracted {len(sub_category_df)} sub-categories records.")
        # Serialize the DataFrames to CSV strings
        group_csv = group_df.to_csv(index=False)
        master_category_csv = master_category_df.to_csv(index=False)
        category_csv = category_df.to_csv(index=False)
        sub_category_csv = sub_category_df.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='group_df', value=group_csv)
        context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
        context['task_instance'].xcom_push(key='category_df', value=category_csv)
        context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)
    else:
        print("[NOTICE] Extracting sub-category IDs from Local")
                
        group_path = os.path.join(base_dir, 'category', 'group.csv')
        master_category_path = os.path.join(base_dir, 'category', 'master_category.csv')
        category_path = os.path.join(base_dir, 'category', 'category.csv')
        sub_category_path = os.path.join(base_dir, 'category', 'sub_category.csv')
        
        # Cast to df
        group_df = pd.DataFrame(pd.read_csv(group_path))
        master_category_df = pd.DataFrame(pd.read_csv(master_category_path))
        category_df = pd.DataFrame(pd.read_csv(category_path))
        sub_category_df = pd.DataFrame(pd.read_csv(sub_category_path))
        
        # Print out notification
        print(f"[SUCCESS] Extracted {len(group_df)} group records")
        print(f"[SUCCESS] Extracted {len(master_category_df)} master categories records")
        print(f"[SUCCESS] Extracted {len(category_df)} categories records")
        print(f"[SUCCESS] Extracted {len(sub_category_df)} sub-categories records")
        
        # Serialize the DataFrames to CSV strings
        group_csv = group_df.to_csv(index=False)
        master_category_csv = master_category_df.to_csv(index=False)
        category_csv = category_df.to_csv(index=False)
        sub_category_csv = sub_category_df.to_csv(index=False)
        
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='group_df', value=group_csv)
        context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
        context['task_instance'].xcom_push(key='category_df', value=category_csv)
        context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)
    return None

def transform_sub_category_func(**context):
#     # Retrieve the CSV string from XCom
#     group_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='group_df')
#     master_category_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='master_category_df')
#     category_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='category_df')
#     sub_category_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='sub_category_df')
    
#     # Deserialize the CSV string to a DataFrame
#     group_df = pd.read_csv(io.StringIO(group_df))
#     master_category_df = pd.read_csv(io.StringIO(master_category_df))
#     category_df = pd.read_csv(io.StringIO(category_df))
#     sub_category_df = pd.read_csv(io.StringIO(sub_category_df))
    
#     # Convert to Dataframe
#     group_df = pd.DataFrame(group_df)
#     master_category_df = pd.DataFrame(master_category_df)
#     category_df = pd.DataFrame(category_df)
#     sub_category_df = pd.DataFrame(sub_category_df)

#     if day == 1:
#         # Drop duplicates
#         group_df = group_df.drop_duplicates()
#         master_category_df = master_category_df.drop_duplicates()
#         category_df = category_df.drop_duplicates()
#         sub_category_df = sub_category_df.drop_duplicates()
#         # Cast to suitable datatype
#         group_df["GroupID"] = group_df["GroupID"].astype(int)
#         master_category_df["MasterCategoryID"] = master_category_df["MasterCategoryID"].astype(int)
#         category_df["CategoryID"] = category_df["CategoryID"].astype(int)
#         sub_category_df["SubCategoryID"] = sub_category_df["SubCategoryID"].astype(int)
        
#         # Print out notification
#         print(f"[SUCCESS] Transformed {len(group_df)} group records")
#         print(f"[SUCCESS] Transformed {len(master_category_df)} master categories records")
#         print(f"[SUCCESS] Transformed {len(category_df)} categories records")
#         print(f"[SUCCESS] Transformed {len(sub_category_df)} sub-categories records")
#         # Serialize the DataFrames to CSV strings
#         group_csv = group_df.to_csv(index=False)
#         master_category_csv = master_category_df.to_csv(index=False)
#         category_csv = category_df.to_csv(index=False)
#         sub_category_csv = sub_category_df.to_csv(index=False)
#         # Push the CSV strings as XCom values
#         context['task_instance'].xcom_push(key='group_df', value=group_csv)
#         context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
#         context['task_instance'].xcom_push(key='category_df', value=category_csv)
#         context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)
#     else:
#         print("[NOTICE] Skipping transformation for group, master category, category, and sub-category")
#         # Print out notification
#         print(f"[SUCCESS] Transformed {len(group_df)} group records")
#         print(f"[SUCCESS] Transformed {len(master_category_df)} master categories records")
#         print(f"[SUCCESS] Transformed {len(category_df)} categories records")
#         print(f"[SUCCESS] Transformed {len(sub_category_df)} sub-categories records")
#         # Serialize the DataFrames to CSV strings
#         group_csv = group_df.to_csv(index=False)
#         master_category_csv = master_category_df.to_csv(index=False)
#         category_csv = category_df.to_csv(index=False)
#         sub_category_csv = sub_category_df.to_csv(index=False)
#         # Push the CSV strings as XCom values
#         context['task_instance'].xcom_push(key='group_df', value=group_csv)
#         context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
#         context['task_instance'].xcom_push(key='category_df', value=category_csv)
#         context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)
    return 0

def load_sub_category_func(**context):
#     # Retrieve the CSV string from XCom
#     group_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='group_df')
#     master_category_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='master_category_df')
#     category_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='category_df')
#     sub_category_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='sub_category_df')
    
#     # Deserialize the CSV string to a DataFrame
#     group_df = pd.read_csv(io.StringIO(group_df))
#     master_category_df = pd.read_csv(io.StringIO(master_category_df))
#     category_df = pd.read_csv(io.StringIO(category_df))
#     sub_category_df = pd.read_csv(io.StringIO(sub_category_df))
    
#     # Convert to Dataframe
#     group_df = pd.DataFrame(group_df)
#     master_category_df = pd.DataFrame(master_category_df)
#     category_df = pd.DataFrame(category_df)
#     sub_category_df = pd.DataFrame(sub_category_df)

#     if day == 1:
#         # Establish the connection
#         conn = pyodbc.connect(conn_str)
#         cursor = conn.cursor()
#         print("[SUCCESS] Connection is established")

#         # For Group
#         group_df
#         table_name = 'Group'
#         check_columns = ['GroupID', 'Name']
#         result = upsert_data(table_name, group_df, check_columns, conn)
#         print(result)

#         # For MasterCategory
#         master_category_df
#         table_name = 'MasterCategory'
#         check_columns = ['MasterCategoryID', 'GroupID', 'Name']
#         result = upsert_data(table_name, master_category_df, check_columns, conn)
#         print(result)

#         # For Category
#         category_df
#         table_name = 'Category'
#         check_columns = ['CategoryID', 'MasterCategoryID', 'Name', 'isCategory']
#         result = upsert_data(table_name, category_df, check_columns, conn)
#         print(result)

#         # For SubCategory
#         sub_category_df
#         table_name = 'SubCategory'
#         check_columns = ['SubCategoryID', 'CategoryID', 'Name', 'isSubCategory']
#         result = upsert_data(table_name, sub_category_df, check_columns, conn)
#         print(result)

#         cursor.close()
#         conn.close()
#     else:
#         print("[NOTICE] Skipping loading for group, master category, category, and sub-category")
#         # Print out notification
#         print(f"[SUCCESS] Loaded {len(group_df)} group records")
#         print(f"[SUCCESS] Loaded {len(master_category_df)} master categories records")
#         print(f"[SUCCESS] Loaded {len(category_df)} categories records")
#         print(f"[SUCCESS] Loaded {len(sub_category_df)} sub-categories records")
    return 0

def extract_all_product_id_func(**context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='sub_category_df')
#     # Deserialize the CSV string to a DataFrame
#     sub_category_df = pd.read_csv(io.StringIO(csv_data))
#     # Convert to Dataframe
#     sub_category_df = pd.DataFrame(sub_category_df)
#     print(f"[SUCCESS] Extracted {len(sub_category_df)} sub-categories records")
#     if day == 1 or day == 15:
#         product_ids = []
#         for sub_category_id in sub_category_df["SubCategoryID"]:
#             product_data = retrieve_product_ids(sub_category_id)
#             product_ids.extend(product_data)
#         reference_product = pd.DataFrame(product_ids)
#         # Create ReferenceID by combining SubCategoryID and ProductID
#         reference_product["ReferenceID"] = reference_product["sub_category_id"].astype(str) + reference_product["product_id"].astype(str)
#         # Modify the dataframe
#         reference_product = reference_product[["ReferenceID", "sub_category_id", "product_id", "brand_name"]]
#         # Rename columns
#         reference_product.columns = ["ReferenceID", "SubCategoryID", "ProductID", "BrandName"]
#         # Print out notification
#         print(f"[SUCCESS] Extracted {len(reference_product)} reference product id records")
#         # Serialize the DataFrames to CSV strings
#         reference_product_csv = reference_product.to_csv(index=False)
#         # Push the CSV strings as XCom values
#         context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)
#     else:
#         print("[NOTICE] Extracting reference product IDs from Azure")
#         # Establish the connection
#         conn = pyodbc.connect(conn_str)
#         print("[SUCCESS] Connection is established")
#         # Execute the queries
#         reference_product = pd.read_sql("SELECT * FROM ReferenceProduct", conn)
#         # Cast to df
#         reference_product = pd.DataFrame(reference_product)
#         # Print out notification
#         print(f"[SUCCESS] Extracted {len(reference_product)} reference product id records")
#         # Serialize the DataFrames to CSV strings
#         reference_product_csv = reference_product.to_csv(index=False)
#         # Push the CSV strings as XCom values
#         context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)
    return 0

def transform_all_product_func(**context):
#     # Retrieve the CSV string from XCom
#     reference_product_df = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='reference_product_df')
#     # Deserialize the CSV string to a DataFrame
#     reference_product_df = pd.read_csv(io.StringIO(reference_product_df))
#     # Convert to Dataframe
#     reference_product_df = pd.DataFrame(reference_product_df)
    
#     if day == 1 or day == 15:
#         # Drop duplicates
#         reference_product_df = reference_product_df.drop_duplicates()
#         # Cast to suitable datatype
#         reference_product_df["ReferenceID"] = reference_product_df["ReferenceID"].astype(int)
#         reference_product_df["SubCategoryID"] = reference_product_df["SubCategoryID"].astype(int)
#         reference_product_df["ProductID"] = reference_product_df["ProductID"].astype(int)
#         # Print out notification
#         print(f"[SUCCESS] Transformed {len(reference_product_df)} reference product records")
#         # Serialize the DataFrames to CSV strings
#         reference_product_csv = reference_product_df.to_csv(index=False)
#         # Push the CSV strings as XCom values
#         context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)
#     else:
#         print("[NOTICE] Skipping transformation for reference product")
#         # Print out notification
#         print(f"[SUCCESS] Transformed {len(reference_product_df)} reference product records")
#         # Serialize the DataFrames to CSV strings
#         reference_product_csv = reference_product_df.to_csv(index=False)
#         # Push the CSV strings as XCom values
#         context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)
    return 0

def load_all_product_func(**context):
#     # Retrieve the CSV string from XCom
#     reference_product_df = context['task_instance'].xcom_pull(task_ids='transform_data_all_product', key='reference_product_df')
#     # Deserialize the CSV string to a DataFrame
#     reference_product_df = pd.read_csv(io.StringIO(reference_product_df))
#     # Convert to Dataframe
#     reference_product_df = pd.DataFrame(reference_product_df)
#     if day == 1 or day == 15:
#         # Establish the connection
#         conn = pyodbc.connect(conn_str)
#         cursor = conn.cursor()
#         print("[SUCCESS] Connection is established")

#         # For ReferenceProduct
#         reference_product_df
#         table_name = 'ReferenceProduct'
#         check_columns = ['ReferenceID', 'SubCategoryID', 'ProductID', 'BrandName']
#         result = upsert_data(table_name, reference_product_df, check_columns, conn)
#         print(result)

#         cursor.close()
#         conn.close()
#     else:
#         print("[NOTICE] Skipping loading for reference products")
#         # Print out notification
#         print(f"[SUCCESS] Loaded {len(reference_product_df)} reference product records")
    return 0

def extract_specify_product_id_func(brands, **context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='reference_product_df')
#     # Deserialize the CSV string to a DataFrame
#     specify_product_ids = pd.read_csv(io.StringIO(csv_data))
#     # Convert brands to a list if it's a single brand
#     if isinstance(brands, str):
#         brands = [brands]
#     # Filter the DataFrame based on the specified brands
#     specify_product_ids = specify_product_ids[specify_product_ids['BrandName'].isin(brands)]
#     # Print out notification
#     print(f"[SUCCESS] Extracted {len(specify_product_ids)} product ids for {brands}")
#     # Serialize the filtered DataFrame to a CSV string
#     specify_product_ids_csv = specify_product_ids.to_csv(index=False)
#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='specify_product_ids', value=specify_product_ids_csv) 
    return 0

def extract_product_data_func(**context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_id", key='specify_product_ids')
#     # Deserialize the CSV string to a DataFrame
#     product_ids_df = pd.read_csv(io.StringIO(csv_data))

#     product_data_list = []
#     for _, row in product_ids_df.iterrows():
#         sub_category_id = row['SubCategoryID']
#         product_id = row['ProductID']

#         URL = f"https://tiki.vn/api/v2/products/{product_id}"
#         PARAMS = {}

#         response = requests.get(URL, headers=HEADERS, params=PARAMS)
#         time.sleep(random.uniform(3.2, 4.7))

#         data = response.json()

#         product_data = {
#             'product_id': data['id'],
#             'product_name': data.get('name', None),
#             'product_url': data.get('short_url', None),
#             'pricing_current': data.get('price', None),
#             'pricing_original': data.get('original_price', None),
#             'product_image_url': data.get('thumbnail_url', None),
#             'inventory_status': data.get('inventory_status', None),
#             'inventory_type': data.get('inventory_type', None),
#             'created_date': data.get('day_ago_created', None),
#             'quantity_sold': data.get('all_time_quantity_sold', None),
#             'brand_id': data.get('brand', {}).get('id', None),
#             'brand_name': data.get('brand', {}).get('name', None),
#             'brand_slug': data.get('brand', {}).get('slug', None),
#             'seller_id': data.get('current_seller', {}).get('id', 0) if data.get('current_seller') else 0,
#             'seller_name': data.get('current_seller', {}).get('name', 0) if data.get('current_seller') else 0,
#             'seller_link': data.get('current_seller', {}).get('link', 0) if data.get('current_seller') else 0,
#             'seller_image_url': data.get('current_seller', {}).get('logo', 0) if data.get('current_seller') else 0,
#             'category_id': data['categories']['id'] if 'categories' in data and data['categories'].get('is_leaf', False) else data['breadcrumbs'][-2]['category_id'] if 'breadcrumbs' in data and len(data['breadcrumbs']) >= 2 else None,
#             'sub_category_id': sub_category_id,
#             'brand_name': row['BrandName']
#         }

#         product_data_list.append(product_data)
#     # Print out notification
#     print(f"[SUCCESS] Extracted {len(product_data_list)} product dataframes for {context['brand']}")
#     # Convert the product data list to a DataFrame
#     product_data_df = pd.DataFrame(product_data_list)
#     # Serialize the DataFrame to a CSV string
#     product_data_csv = product_data_df.to_csv(index=False)
#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='product_data', value=product_data_csv)
    return 0

def transform_specify_product_func(**context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_data", key='product_data')
#     # Deserialize the CSV string to a DataFrame
#     product_df = pd.read_csv(io.StringIO(csv_data))
    
#     # TRANSFORM data
#     # product df
#     product = product_df[["product_id",
#                              "brand_id",
#                              "seller_id",
#                              "sub_category_id",
#                              "product_name",
#                              "product_url",
#                              "product_image_url",
#                              "created_date",
#                              "quantity_sold"]]
#     product = product.rename(columns={"product_id": "ProductID",
#                                             "brand_id": "BrandID",
#                                             "seller_id": "SellerID",
#                                             "sub_category_id": "SubCategoryID",
#                                             "product_name": "Name",
#                                             "product_url": "URL",
#                                             "product_image_url": "ImageURL",
#                                             "created_date": "CreatedDate",
#                                             "quantity_sold": "QuantitySold"})
#     product = product.drop_duplicates()
    
#     # inventory df
#     inventory = pd.DataFrame({
#         "InventoryID": range(1, len(product_df) + 1),
#         "ProductID": product_df["product_id"],
#         "Status": product_df["inventory_status"],
#         "Type": product_df["inventory_type"],
#         "LastUpdated": datetime.now()
#     })
#     inventory = inventory.drop_duplicates()

#     # pricing df
#     pricing = pd.DataFrame({
#         "PricingID": range(1, len(product_df) + 1),
#         "ProductID": product_df["product_id"],
#         "CurrentPrice": product_df["pricing_current"],
#         "OriginalPrice": product_df["pricing_original"],
#         "LastUpdated": datetime.now()
#     })
#     pricing = pricing.drop_duplicates()

#     # brand df
#     brand = product_df[["brand_id",
#                            "brand_name",
#                            "brand_slug"]]
#     brand = brand.rename(columns={"brand_name": "Name",
#                                   "brand_slug": "Slug"})
#     brand = brand.drop_duplicates()

#     # seller df
#     seller = product_df[["seller_id",
#                             "seller_name",
#                             "seller_link",
#                             "seller_image_url"]]
#     seller = seller.rename(columns={"seller_name": "Name",
#                                     "seller_link": "Link",
#                                     "seller_image_url": "ImageURL"})
#     seller = seller.drop_duplicates()
    
#     # Print out notification
#     print(f"[SUCCESS] Transformed {len(product)} product records")
#     print(f"[SUCCESS] Transformed {len(inventory)} inventory records")
#     print(f"[SUCCESS] Transformed {len(pricing)} pricing records")
#     print(f"[SUCCESS] Transformed {len(brand)} brand records")
#     print(f"[SUCCESS] Transformed {len(seller)} seller records")

#     # Serialize the DataFrame to a CSV string
#     product_csv = product.to_csv(index=False)
#     inventory_csv = inventory.to_csv(index=False)
#     pricing_csv = pricing.to_csv(index=False)
#     brand_csv = brand.to_csv(index=False)
#     seller_csv = seller.to_csv(index=False)
#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='product_df', value=product_csv)
#     context['task_instance'].xcom_push(key='inventory_df', value=inventory_csv)
#     context['task_instance'].xcom_push(key='pricing_df', value=pricing_csv)
#     context['task_instance'].xcom_push(key='brand_df', value=brand_csv)
#     context['task_instance'].xcom_push(key='seller_df', value=seller_csv)
    return 0

def load_specify_product_func(**context):
#     # Retrieve the CSV string from XCom
#     product_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='product_df')
#     inventory_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='inventory_df')
#     pricing_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='pricing_df')
#     brand_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='brand_df')
#     seller_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='seller_df')
    
#     # Deserialize the CSV string to a DataFrame
#     product_df = pd.read_csv(io.StringIO(product_df))
#     inventory_df = pd.read_csv(io.StringIO(inventory_df))
#     pricing_df = pd.read_csv(io.StringIO(pricing_df))
#     brand_df = pd.read_csv(io.StringIO(brand_df))
#     seller_df = pd.read_csv(io.StringIO(seller_df))
    
#     # Convert to Dataframe
#     product_df = pd.DataFrame(product_df)
#     inventory_df = pd.DataFrame(inventory_df)
#     pricing_df = pd.DataFrame(pricing_df)
#     brand_df = pd.DataFrame(brand_df)
#     seller_df = pd.DataFrame(seller_df)
    
#     # Establish the connection
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()
#     print("[SUCCESS] Connection is established")

#     # For Product
#     table_name = 'Product'
#     check_columns = ['ProductID', 'BrandID', 'SellerID', 'SubCategoryID', 'Name', 'URL', 'ImageURL', 'CreatedDate', 'QuantitySold']
#     result = upsert_data(table_name, product_df, check_columns, conn)
#     print(result)
    
#     # For Inventory
#     table_name = 'Inventory'
#     check_columns = ['InventoryID', 'ProductID', 'Status', 'Type', 'LastUpdated']
#     result = upsert_data(table_name, inventory_df, check_columns, conn)
#     print(result)
    
#     # For Pricing 
#     table_name = 'Pricing'
#     check_columns = ['PricingID', 'ProductID', 'CurrentPrice', 'OriginalPrice', 'LastUpdated']
#     result = upsert_data(table_name, pricing_df, check_columns, conn)
#     print(result)
    
#     # For Brand
#     table_name = 'Brand'
#     check_columns = ['BrandID', 'Name', 'Slug']
#     result = upsert_data(table_name, brand_df, check_columns, conn)
#     print(result)
    
#     # For Seller
#     table_name = 'Seller'
#     check_columns = ['SellerID', 'Name', 'Link', 'ImageURL']
#     result = upsert_data(table_name, seller_df, check_columns, conn)
#     print(result)
    
#     cursor.close()
#     conn.close()
    return 0

def extract_feedback_data_func(**context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_data", key='product_data')
#     # Deserialize the CSV string to a DataFrame
#     product_df = pd.read_csv(io.StringIO(csv_data))
#     feedback_data_list = []
#     for _, row in product_df.iterrows():
#       sub_category_id = row['sub_category_id']
#       product_id = row['product_id']
#       URL = "https://tiki.vn/api/v2/reviews"
#       PARAMS = {"limit": 20, 
#                 "spid": sub_category_id, 
#                 "product_id": product_id}

#       response = requests.get(URL, headers=HEADERS, params=PARAMS)
#       data = response.json()
#       total_pages = data.get("paging", {}).get("last_page", 1)

#       # Fetch data from each page
#       for page in range(1, total_pages + 1):
#         PARAMS["page"] = page
#         response = requests.get(URL, headers=HEADERS, params=PARAMS)
#         time.sleep(random.uniform(3.2, 4.7))
#         data = response.json()

#         stars = data.get("stars", {})
#         OneStarCount = stars.get("1", {}).get("count", 0)
#         TwoStarCount = stars.get("2", {}).get("count", 0)
#         ThreeStarCount = stars.get("3", {}).get("count", 0)
#         FourStarCount = stars.get("4", {}).get("count", 0)
#         FiveStarCount = stars.get("5", {}).get("count", 0)
#         reviews_count = data.get("reviews_count", 0)
#         review_data = data.get("data", [])

#         for review in review_data:
#           review_id = review.get("id")
#           review_title = review.get("title")
#           review_content = review.get("content")
#           review_upvote = review.get("thank_count", 0)
#           review_rating = review.get("rating")
#           review_created_at = review.get("created_at")
#           reviewer = review.get("created_by", {})

#           if reviewer is not None:
#               user_id = reviewer.get("id")
#               username = reviewer.get("name")
#               joined_time = reviewer.get("created_time")
#               total_reviews = reviewer.get("contribute_info", {}).get("summary", {}).get("total_review", 0)
#               total_upvotes = reviewer.get("contribute_info", {}).get("summary", {}).get("total_thank", 0)
#           else:
#               user_id = None
#               username = None
#               joined_time = None
#               total_reviews = 0
#               total_upvotes = 0

#           feedback_data_list.append({
#               "ProductID": product_id,
#               "OneStarCount": OneStarCount,
#               "TwoStarCount": TwoStarCount,
#               "ThreeStarCount": ThreeStarCount,
#               "FourStarCount": FourStarCount,
#               "FiveStarCount": FiveStarCount,
#               "reviews_count": reviews_count,
#               "review_id": review_id,
#               "review_title": review_title,
#               "review_content": review_content,
#               "review_upvote": review_upvote,
#               "review_rating": review_rating,
#               "review_created_at": review_created_at,
#               "user_id": user_id,
#               "username": username,
#               "joined_time": joined_time,
#               "total_reviews": total_reviews,
#               "total_upvotes": total_upvotes
#           })

#     # Convert the feedback data list to a DataFrame
#     feedback_data_df = pd.DataFrame(feedback_data_list)
    
#     # Print out notification
#     print(f"[SUCCESS] Extracted {len(feedback_data_df)} product feedbacks for {context['brand']}")

#     # Serialize the DataFrame to a CSV string
#     feedback_data_csv = feedback_data_df.to_csv(index=False)
#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='feedback_data', value=feedback_data_csv)
    return 0

def transform_specify_feedback_func(**context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_feedback_data", key='feedback_data')
#     # Deserialize the CSV string to a DataFrame
#     feedback_df = pd.read_csv(io.StringIO(csv_data))
    
#     # TRANSFORM data
#     # user df
#     user = feedback_df[["user_id",
#                            "username",
#                            "joined_time",
#                            "total_reviews",
#                            "total_upvotes"]]
#     user = user.rename(columns={"user_id": "UserID",
#                                       "username": "Name",
#                                       "joined_time": "JoinedDate",
#                                       "total_reviews": "TotalReview",
#                                       "total_upvotes": "TotalUpvote"})
#     user = user.drop_duplicates()
    
#     # general_feedback df
#     general_feedback = feedback_df[["review_id",
#                                     "OneStarCount",
#                                     "TwoStarCount",
#                                     "ThreeStarCount",
#                                     "FourStarCount",
#                                     "FiveStarCount",
#                                     "reviews_count"]]
#     general_feedback = general_feedback.rename(columns={"review_id": "GeneralFeedbackID",
#                                                         "OneStarCount": "OneStar",
#                                                         "TwoStarCount": "TwoStar",
#                                                         "ThreeStarCount": "ThreeStar",
#                                                         "FourStarCount": "FourStar",
#                                                         "FiveStarCount": "FiveStar",
#                                                         "reviews_count": "ReviewCount"})
#     general_feedback["LastUpdated"] = datetime.now()
#     general_feedback = general_feedback.drop_duplicates()
    
#     # feedback_detail df
#     feedback_detail = feedback_df[["ProductID",
#                                    "user_id",
#                                    "review_id",
#                                    "review_title",
#                                    "review_content",
#                                    "review_upvote",
#                                    "review_rating",
#                                    "review_created_at"]]
#     feedback_detail = feedback_detail.rename(columns={"review_id": "GeneralFeedbackID",
#                                                       "user_id": "UserID",
#                                                       "review_title": "Title",
#                                                       "review_content": "Content",
#                                                       "review_upvote": "Upvote",
#                                                       "review_rating": "Rating",
#                                                       "review_created_at": "CreatedDate"})
#     feedback_detail["FeedbackDetailID"] = range(1, len(feedback_detail) + 1)
#     feedback_detail = feedback_detail.drop_duplicates()
    
#     # Print out notification
#     print(f"[SUCCESS] Transformed {len(user)} user records")
#     print(f"[SUCCESS] Transformed {len(general_feedback)} general feedback records")
#     print(f"[SUCCESS] Transformed {len(feedback_detail)} feedback detail records")
    
#     # Serialize the DataFrame to a CSV string
#     user_csv = user.to_csv(index=False)
#     general_feedback_csv = general_feedback.to_csv(index=False)
#     feedback_detail_csv = feedback_detail.to_csv(index=False)
#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='user_df', value=user_csv)
#     context['task_instance'].xcom_push(key='general_feedback_df', value=general_feedback_csv)
#     context['task_instance'].xcom_push(key='feedback_detail_df', value=feedback_detail_csv)
    return 0

def load_specify_feedback_func(**context):
#     # Retrieve the CSV string from XCom
#     user_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='user_df')
#     general_feedback_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='general_feedback_df')
#     feedback_detail_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='feedback_detail_df')
    
#     # Deserialize the CSV string to a DataFrame
#     user_df = pd.read_csv(io.StringIO(user_df))
#     general_feedback_df = pd.read_csv(io.StringIO(general_feedback_df))
#     feedback_detail_df = pd.read_csv(io.StringIO(feedback_detail_df))
    
#     # Convert to Dataframe
#     user_df = pd.DataFrame(user_df)
#     general_feedback_df = pd.DataFrame(general_feedback_df)
#     feedback_detail_df = pd.DataFrame(feedback_detail_df)
    
#     # Establish the connection
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()
#     print("[SUCCESS] Connection is established")
    
#     # For User
#     table_name = 'User'
#     check_columns = ['UserID', 'Name', 'JoinedDate', 'TotalReview', 'TotalUpvote']
#     result = upsert_data(table_name, user_df, check_columns, conn)
#     print(result)
    
#     # For GeneralFeedback
#     table_name = 'GeneralFeedback'
#     check_columns = ['GeneralFeedbackID', 'OneStar', 'TwoStar', 'ThreeStar', 'FourStar', 'FiveStar', 'ReviewCount', 'LastUpdated']
#     result = upsert_data(table_name, general_feedback_df, check_columns, conn)
#     print(result)
    
#     # For FeedbackDetail
#     table_name = 'FeedbackDetail'
#     check_columns = ['FeedbackDetailID', 'GeneralFeedbackID', 'ProductID', 'UserID', 'Title', 'Content', 'Upvote', 'Rating', 'CreatedDate']
#     result = upsert_data(table_name, feedback_detail_df, check_columns, conn)
#     print(result)
    
#     cursor.close()
#     conn.close()
    return 0


















# Define the DAG
with DAG(dag_id="Local_ETL", 
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
        python_callable=transform_sub_category_func
    )

    load_data_sub_category = PythonOperator(
        task_id='load_data_sub_category',
        python_callable=load_sub_category_func
    )

    extract_all_product_id = PythonOperator(
        task_id='extract_all_product_id',
        python_callable=extract_all_product_id_func,
        op_kwargs={'sub_category_df': XComArg(extract_sub_category_id)}
    )

    transform_data_all_product = PythonOperator(
        task_id='transform_data_all_product',
        python_callable=transform_all_product_func
    )

    load_data_all_product = PythonOperator(
        task_id='load_data_all_product',
        python_callable=load_all_product_func
    )

    # list_of_brands = ['Apple', 'HP', 'Asus', 'Samsung']
    # Testing
    list_of_brands = ['Asus']
    
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

        transform_data_product_task = PythonOperator(
            task_id=f'transform_data_product_{brand.lower()}',
            python_callable=transform_specify_product_func
        )
        transform_data_product_tasks[brand] = transform_data_product_task

        load_data_product_task = PythonOperator(
            task_id=f'load_data_product_{brand.lower()}',
            python_callable=load_specify_product_func
        )
        load_data_product_tasks[brand] = load_data_product_task



        extract_feedback_data_task = PythonOperator(
            task_id=f'extract_{brand.lower()}_feedback_data',
            python_callable=extract_feedback_data_func,
            op_kwargs={'brand': brand}
        )
        extract_feedback_data_tasks[brand] = extract_feedback_data_task


        transform_data_feedback_task = PythonOperator(
            task_id=f'transform_data_feedback_{brand.lower()}',
            python_callable=transform_specify_feedback_func
        )
        transform_data_feedback_tasks[brand] = transform_data_feedback_task

        load_data_feedback_task = PythonOperator(
            task_id=f'load_data_feedback_{brand.lower()}',
            python_callable=load_specify_feedback_func
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