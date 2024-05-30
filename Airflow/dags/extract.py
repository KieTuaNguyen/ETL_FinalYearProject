import pandas as pd
import requests
import random
import time
from datetime import datetime
import io

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
      product_data.append({"product_id": product_id, "brand_name": brand_name})

  return product_data

# Extract functions
def extract_sub_category_id_func(**context):
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
  category["GroupID"] = category["GroupID"].astype(int)
  category["MasterCategoryID"] = category["MasterCategoryID"].astype(int)
  category["CategoryID"] = category["CategoryID"].astype(int)
  category["SubCategoryID"] = category["SubCategoryID"].astype(int)

  # master_category df
  master_category_df = category[["MasterCategoryID", "GroupID", "MasterCategoryName"]].drop_duplicates()
  master_category_df = master_category_df.rename(columns={"MasterCategoryName": "Name"})

  # category df
  category_df = category[["CategoryID", "MasterCategoryID", "CategoryName", "isCategory"]].drop_duplicates()
  category_df = category_df.rename(columns={"CategoryName": "Name"})

  # sub_category df
  sub_category_df = category[["SubCategoryID", "CategoryID", "SubCategoryName", "isSubCategory"]].drop_duplicates()
  sub_category_df = sub_category_df.rename(columns={"SubCategoryName": "Name"})

  print(f"Success fetching data for {len(master_category_df)} master categories")
  print(f"Success fetching data for {len(category_df)} categories")
  print(f"Success fetching data for {len(sub_category_df)} sub categories")

  # Serialize the DataFrames to CSV strings
  master_category_csv = master_category_df.to_csv(index=False)
  category_csv = category_df.to_csv(index=False)
  sub_category_csv = sub_category_df.to_csv(index=False)

  # Push the CSV strings as XCom values
  context['task_instance'].xcom_push(key='master_category_df', value=master_category_csv)
  context['task_instance'].xcom_push(key='category_df', value=category_csv)
  context['task_instance'].xcom_push(key='sub_category_df', value=sub_category_csv)

  return None

def extract_all_product_id_func(**context):
  # Retrieve the CSV string from XCom
  csv_data = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='sub_category_df')

  # Deserialize the CSV string to a DataFrame
  sub_category_df = pd.read_csv(io.StringIO(csv_data))

  # EXTRACT product ids
  product_ids = []
  for _, sub_category in sub_category_df.iterrows():
      sub_category_id = sub_category["SubCategoryID"]
      product_data = retrieve_product_ids(sub_category_id)
      for product in product_data:
          product_ids.append([sub_category_id, product["product_id"], product["brand_name"]])

  print(f"Success fetching data {len(product_ids)} product ids for all sub categories")
  
  product_ids_df = pd.DataFrame(product_ids, columns=["SubCategoryID", "ProductID", "BrandName"])

  # Serialize the DataFrame to a CSV string
  csv_data = product_ids_df.to_csv(index=False)

  # Push the CSV string as an XCom value
  context['task_instance'].xcom_push(key='return_value', value=csv_data)

  return None

def extract_specify_product_id_func(brands, **context):
  # Retrieve the CSV string from XCom
  csv_data = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='return_value')

  # Deserialize the CSV string to a DataFrame
  product_ids_df = pd.read_csv(io.StringIO(csv_data))

  # Convert brands to a list if it's a single brand
  if isinstance(brands, str):
      brands = [brands]

  # Filter the DataFrame based on the specified brands
  product_ids_df = product_ids_df[product_ids_df['BrandName'].isin(brands)]

  print(f"Success fetching data for {len(product_ids_df)} product ids for {brands}")
  
  # Serialize the filtered DataFrame to a CSV string
  csv_data = product_ids_df.to_csv(index=False)

  # Push the CSV string as an XCom value
  context['task_instance'].xcom_push(key='return_value', value=csv_data)

  return None
  
def extract_product_data_func(**context):
  # Retrieve the CSV string from XCom
  csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_id", key='return_value')

  # Deserialize the CSV string to a DataFrame
  product_ids_df = pd.read_csv(io.StringIO(csv_data))

  product_data_list = []
  for _, row in product_ids_df.iterrows():
      sub_category_id = row['SubCategoryID']
      product_id = row['ProductID']

      URL = f"https://tiki.vn/api/v2/products/{product_id}"
      PARAMS = {}

      response = requests.get(URL, headers=HEADERS, params=PARAMS)
      time.sleep(random.uniform(3.2, 4.7))

      data = response.json()

      product_data = {
          'product_id': data['id'],
          'product_name': data.get('name', None),
          'product_url': data.get('short_url', None),
          'pricing_current': data.get('price', None),
          'pricing_original': data.get('original_price', None),
          'product_image_url': data.get('thumbnail_url', None),
          'inventory_status': data.get('inventory_status', None),
          'inventory_type': data.get('inventory_type', None),
          'created_date': data.get('day_ago_created', None),
          'quantity_sold': data.get('all_time_quantity_sold', None),
          'brand_id': data.get('brand', {}).get('id', None),
          'brand_name': data.get('brand', {}).get('name', None),
          'brand_slug': data.get('brand', {}).get('slug', None),
          'seller_id': data.get('current_seller', {}).get('id', 0) if data.get('current_seller') else 0,
          'seller_name': data.get('current_seller', {}).get('name', 0) if data.get('current_seller') else 0,
          'seller_link': data.get('current_seller', {}).get('link', 0) if data.get('current_seller') else 0,
          'seller_image_url': data.get('current_seller', {}).get('logo', 0) if data.get('current_seller') else 0,
          'category_id': data['categories']['id'] if 'categories' in data and data['categories'].get('is_leaf', False) else data['breadcrumbs'][-2]['category_id'] if 'breadcrumbs' in data and len(data['breadcrumbs']) >= 2 else None,
          'sub_category_id': sub_category_id,
          'brand_name': row['BrandName']
      }

      product_data_list.append(product_data)

  print(f"Success fetching data for {len(product_data_list)} products for {context['brand']}")

  # Convert the product data list to a DataFrame
  product_data_df = pd.DataFrame(product_data_list)

  # Serialize the DataFrame to a CSV string
  csv_data = product_data_df.to_csv(index=False)

  # Push the CSV string as an XCom value
  context['task_instance'].xcom_push(key='return_value', value=csv_data)

  return None

def extract_feedback_data_func(**context):
  # Retrieve the CSV string from XCom
  csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_data", key='return_value')

  # Deserialize the CSV string to a DataFrame
  product_df = pd.read_csv(io.StringIO(csv_data))

  feedback_data_list = []
  for _, row in product_df.iterrows():
      sub_category_id = row['sub_category_id']
      product_id = row['product_id']
      URL = "https://tiki.vn/api/v2/reviews"
      PARAMS = {"limit": 20, 
                "spid": sub_category_id, 
                "product_id": product_id}

      response = requests.get(URL, headers=HEADERS, params=PARAMS)
      data = response.json()
      total_pages = data.get("paging", {}).get("last_page", 1)

      # Fetch data from each page
      for page in range(1, total_pages + 1):
          PARAMS["page"] = page
          response = requests.get(URL, headers=HEADERS, params=PARAMS)
          time.sleep(random.uniform(3.2, 4.7))
          data = response.json()

          stars = data.get("stars", {})
          OneStarCount = stars.get("1", {}).get("count", 0)
          TwoStarCount = stars.get("2", {}).get("count", 0)
          ThreeStarCount = stars.get("3", {}).get("count", 0)
          FourStarCount = stars.get("4", {}).get("count", 0)
          FiveStarCount = stars.get("5", {}).get("count", 0)
          reviews_count = data.get("reviews_count", 0)
          review_data = data.get("data", [])

          for review in review_data:
              review_id = review.get("id")
              review_title = review.get("title")
              review_content = review.get("content")
              review_upvote = review.get("thank_count", 0)
              review_rating = review.get("rating")
              review_created_at = review.get("created_at")
              reviewer = review.get("created_by", {})

              if reviewer is not None:
                  user_id = reviewer.get("id")
                  username = reviewer.get("name")
                  joined_time = reviewer.get("created_time")
                  total_reviews = reviewer.get("contribute_info", {}).get("summary", {}).get("total_review", 0)
                  total_upvotes = reviewer.get("contribute_info", {}).get("summary", {}).get("total_thank", 0)
              else:
                  user_id = None
                  username = None
                  joined_time = None
                  total_reviews = 0
                  total_upvotes = 0

              feedback_data_list.append({
                  "ProductID": product_id,
                  "OneStarCount": OneStarCount,
                  "TwoStarCount": TwoStarCount,
                  "ThreeStarCount": ThreeStarCount,
                  "FourStarCount": FourStarCount,
                  "FiveStarCount": FiveStarCount,
                  "reviews_count": reviews_count,
                  "review_id": review_id,
                  "review_title": review_title,
                  "review_content": review_content,
                  "review_upvote": review_upvote,
                  "review_rating": review_rating,
                  "review_created_at": review_created_at,
                  "user_id": user_id,
                  "username": username,
                  "joined_time": joined_time,
                  "total_reviews": total_reviews,
                  "total_upvotes": total_upvotes
              })

  print(f"Success fetching data for {len(feedback_data_list)} feedbacks")
  
  # Convert the feedback data list to a DataFrame
  feedback_data_df = pd.DataFrame(feedback_data_list)
  
  print(f"Success fetching data for {len(feedback_data_df)} feedbacks for {context['brand']}")
  
  # Serialize the DataFrame to a CSV string
  csv_data = feedback_data_df.to_csv(index=False)
  
  # Push the CSV string as an XCom value
  context['task_instance'].xcom_push(key='return_value', value=csv_data)
  return None

# import pandas as pd
# import io

# def extract_sub_category_id_func(**context):
#     print("Extracting sub category ID")
#     # Sample df
#     df = pd.DataFrame({
#         'MasterCategoryID': [1, 1, 1, 2, 2, 2, 3, 3, 3],
#         'MasterCategoryName': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C'],
#         'CategoryID': [11, 11, 12, 21, 21, 22, 31, 31, 32],
#         'CategoryName': ['AA', 'AA', 'AB', 'BA', 'BA', 'BB', 'CA', 'CA', 'CB'],
#         'SubCategoryID': [111, 112, 112, 211, 212, 212, 311, 312, 312],
#         'SubCategoryName': ['AAA', 'AAB', 'AAB', 'BAA', 'BAB', 'BAB', 'CAA', 'CAB', 'CAB']
#     })

#     # Serialize the DataFrame to a CSV string
#     csv_data = df.to_csv(index=False)

#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='return_value', value=csv_data)
#     print("Success set up the initial dataframe")
#     print("The cols of this DataFrame: ", df.columns)

# def extract_all_product_id_func(**context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='return_value')

#     # Deserialize the CSV string to a DataFrame
#     df = pd.read_csv(io.StringIO(csv_data))
    
#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='return_value', value=csv_data)

#     print("The cols of this DataFrame: ", df.columns)
#     print("The following are the unique SubCategoryIDs: ", df['SubCategoryID'].unique())

# def extract_specify_product_id_func(brand, **context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='return_value')

#     # Deserialize the CSV string to a DataFrame
#     df = pd.read_csv(io.StringIO(csv_data))

#     # Modify the df by adding a new column with the brand name
#     df['BrandName'] = brand

#     # Serialize the DataFrame to a CSV string
#     csv_data = df.to_csv(index=False)

#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='return_value', value=csv_data)
    
#     print("The cols of this DataFrame: ", df.columns)
#     print("The following are the unique SubCategoryIDs: ", df['SubCategoryID'].unique())
#     print("The following are the BrandNames: ", df['BrandName'])
#     print("First 2 rows of the DataFrame: ", df.head(2))
  
# def extract_product_data_func(brand, **context):
#     # Retrieve the CSV string from XCom
#     csv_data_1 = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='return_value')
#     csv_data_2 = context['task_instance'].xcom_pull(task_ids=f'extract_{brand.lower()}_product_id', key='return_value')

#     # Deserialize the CSV string to a DataFrame
#     df_1 = pd.read_csv(io.StringIO(csv_data_1))
#     df_2 = pd.read_csv(io.StringIO(csv_data_2))

#     # Merge the two DataFrames, selecting the desired columns
#     df = pd.merge(df_1[['MasterCategoryID', 'MasterCategoryName', 'CategoryID', 'CategoryName', 'SubCategoryID', 'SubCategoryName']],
#                   df_2[['SubCategoryID', 'BrandName']],
#                   on='SubCategoryID')

#     # Modify the BrandName column with the brand name
#     df['BrandName'] = f'{brand} Co.'

#     # Serialize the DataFrame to a CSV string
#     csv_data = df.to_csv(index=False)

#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='return_value', value=csv_data)

#     print("The cols of this DataFrame: ", df.columns)
#     print("First 3 rows of the DataFrame: ", df.head(3))
      
# def extract_feedback_data_func(brand, **context):
#     # Retrieve the CSV string from XCom
#     csv_data = context['task_instance'].xcom_pull(task_ids=f'extract_{brand.lower()}_product_data', key='return_value')

#     # Deserialize the CSV string to a DataFrame
#     df = pd.read_csv(io.StringIO(csv_data))

#     # Delete all columns except 'MasterCategoryID' and 'BrandName'
#     df = df[['MasterCategoryID', 'BrandName']]

#     # Serialize the DataFrame to a CSV string
#     csv_data = df.to_csv(index=False)

#     # Push the CSV string as an XCom value
#     context['task_instance'].xcom_push(key='return_value', value=csv_data)
    
#     print("The cols of this DataFrame: ", df.columns)
#     print("First 10 rows of the DataFrame: ", df.head(15))
#     print("Successfully extracted feedback data, length: ", len(df))
