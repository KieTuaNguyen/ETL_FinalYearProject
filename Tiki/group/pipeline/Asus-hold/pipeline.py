# Import libraries
import pandas as pd
import requests
import random
import time
from datetime import datetime
# Parameters
GroupID = ['1846', '1789']
brands = ['Asus']
# Define functions
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
# EXTRACT group of categories
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
cleaned_df = remove_single_category(category)
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
# EXTRACT product ids
product_ids = []
for sub_category_id in sub_category_df["SubCategoryID"]:
    product_data = retrieve_product_ids(sub_category_id)
    for product in product_data:
        product_ids.append([sub_category_id, product["product_id"], product["brand_name"]])

print(f"Success fetching data for {len(product_ids)} product ids")
product_ids = pd.DataFrame(product_ids, columns=["SubCategoryID", "ProductID", "BrandName"])
# EXTRACT produt id by brand
product_ids = product_ids[product_ids['BrandName'].isin(brands)]
# EXTRACT product information based on product_ids
product_data_list = []
for _, row in product_ids.iterrows():
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
        'category_id': data['categories']['id'] if data['categories']['is_leaf'] else data['breadcrumbs'][-2]['category_id'],
        'sub_category_id': sub_category_id,
        'brand_name': row['BrandName']
    }

    product_data_list.append(product_data)

print(f"Success fetching data for {len(product_data_list)} products")
product_df = pd.DataFrame(product_data_list, columns=['product_id', 'product_name', 'product_url', 'pricing_current', 'pricing_original', 'product_image_url', 'inventory_status', 'inventory_type', 'created_date', 'quantity_sold', 'brand_id', 'brand_name', 'brand_slug', 'seller_id', 'seller_name', 'seller_link', 'seller_image_url', 'category_id', 'sub_category_id', 'brand_name'])
# EXTRACT feedback data
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

            feedback_data_list.append([product_id, OneStarCount, TwoStarCount, 
                                       ThreeStarCount, FourStarCount, 
                                       FiveStarCount, reviews_count, 
                                       review_id, review_title, 
                                       review_content, review_upvote, 
                                       review_rating, review_created_at, 
                                       user_id, username, joined_time, 
                                       total_reviews, total_upvotes])


print(f"Success fetching data for {len(feedback_data_list)} feedbacks")
feedback_df = pd.DataFrame(feedback_data_list, columns=["ProductID", "OneStarCount", "TwoStarCount", "ThreeStarCount", "FourStarCount", "FiveStarCount", "reviews_count", "review_id", "review_title", "review_content", "review_upvote", "review_rating", "review_created_at", "user_id", "username", "joined_time", "total_reviews", "total_upvotes"])
# TRANSFORM data
# product df
product = product_df[["product_id",
                         "brand_id",
                         "seller_id",
                         "sub_category_id",
                         "product_name",
                         "product_url",
                         "product_image_url",
                         "created_date",
                         "quantity_sold"]]
product = product.rename(columns={"product_id": "ProductID",
                                        "brand_id": "BrandID",
                                        "seller_id": "SellerID",
                                        "sub_category_id": "SubCategoryID",
                                        "product_name": "Name",
                                        "product_url": "URL",
                                        "product_image_url": "ImageURL",
                                        "created_date": "CreatedDate",
                                        "quantity_sold": "QuantitySold"})

# inventory df
inventory = pd.DataFrame({
    "InventoryID": range(1, len(product_df) + 1),
    "ProductID": product_df["product_id"],
    "Status": product_df["inventory_status"],
    "Type": product_df["inventory_type"],
    "LastUpdated": datetime.now()
})

# pricing df
pricing = pd.DataFrame({
    "PricingID": range(1, len(product_df) + 1),
    "ProductID": product_df["product_id"],
    "CurrentPrice": product_df["pricing_current"],
    "OriginalPrice": product_df["pricing_original"],
    "LastUpdated": datetime.now()
})

# brand df
brand = product_df[["brand_id",
                       "brand_name",
                       "brand_slug"]]
brand = brand.rename(columns={"brand_name": "Name",
                                    "brand_slug": "Slug"})
brand = brand.drop_duplicates()

# seller df
seller = product_df[["seller_id",
                        "seller_name",
                        "seller_link",
                        "seller_image_url"]]
seller = seller.rename(columns={"seller_name": "Name",
                                      "seller_link": "Link",
                                      "seller_image_url": "ImageURL"})
seller = seller.drop_duplicates()

# user df
user = feedback_df[["user_id",
                       "username",
                       "joined_time",
                       "total_reviews",
                       "total_upvotes"]]
user = user.rename(columns={"user_id": "UserID",
                                  "username": "Name",
                                  "joined_time": "JoinedDate",
                                  "total_reviews": "TotalReview",
                                  "total_upvotes": "TotalUpvote"})
user = user.drop_duplicates()

# general_feedback df
general_feedback = feedback_df[["review_id",
                                   "OneStarCount",
                                   "TwoStarCount",
                                   "ThreeStarCount",
                                   "FourStarCount",
                                   "FiveStarCount",
                                   "reviews_count"]]
general_feedback = general_feedback.rename(columns={"review_id": "GeneralFeedbackID",
                                                          "OneStarCount": "OneStar",
                                                          "TwoStarCount": "TwoStar",
                                                          "ThreeStarCount": "ThreeStar",
                                                          "FourStarCount": "FourStar",
                                                          "FiveStarCount": "FiveStar",
                                                          "reviews_count": "ReviewCount"})
general_feedback["LastUpdated"] = datetime.now()
general_feedback = general_feedback.drop_duplicates()

# FeedbackDetail df
feedback_detail = feedback_df[["ProductID",
                               "user_id",
                               "review_id",
                               "review_title",
                               "review_content",
                               "review_upvote",
                               "review_rating",
                               "review_created_at"]]
feedback_detail = feedback_detail.rename(columns={"review_id": "GeneralFeedbackID",
                                                        "user_id": "UserID",
                                                        "review_title": "Title",
                                                        "review_content": "Content",
                                                        "review_upvote": "Upvote",
                                                        "review_rating": "Rating",
                                                        "review_created_at": "CreatedDate"})

feedback_detail["FeedbackDetailID"] = range(1, len(feedback_detail) + 1)
# LOAD data
group_df
master_category_df
category_df
sub_category_df
product
inventory
pricing
brand
seller
user
general_feedback
feedback_detail