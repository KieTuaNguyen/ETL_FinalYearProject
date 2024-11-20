try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.models.xcom_arg import XComArg
    from airflow.hooks.base import BaseHook
    print("Modules were imported successfully")
except Exception as e:
    print("Error {} ".format(e))

from dependencies import *
from Local_function import upsert_data, insert_data

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

def send_email(subject, body):
    try:
        # Get connection details from Airflow connection
        conn = BaseHook.get_connection('email_default')
        sender_email = conn.login
        password = conn.password
        
        sender_name = "Flexiboard"
        receiver_email = "kietdng2@gmail.com"

        # Create message
        message = MIMEMultipart()
        message["From"] = formataddr((sender_name, sender_email))
        message["To"] = receiver_email
        message["Subject"] = subject

        # Add body to email
        message.attach(MIMEText(body, "html"))

        # Create SMTP session
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()  # Enable TLS
            server.login(sender_email, password)
            text = message.as_string()
            server.sendmail(sender_email, receiver_email, text)
        
        logging.info(f"Email sent successfully: {subject}")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
                   
def send_success_email(context):
    """Send an email notification when a task succeeds."""
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    log_url = context['task_instance'].log_url
    
    # Get current time in Vietnam timezone
    current_time = datetime.now() + timedelta(hours=7)
    formatted_date = current_time.strftime("%I:%M:%S %p on %Y-%m-%d")
    
    subject = f"[SUCCESS] Flexiboard task {task_id} succeeded"
    
    # Modified version of your original HTML with email-safe adjustments
    body = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family: Arial, sans-serif; background-color: #F0FDF4; margin: 0; padding: 40px 24px; font-size: 14px; line-height: 1.6; color: #374151;">
        <div style="max-width: 500px; margin: 0 auto; background: white; padding: 40px; border-radius: 40px; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.08);">
            <!-- Header -->
            <div style="margin: -40px -40px 40px; padding: 40px; background-color: #16A34A; border-radius: 40px 40px 60px 60px; position: relative;">
                <!-- Header Content -->
                <div style="position: relative; z-index: 1;">
                    <div style="display: inline-block; background-color: rgba(255,255,255,0.2); padding: 8px 16px; border-radius: 100px; margin-bottom: 16px;">
                        <span style="width: 24px; height: 24px; background: white; border-radius: 50%; margin-right: 8px; display: inline-block; text-align: center; color: #15803D; font-weight: bold;">F</span>
                        <span style="color: white; font-weight: 500;">Flexiboard</span>
                    </div>
                    <h1 style="font-weight: 800; font-size: 28px; color: white; margin: 0 0 8px;">Great News!</h1>
                    <p style="font-size: 16px; color: rgba(255,255,255,0.9); margin: 0;">Your task has been completed successfully.</p>
                </div>
            </div>
            
            <!-- Success Card -->
            <div style="background: white; border-radius: 24px; padding: 24px; margin: -60px 20px 32px; box-shadow: 0 15px 30px -5px rgba(0,0,0,0.1);">
                <div style="margin-bottom: 16px;">
                    <div style="display: inline-block; vertical-align: middle; width: 48px; height: 48px; background: #F0FDF4; border-radius: 16px; text-align: center; line-height: 48px; margin-right: 16px;">
                        <div style="display: inline-block; width: 24px; height: 24px; background: #16A34A; border-radius: 50%; color: white; font-size: 14px; line-height: 24px;">✓</div>
                    </div>
                    <div style="display: inline-block; vertical-align: middle;">
                        <div style="font-weight: 700; color: #16A34A; font-size: 18px;">Mission Accomplished!</div>
                        <div style="color: #6B7280; font-size: 14px;">Everything went smoothly</div>
                    </div>
                </div>
                
                <!-- Progress Bar -->
                <div style="height: 8px; background: #F3F4F6; border-radius: 4px;">
                    <div style="width: 100%; height: 100%; background-color: #16A34A; border-radius: 4px;"></div>
                </div>
            </div>

            <!-- Task Details -->
            <div style="background: #F0FDF4; padding: 32px; border-radius: 24px; margin-bottom: 32px; border: 1px solid #DCFCE7;">
                <div style="margin-bottom: 24px;">
                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                        <tr>
                            <td><h2 style="font-size: 20px; font-weight: 700; margin: 0; color: #166534;">Task Summary</h2></td>
                            <td align="right"><div style="padding: 6px 12px; background: white; color: #15803D; border-radius: 100px; font-size: 13px; font-weight: 600; display: inline-block;">{escape(formatted_date)}</div></td>
                        </tr>
                    </table>
                </div>
                
                <!-- Task Info -->
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td width="50%" style="padding-right: 8px;">
                            <div style="padding: 16px; background: white; border-radius: 16px; border: 1px solid #BBF7D0;">
                                <div style="color: #15803D; font-size: 13px; margin-bottom: 4px;">Task ID</div>
                                <div style="font-weight: 600; color: #166534;">{escape(task_id)}</div>
                            </div>
                        </td>
                        <td width="50%" style="padding-left: 8px;">
                            <div style="padding: 16px; background: white; border-radius: 16px; border: 1px solid #BBF7D0;">
                                <div style="color: #15803D; font-size: 13px; margin-bottom: 4px;">Status</div>
                                <div style="font-weight: 600; color: #166534;">Success</div>
                            </div>
                        </td>
                    </tr>
                </table>
                
                <!-- Achievement Message -->
                <div style="background: white; border: 1px solid #BBF7D0; padding: 16px; border-radius: 12px; margin-top: 24px;">
                    <div style="margin-bottom: 8px;">
                        <span style="color: #16A34A; font-size: 18px; margin-right: 8px;">🎯</span>
                        <span style="font-weight: 600; color: #16A34A;">Achievement Unlocked!</span>
                    </div>
                    <p style="margin: 0; color: #166534; font-size: 14px;">All operations completed successfully with optimal performance.</p>
                </div>
            </div>

            <!-- Action Buttons -->
            <div style="margin-bottom: 32px;">
                <a href="{escape(log_url)}" style="display: block; background-color: #16A34A; color: white; padding: 16px 32px; text-decoration: none; border-radius: 16px; font-weight: 600; font-size: 16px; text-align: center; margin-bottom: 16px;">
                    <span style="margin-right: 8px;">View Complete Details</span>
                    <span style="font-size: 20px;">→</span>
                </a>
                <a href="#" style="display: block; background: white; color: #15803D; padding: 16px 32px; text-decoration: none; border-radius: 16px; font-weight: 600; font-size: 16px; text-align: center; border: 1px solid #BBF7D0;">
                    <span>Return to Dashboard</span>
                </a>
            </div>

            <!-- Support Section -->
            <div style="text-align: center; padding: 32px; background-color: #F0FDF4; border-radius: 24px;">
                <h3 style="margin: 0 0 16px; font-weight: 700; color: #166534; font-size: 18px;">We're Here to Help!</h3>
                <p style="margin: 0 0 16px; color: #166534;">Our support team is always ready to assist you</p>
                <a href="mailto:support@flexiboard.com" style="display: inline-block; color: #15803D; text-decoration: none; font-weight: 600; padding: 8px 16px; background: white; border-radius: 100px;">
                    <span style="margin-right: 8px;">support@flexiboard.com</span>
                    <span style="font-size: 18px;">💌</span>
                </a>
            </div>
        </div>
        
        <!-- Footer -->
        <div style="text-align: center; margin-top: 32px; color: #166534; font-size: 13px;">
            © 2024 Flexiboard. All rights reserved.
        </div>
    </body>
    </html>
    """
    
    send_email(subject, body)
    
def send_failure_email(context):
    """Send an email notification when a task fails."""
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    log_url = context['task_instance'].log_url
    
    # Get current time in Vietnam timezone
    current_time = datetime.now() + timedelta(hours=7)
    formatted_date = current_time.strftime("%I:%M:%S %p on %Y-%m-%d")
    
    subject = f"[FAILED] Flexiboard task {task_id} failed"
    body = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family: Arial, sans-serif; background-color: #F9FAFB; margin: 0; padding: 40px 24px; font-size: 14px; line-height: 1.6; color: #374151;">
        <div style="max-width: 500px; margin: 0 auto; background: white; padding: 40px; border-radius: 40px; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.08);">
            <!-- Header -->
            <div style="margin: -40px -40px 40px; padding: 40px; background-color: #DC2626; border-radius: 40px 40px 60px 60px; position: relative;">                
                <!-- Header Content -->
                <div style="position: relative; z-index: 1;">
                    <div style="display: inline-block; background-color: rgba(255,255,255,0.2); padding: 8px 16px; border-radius: 100px; margin-bottom: 16px;">
                        <span style="width: 24px; height: 24px; background: white; border-radius: 50%; margin-right: 8px; display: inline-block; text-align: center; color: #DC2626; font-weight: bold;">F</span>
                        <span style="color: white; font-weight: 500;">Flexiboard</span>
                    </div>
                    <h1 style="font-weight: 800; font-size: 28px; color: white; margin: 0 0 8px;">Task Error Detected</h1>
                    <p style="font-size: 16px; color: rgba(255,255,255,0.9); margin: 0;">We need your attention! ⚠️</p>
                </div>
            </div>
            
            <!-- Status Card -->
            <div style="background: white; border-radius: 24px; padding: 24px; margin: -60px 20px 32px; box-shadow: 0 15px 30px -5px rgba(0,0,0,0.1);">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td width="48" style="vertical-align: top;">
                            <div style="width: 48px; height: 48px; background: #FEF2F2; border-radius: 16px; text-align: center; line-height: 48px;">
                                <div style="width: 24px; height: 24px; background: #DC2626; border-radius: 50%; color: white; font-size: 14px; display: inline-block; line-height: 24px;">!</div>
                            </div>
                        </td>
                        <td style="padding-left: 16px;">
                            <div style="font-weight: 700; color: #DC2626; font-size: 18px;">Failed</div>
                            <div style="color: #6B7280; font-size: 14px;">Task encountered an error</div>
                        </td>
                    </tr>
                </table>
                
                <!-- Error Progress Bar -->
                <div style="height: 8px; background: #F3F4F6; border-radius: 4px; margin-top: 16px;">
                    <div style="width: 35%; height: 100%; background-color: #DC2626; border-radius: 4px;"></div>
                </div>
            </div>

            <!-- Task Details -->
            <div style="background: #F8FAFC; padding: 32px; border-radius: 24px; margin-bottom: 32px; border: 1px solid #E2E8F0;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 24px;">
                    <tr>
                        <td><h2 style="font-size: 20px; font-weight: 700; margin: 0; color: #1E293B;">Task Details</h2></td>
                        <td align="right"><div style="padding: 6px 12px; background: #FEF2F2; color: #DC2626; border-radius: 100px; font-size: 13px; font-weight: 600; display: inline-block;">{escape(formatted_date)}</div></td>
                    </tr>
                </table>
                
                <!-- Task Info -->
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 24px;">
                    <tr>
                        <td width="50%" style="padding-right: 8px;">
                            <div style="padding: 16px; background: white; border-radius: 16px; border: 1px solid #E2E8F0;">
                                <div style="color: #64748B; font-size: 13px; margin-bottom: 4px;">Task ID</div>
                                <div style="font-weight: 600; color: #1E293B;">{escape(task_id)}</div>
                            </div>
                        </td>
                        <td width="50%" style="padding-left: 8px;">
                            <div style="padding: 16px; background: white; border-radius: 16px; border: 1px solid #E2E8F0;">
                                <div style="color: #64748B; font-size: 13px; margin-bottom: 4px;">Status</div>
                                <div style="font-weight: 600; color: #DC2626;">Failed</div>
                            </div>
                        </td>
                    </tr>
                </table>
                
                <!-- Error Message -->
                <div style="background: #FEF2F2; border: 1px solid #FEE2E2; padding: 16px; border-radius: 12px; margin-bottom: 24px;">
                    <div style="margin-bottom: 8px;">
                        <span style="color: #DC2626; font-size: 18px; margin-right: 8px;">⚠️</span>
                        <span style="font-weight: 600; color: #DC2626;">Error Details</span>
                    </div>
                    <p style="margin: 0; color: #7F1D1D; font-size: 14px;">The task was cancelled due to an unexpected error. Please check the logs for more information.</p>
                </div>
                
                <p style="margin: 0; color: #64748B; font-size: 15px;">Our team has been notified and is investigating the issue.</p>
            </div>

            <!-- Action Buttons -->
            <div style="margin-bottom: 32px;">
                <a href="{escape(log_url)}" style="display: block; background-color: #DC2626; color: white; padding: 16px 32px; text-decoration: none; border-radius: 16px; font-weight: 600; font-size: 16px; text-align: center; margin-bottom: 16px;">
                    <span style="margin-right: 8px;">View Error Logs</span>
                    <span style="font-size: 20px;">→</span>
                </a>
                <a href="#" style="display: block; background: #F8FAFC; color: #DC2626; padding: 16px 32px; text-decoration: none; border-radius: 16px; font-weight: 600; font-size: 16px; text-align: center; border: 1px solid #FEE2E2;">
                    <span>Retry Task</span>
                </a>
            </div>

            <!-- Support Section -->
            <div style="text-align: center; padding: 32px; background-color: #F8FAFC; border-radius: 24px;">
                <h3 style="margin: 0 0 16px; font-weight: 700; color: #1E293B; font-size: 18px;">Need Immediate Help?</h3>
                <p style="margin: 0 0 16px; color: #64748B;">Our support team is ready to assist you</p>
                <a href="mailto:support@flexiboard.com" style="display: inline-block; color: #DC2626; text-decoration: none; font-weight: 600; padding: 8px 16px; background: white; border-radius: 100px;">
                    <span style="margin-right: 8px;">support@flexiboard.com</span>
                    <span style="font-size: 18px;">📧</span>
                </a>
            </div>
        </div>
        
        <!-- Footer -->
        <div style="text-align: center; margin-top: 32px; color: #6B7280; font-size: 13px;">
            © 2024 Flexiboard. All rights reserved.
        </div>
    </body>
    </html>
    """
    
    send_email(subject, body)

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
        # Config the path
        warehouse_path = os.path.join(base_dir, 'warehouse', 'category')
        group_path = os.path.join(warehouse_path, 'Group.csv')
        master_category_path = os.path.join(warehouse_path, 'MasterCategory.csv')
        category_path = os.path.join(warehouse_path, 'Category.csv')
        sub_category_path = os.path.join(warehouse_path, 'SubCategory.csv')
        
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

def transform_sub_category_func(**context):
    # Retrieve the CSV string from XCom
    group_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='group_df')
    master_category_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='master_category_df')
    category_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='category_df')
    sub_category_df = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='sub_category_df')
    
    # Deserialize the CSV string to a DataFrame
    group_df = pd.read_csv(io.StringIO(group_df))
    master_category_df = pd.read_csv(io.StringIO(master_category_df))
    category_df = pd.read_csv(io.StringIO(category_df))
    sub_category_df = pd.read_csv(io.StringIO(sub_category_df))
    
    # Convert to Dataframe
    group_df = pd.DataFrame(group_df)
    master_category_df = pd.DataFrame(master_category_df)
    category_df = pd.DataFrame(category_df)
    sub_category_df = pd.DataFrame(sub_category_df)

    if day == 1:
        # Drop duplicates
        group_df = group_df.drop_duplicates()
        master_category_df = master_category_df.drop_duplicates()
        category_df = category_df.drop_duplicates()
        sub_category_df = sub_category_df.drop_duplicates()
        # Cast to suitable datatype
        group_df["GroupID"] = group_df["GroupID"].astype(int)
        master_category_df["MasterCategoryID"] = master_category_df["MasterCategoryID"].astype(int)
        category_df["CategoryID"] = category_df["CategoryID"].astype(int)
        sub_category_df["SubCategoryID"] = sub_category_df["SubCategoryID"].astype(int)
        
        # Print out notification
        print(f"[SUCCESS] Transformed {len(group_df)} group records")
        print(f"[SUCCESS] Transformed {len(master_category_df)} master categories records")
        print(f"[SUCCESS] Transformed {len(category_df)} categories records")
        print(f"[SUCCESS] Transformed {len(sub_category_df)} sub-categories records")
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
        print("[NOTICE] Skipping transformation for group, master category, category, and sub-category")
        # Print out notification
        print(f"[SUCCESS] Transformed {len(group_df)} group records")
        print(f"[SUCCESS] Transformed {len(master_category_df)} master categories records")
        print(f"[SUCCESS] Transformed {len(category_df)} categories records")
        print(f"[SUCCESS] Transformed {len(sub_category_df)} sub-categories records")
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

def load_sub_category_func(**context):
    # Retrieve the CSV string from XCom
    group_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='group_df')
    master_category_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='master_category_df')
    category_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='category_df')
    sub_category_df = context['task_instance'].xcom_pull(task_ids='transform_data_sub_category', key='sub_category_df')
    
    # Deserialize the CSV string to a DataFrame
    group_df = pd.read_csv(io.StringIO(group_df))
    master_category_df = pd.read_csv(io.StringIO(master_category_df))
    category_df = pd.read_csv(io.StringIO(category_df))
    sub_category_df = pd.read_csv(io.StringIO(sub_category_df))
    
    # Convert to Dataframe
    group_df = pd.DataFrame(group_df)
    master_category_df = pd.DataFrame(master_category_df)
    category_df = pd.DataFrame(category_df)
    sub_category_df = pd.DataFrame(sub_category_df)

    if day == 1:
        # Config the collection folder
        collection = 'category'

        # For Group
        group_df
        database = 'Group.csv'
        check_columns = ['GroupID']
        result = upsert_data(base_dir, collection, database, group_df, check_columns)
        print(result)

        # For MasterCategory
        master_category_df
        database = 'MasterCategory.csv'
        check_columns = ['MasterCategoryID']
        result = upsert_data(base_dir, collection, database, master_category_df, check_columns)
        print(result)

        # For Category
        category_df
        database = 'Category.csv'
        check_columns = ['CategoryID']
        result = upsert_data(base_dir, collection, database, category_df, check_columns)
        print(result)

        # For SubCategory
        sub_category_df
        database = 'SubCategory.csv'
        check_columns = ['SubCategoryID']
        result = upsert_data(base_dir, collection, database, sub_category_df, check_columns)
        print(result)

    else:
        print("[NOTICE] Skipping loading for group, master category, category, and sub-category")
        # Print out notification
        print(f"[SUCCESS] Loaded {len(group_df)} group records")
        print(f"[SUCCESS] Loaded {len(master_category_df)} master categories records")
        print(f"[SUCCESS] Loaded {len(category_df)} categories records")
        print(f"[SUCCESS] Loaded {len(sub_category_df)} sub-categories records")

def extract_all_product_id_func(**context):
    # Retrieve the CSV string from XCom
    csv_data = context['task_instance'].xcom_pull(task_ids='extract_sub_category_id', key='sub_category_df')
    # Deserialize the CSV string to a DataFrame
    sub_category_df = pd.read_csv(io.StringIO(csv_data))
    # Convert to Dataframe
    sub_category_df = pd.DataFrame(sub_category_df)
    print(f"[SUCCESS] Extracted {len(sub_category_df)} sub-categories records")
    if day == 1 or day == 15:
        product_ids = []
        for sub_category_id in sub_category_df["SubCategoryID"]:
            product_data = retrieve_product_ids(sub_category_id)
            product_ids.extend(product_data)
        reference_product = pd.DataFrame(product_ids)
        # Create ReferenceID by combining SubCategoryID and ProductID
        reference_product["ReferenceID"] = reference_product["sub_category_id"].astype(str) + reference_product["product_id"].astype(str)
        # Modify the dataframe
        reference_product = reference_product[["ReferenceID", "sub_category_id", "product_id", "brand_name"]]
        # Rename columns
        reference_product.columns = ["ReferenceID", "SubCategoryID", "ProductID", "BrandName"]
        # Print out notification
        print(f"[SUCCESS] Extracted {len(reference_product)} reference product id records")
        # Serialize the DataFrames to CSV strings
        reference_product_csv = reference_product.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)
    else:
        print("[NOTICE] Extracting reference product IDs from Local")
        # Config the path
        warehouse_path = os.path.join(base_dir, 'warehouse', 'product')
        reference_product_path = os.path.join(warehouse_path, 'ReferenceProduct.csv')

        # Cast to df
        reference_product = pd.DataFrame(pd.read_csv(reference_product_path))
        # Print out notification
        print(f"[SUCCESS] Extracted {len(reference_product)} reference product id records")
        # Serialize the DataFrames to CSV strings
        reference_product_csv = reference_product.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)

def transform_all_product_func(**context):
    # Retrieve the CSV string from XCom
    reference_product_df = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='reference_product_df')
    # Deserialize the CSV string to a DataFrame
    reference_product_df = pd.read_csv(io.StringIO(reference_product_df))
    # Convert to Dataframe
    reference_product_df = pd.DataFrame(reference_product_df)
    
    if day == 1 or day == 15:
        # Drop duplicates
        reference_product_df = reference_product_df.drop_duplicates()
        # Cast to suitable datatype
        reference_product_df["ReferenceID"] = reference_product_df["ReferenceID"].astype(int)
        reference_product_df["SubCategoryID"] = reference_product_df["SubCategoryID"].astype(int)
        reference_product_df["ProductID"] = reference_product_df["ProductID"].astype(int)
        # Print out notification
        print(f"[SUCCESS] Transformed {len(reference_product_df)} reference product records")
        # Serialize the DataFrames to CSV strings
        reference_product_csv = reference_product_df.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)
    else:
        print("[NOTICE] Skipping transformation for reference product")
        # Print out notification
        print(f"[SUCCESS] Transformed {len(reference_product_df)} reference product records")
        # Serialize the DataFrames to CSV strings
        reference_product_csv = reference_product_df.to_csv(index=False)
        # Push the CSV strings as XCom values
        context['task_instance'].xcom_push(key='reference_product_df', value=reference_product_csv)

def load_all_product_func(**context):
    # Retrieve the CSV string from XCom
    reference_product_df = context['task_instance'].xcom_pull(task_ids='transform_data_all_product', key='reference_product_df')
    # Deserialize the CSV string to a DataFrame
    reference_product_df = pd.read_csv(io.StringIO(reference_product_df))
    # Convert to Dataframe
    reference_product_df = pd.DataFrame(reference_product_df)
    if day == 1 or day == 15:
        # Config paths with warehouse directory
        warehouse_path = os.path.join(base_dir, 'warehouse', 'product')
        full_path = os.path.join(warehouse_path, 'ReferenceProduct.csv')
        
        # Ensure directory exists
        os.makedirs(warehouse_path, exist_ok=True)
        
        # Save data
        reference_product_df.to_csv(full_path, index=False)
        print(f"[CREATE NEW] The file '{full_path}' created.")
        return f"[INSERT] There are {len(reference_product_df)} records inserted."
    else:
        print("[NOTICE] Skipping loading for reference products")
        # Print out notification
        print(f"[SUCCESS] Loaded {len(reference_product_df)} reference product records")

def extract_specify_product_id_func(brands, **context):
    # Retrieve the CSV string from XCom
    csv_data = context['task_instance'].xcom_pull(task_ids='extract_all_product_id', key='reference_product_df')
    # Deserialize the CSV string to a DataFrame
    specify_product_ids = pd.read_csv(io.StringIO(csv_data))
    # Convert brands to a list if it's a single brand
    if isinstance(brands, str):
        brands = [brands]
    # Filter the DataFrame based on the specified brands
    specify_product_ids = specify_product_ids[specify_product_ids['BrandName'].isin(brands)]
    # Print out notification
    print(f"[SUCCESS] Extracted {len(specify_product_ids)} product ids for {brands}")
    # Serialize the filtered DataFrame to a CSV string
    specify_product_ids_csv = specify_product_ids.to_csv(index=False)
    # Push the CSV string as an XCom value
    context['task_instance'].xcom_push(key='specify_product_ids', value=specify_product_ids_csv) 

def extract_product_data_func(**context):
    # Retrieve the CSV string from XCom
    csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_id", key='specify_product_ids')
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
    # Print out notification
    print(f"[SUCCESS] Extracted {len(product_data_list)} product dataframes for {context['brand']}")
    # Convert the product data list to a DataFrame
    product_data_df = pd.DataFrame(product_data_list)
    # Serialize the DataFrame to a CSV string
    product_data_csv = product_data_df.to_csv(index=False)
    # Push the CSV string as an XCom value
    context['task_instance'].xcom_push(key='product_data', value=product_data_csv)

def transform_specify_product_func(brand, **context):
    # Retrieve the CSV string from XCom
    csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{brand.lower()}_product_data", key='product_data')
    # Deserialize the CSV string to a DataFrame
    product_df = pd.read_csv(io.StringIO(csv_data))
    
    # TRANSFORM DATA
    # Convert to Dataframe and set the column names
    product_df_temp = product_df[["product_id",
                         "brand_id",
                         "seller_id",
                         "sub_category_id",
                         "product_name",
                         "product_url",
                         "product_image_url",
                         "created_date",
                         "quantity_sold"]]
    
    product_df_temp = product_df_temp.rename(columns={"product_id": "ProductID",
                                    "brand_id": "BrandID",
                                    "seller_id": "SellerID",
                                    "sub_category_id": "SubCategoryID",
                                    "product_name": "Name",
                                    "product_url": "URL",
                                    "product_image_url": "ImageURL",
                                    "created_date": "CreatedDate",
                                    "quantity_sold": "QuantitySold"})
    
    # Cast to suitable datatype
    product_df_temp['ProductID'] = product_df_temp['ProductID'].fillna(0).astype(int)
    product_df_temp['BrandID'] = product_df_temp['BrandID'].fillna(0).astype(int)
    product_df_temp['SellerID'] = product_df_temp['SellerID'].fillna(0).astype(int)
    product_df_temp['SubCategoryID'] = product_df_temp['SubCategoryID'].fillna(0).astype(int)
    product_df_temp['QuantitySold'] = product_df_temp['QuantitySold'].fillna(0).astype(int)

    # Sort the data by ProductID and CreatedDate in descending order
    product_df_temp = product_df_temp.sort_values(by=['ProductID', 'CreatedDate'], ascending=[True, False])

    # Get current time in UTC+7
    reference_date = pd.Timestamp.now() + timedelta(hours=7)

    # Process dates for LastUpdated
    def calculate_last_updated(group):
        """
        Calculate LastUpdated date for each product group

        Args:
            group (pd.Series): Group of CreatedDate values for a product

        Returns:
            pd.Series: Series of calculated LastUpdated dates
        """
        try:
            # Ensure numeric values
            group = pd.to_numeric(group, errors='coerce')

            # Get max date value
            max_date = group.max()

            # Calculate days difference and convert to float
            def calculate_date(x):
                if pd.isna(x) or pd.isna(max_date):
                    return None
                days_diff = float(max_date - x)  # Convert to float
                return reference_date - timedelta(days=days_diff)

            return group.apply(calculate_date)

        except Exception as e:
            logging.error(f"Error in calculate_last_updated: {str(e)}")
            # Return None for all values in case of error
            return pd.Series([None] * len(group))

    product_df_temp['LastUpdated'] = product_df_temp.groupby('ProductID')['CreatedDate'].transform(calculate_last_updated)

    # Process dates for LastUpdated with better error handling
    try:
        product_df_temp['LastUpdated'] = product_df_temp.groupby('ProductID')['CreatedDate'].transform(calculate_last_updated)

        # Ensure LastUpdated is datetime
        product_df_temp['LastUpdated'] = pd.to_datetime(product_df_temp['LastUpdated'])

    except Exception as e:
        logging.error(f"Failed to calculate LastUpdated: {str(e)}")
        # Set default value in case of failure
        product_df_temp['LastUpdated'] = reference_date

    # Calculate CreatedAt with error handling
    try:
        product_df_temp['CreatedAt'] = product_df_temp.apply(
            lambda row: row['LastUpdated'] - timedelta(days=float(row['CreatedDate'])) 
            if pd.notna(row['CreatedDate']) else None, 
            axis=1
        )
    except Exception as e:
        logging.error(f"Failed to calculate CreatedAt: {str(e)}")
        product_df_temp['CreatedAt'] = None

    # Drop CreatedDate column
    product_df_temp = product_df_temp.drop(columns=['CreatedDate'])

    # Reset index
    product_df_temp = product_df_temp.reset_index(drop=True)
    
    # Drop duplicates
    product_df_temp = product_df_temp.drop_duplicates()
    # Drop row where SellerID = 0 
    product_df_temp = product_df_temp[product_df_temp['SellerID'] != 0]

    
    # SPLIT TABLES
    # Product df
    product = product_df_temp[['ProductID', 'BrandID', 
                          'SellerID', 'SubCategoryID', 
                          'Name', 'URL', 
                          'ImageURL', 'CreatedAt']]
    product = product.drop_duplicates(subset='ProductID', keep='first')
    product = product.dropna(subset=['CreatedAt'])
    product = product.reset_index(drop=True)
    product = pd.DataFrame(product)
    product = product[['ProductID', 'BrandID', 
                       'SellerID', 'SubCategoryID', 
                       'Name', 'URL', 
                       'ImageURL', 'CreatedAt']]
    
    # ProductSale df
    productSale = product_df_temp[['ProductID', 'QuantitySold', 
                              'LastUpdated']]
    productSale = productSale.sort_values(['LastUpdated', 'ProductID']).reset_index(drop=True)
    # Calculate quantity fluctuation within each ProductID group

    def calculate_quantity_fluctuation(group):
        """
        Calculate quantity fluctuation for product sales over time.
        """
        try:
            # Create a copy to avoid SettingWithCopyWarning
            group = group.copy()

            # Ensure LastUpdated is in datetime format first
            group['LastUpdated'] = pd.to_datetime(group['LastUpdated'])

            # Sort by LastUpdated within the group
            group = group.sort_values('LastUpdated')

            # Initialize QuantityFluctuation column
            group['QuantityFluctuation'] = 0

            # Validate and clean quantity values
            group['QuantitySold'] = pd.to_numeric(group['QuantitySold'], errors='coerce')
            group['QuantitySold'] = group['QuantitySold'].fillna(0).astype(int)

            # Group by date to handle multiple records per day
            daily_records = group.groupby(group['LastUpdated'].dt.date, as_index=False).agg({
                'QuantitySold': 'last'  # Take the last record of each day
            })

            # Calculate daily fluctuations
            daily_records['QuantityFluctuation'] = daily_records['QuantitySold'].diff().fillna(0)

            # Fill first day's fluctuation with its quantity
            if not daily_records.empty:
                daily_records.loc[daily_records.index[0], 'QuantityFluctuation'] = daily_records.iloc[0]['QuantitySold']

            # Map fluctuations back to original records
            for date in group['LastUpdated'].dt.date.unique():
                date_records = daily_records[daily_records['LastUpdated'].dt.date == date]
                if not date_records.empty:
                    group.loc[group['LastUpdated'].dt.date == date, 'QuantityFluctuation'] = \
                        date_records['QuantityFluctuation'].iloc[0]

            # Handle negative fluctuations
            group['QuantityFluctuation'] = group['QuantityFluctuation'].clip(lower=0)

            # Ensure all fluctuations are integers
            group['QuantityFluctuation'] = group['QuantityFluctuation'].fillna(0).astype(int)

            return group

        except Exception as e:
            logging.error(f"Error calculating quantity fluctuation: {str(e)}")
            # Return original group with zero fluctuations if calculation fails
            group['QuantityFluctuation'] = 0
            return group
        
    productSale = productSale.groupby('ProductID', group_keys=False).apply(calculate_quantity_fluctuation)

    # Convert LastUpdated back to string format 'YYYY-MM-DD'
    productSale['LastUpdated'] = productSale['LastUpdated'].dt.strftime('%Y-%m-%d')
    productSale = productSale.reset_index(drop=True)
    productSale['ProductSaleID'] = productSale.index + 1
    productSale = pd.DataFrame(productSale)
    productSale = productSale[['ProductID', 'QuantitySold', 
                               'QuantityFluctuation', 'LastUpdated']]

    # inventory df
    inventory = pd.DataFrame({
        "InventoryID": range(1, len(product_df) + 1),
        "ProductID": product_df["product_id"],
        "Status": product_df["inventory_status"],
        "Type": product_df["inventory_type"],
        "LastUpdated": pd.Timestamp.now() + timedelta(hours=7)
    })
    # Cast to suitable datatype
    inventory['InventoryID'] = inventory['InventoryID'].fillna(0).astype(int)
    inventory['ProductID'] = inventory['ProductID'].fillna(0).astype(int)
    inventory['LastUpdated'] = pd.to_datetime(inventory['LastUpdated'])
    # Finalize the inventory df
    inventory = inventory.sort_values(['LastUpdated', 'InventoryID'])
    inventory = inventory.drop_duplicates(subset='InventoryID', keep='first')
    inventory = inventory.reset_index(drop=True)
    inventory = inventory[['InventoryID', 'ProductID', 'Status', 'Type', 'LastUpdated']]

    # pricing df
    pricing = pd.DataFrame({
        "PricingID": range(1, len(product_df) + 1),
        "ProductID": product_df["product_id"],
        "CurrentPrice": product_df["pricing_current"],
        "OriginalPrice": product_df["pricing_original"],
        "LastUpdated": pd.Timestamp.now() + timedelta(hours=7)
    })
    # Cast to suitable datatype
    pricing['PricingID'] = pricing['PricingID'].fillna(0).astype(int)
    pricing['ProductID'] = pricing['ProductID'].fillna(0).astype(int)
    pricing['CurrentPrice'] = pricing['CurrentPrice'].fillna(0).astype(float)
    pricing['OriginalPrice'] = pricing['OriginalPrice'].fillna(0).astype(float)
    pricing['LastUpdated'] = pd.to_datetime(pricing['LastUpdated'])
    # Finalize the pricing df
    pricing = pricing.sort_values(['PricingID', 'LastUpdated'], ascending=[True, False])
    pricing = pricing.drop_duplicates(subset='PricingID', keep='first')
    pricing = pricing.reset_index(drop=True)
    pricing = pricing[['PricingID', 'ProductID', 'CurrentPrice', 'OriginalPrice', 'LastUpdated']]

    # brand df
    brand = product_df[["brand_id",
                        "brand_name",
                        "brand_slug"]]
    brand = brand.rename(columns={"brand_id": "BrandID",
                                  "brand_name": "Name",
                                  "brand_slug": "Slug"})
    brand = brand.drop_duplicates(subset='BrandID', keep='first')
    brand = brand.reset_index(drop=True)
    brand = brand[['BrandID', 'Name', 'Slug']]

    # seller df
    seller = product_df[["seller_id",
                        "seller_name",
                        "seller_link",
                        "seller_image_url"]]
    seller = seller.rename(columns={"seller_id": "SellerID",
                                  "seller_name": "Name",
                                  "seller_link": "Link",
                                  "seller_image_url": "ImageURL"})
    def clean_seller_data(df):
        """
        Clean seller data by removing duplicated SellerID rows where ImageURL contains 'nan'

        Parameters:
        df (pandas.DataFrame): Input DataFrame with seller data

        Returns:
        pandas.DataFrame: Cleaned DataFrame
        """
        # Find duplicated SellerIDs
        duplicated_ids = df[df['SellerID'].duplicated(keep=False)]['SellerID'].unique()

        # Create a mask for rows to drop
        drop_mask = (df['SellerID'].isin(duplicated_ids)) & (df['ImageURL'].str.contains('nan', na=False))

        # Remove the rows matching our criteria
        cleaned_df = df[~drop_mask]

        # Reset index after dropping rows
        cleaned_df = cleaned_df.reset_index(drop=True)

        return cleaned_df
    # Cast to suitable datatypes
    seller['SellerID'] = seller['SellerID'].fillna(0).astype(int)
    seller['ImageURL'] = seller['ImageURL'].astype(str)
    # Standardize ImageURL with prefix
    prefix = "https://vcdn.tikicdn.com/ts/seller/"
    seller['ImageURL'] = seller['ImageURL'].apply(lambda url: url if url.startswith('http') else prefix + url)
    # Drop duplicates and sort
    seller = clean_seller_data(seller)
    seller = seller.drop_duplicates(subset='SellerID', keep='first')
    # Drop the row where SellerID  AND ImageURL = https://vcdn.tikicdn.com/ts/seller/nan
    
    seller = seller.sort_values('SellerID', ascending=False)
    seller = seller.reset_index(drop=True)
    seller = seller[['SellerID', 'Name', 'Link', 'ImageURL']]
    
    # Print out notification
    print(f"[SUCCESS] Transformed {len(product)} product records")
    print(f"[SUCCESS] Transformed {len(productSale)} product sale records")
    print(f"[SUCCESS] Transformed {len(inventory)} inventory records")
    print(f"[SUCCESS] Transformed {len(pricing)} pricing records")
    print(f"[SUCCESS] Transformed {len(brand)} brand records")
    print(f"[SUCCESS] Transformed {len(seller)} seller records")

    # Serialize the DataFrame to a CSV string
    product_csv = product.to_csv(index=False)
    productSale_csv = productSale.to_csv(index=False)
    inventory_csv = inventory.to_csv(index=False)
    pricing_csv = pricing.to_csv(index=False)
    brand_csv = brand.to_csv(index=False)
    seller_csv = seller.to_csv(index=False)
    # Push the CSV string as an XCom value
    context['task_instance'].xcom_push(key='product_df', value=product_csv)
    context['task_instance'].xcom_push(key='productSale_df', value=productSale_csv)
    context['task_instance'].xcom_push(key='inventory_df', value=inventory_csv)
    context['task_instance'].xcom_push(key='pricing_df', value=pricing_csv)
    context['task_instance'].xcom_push(key='brand_df', value=brand_csv)
    context['task_instance'].xcom_push(key='seller_df', value=seller_csv)
    
def load_specify_product_func(**context):
    # Retrieve the CSV string from XCom
    product_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='product_df')
    productSale_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='productSale_df')
    inventory_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='inventory_df')
    pricing_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='pricing_df')
    brand_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='brand_df')
    seller_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_product_{context['brand'].lower()}", key='seller_df')
    
    # Deserialize the CSV string to a DataFrame
    product_df = pd.read_csv(io.StringIO(product_df))
    productSale_df = pd.read_csv(io.StringIO(productSale_df))
    inventory_df = pd.read_csv(io.StringIO(inventory_df))
    pricing_df = pd.read_csv(io.StringIO(pricing_df))
    brand_df = pd.read_csv(io.StringIO(brand_df))
    seller_df = pd.read_csv(io.StringIO(seller_df))
    
    # Convert to Dataframe
    product_df = pd.DataFrame(product_df)
    productSale_df = pd.DataFrame(productSale_df)
    inventory_df = pd.DataFrame(inventory_df)
    pricing_df = pd.DataFrame(pricing_df)
    brand_df = pd.DataFrame(brand_df)
    seller_df = pd.DataFrame(seller_df)
    
    # Config the collection folder
    collection = 'product'

    # For Product
    database = 'Product'
    result = insert_data(base_dir, collection, database, product_df)
    print(result)
    
    # For ProductSale
    database = 'ProductSale'
    result = insert_data(base_dir, collection, database, productSale_df)
    print(result)
    
    # For Inventory
    database = 'Inventory'
    result = insert_data(base_dir, collection, database, inventory_df)
    print(result)
    
    # For Pricing
    database = 'Pricing'
    result = insert_data(base_dir, collection, database, pricing_df)
    print(result)
    
    # For Brand
    database = 'Brand'
    result = insert_data(base_dir, collection, database, brand_df)
    print(result)
    
    # For Seller
    database = 'Seller'
    result = insert_data(base_dir, collection, database, seller_df)
    print(result)

def extract_feedback_data_func(**context):
    # Retrieve the CSV string from XCom
    csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{context['brand'].lower()}_product_data", key='product_data')
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

    # Convert the feedback data list to a DataFrame
    feedback_data_df = pd.DataFrame(feedback_data_list)
    
    # Print out notification
    print(f"[SUCCESS] Extracted {len(feedback_data_df)} product feedbacks for {context['brand']}")

    # Serialize the DataFrame to a CSV string
    feedback_data_csv = feedback_data_df.to_csv(index=False)
    # Push the CSV string as an XCom value
    context['task_instance'].xcom_push(key='feedback_data', value=feedback_data_csv)

def transform_specify_feedback_func(brand, **context):
    # Retrieve the CSV string from XCom
    csv_data = context['task_instance'].xcom_pull(task_ids=f"extract_{brand.lower()}_feedback_data", key='feedback_data')
    # Deserialize the CSV string to a DataFrame
    feedback_df = pd.read_csv(io.StringIO(csv_data))
    
    # TRANSFORM data
    # hisotry df    
    history = feedback_df[["user_id",
                           "total_reviews",
                           "total_upvotes"]]
    history = history.rename(columns={"user_id": "UserID",
                                      "total_reviews": "TotalReview",
                                      "total_upvotes": "TotalUpvote"})
    # Cast to suitable datatype
    history['UserID'] = history['UserID'].fillna(0).astype(int)
    history['TotalReview'] = history['TotalReview'].fillna(0).astype(int)
    history['TotalUpvote'] = history['TotalUpvote'].fillna(0).astype(int)
    history = history.drop_duplicates()
    # Drop records with UserID = 0
    history = history[history['UserID'] != 0]   
    # Create new column
    history["HistoryID"] = range(1, len(history) + 1)
    history = pd.DataFrame(history)
    history = history[['HistoryID', 'UserID', 'TotalReview', 'TotalUpvote']]
    
    # user df
    user = feedback_df[["user_id", "username", "joined_time"]]
    user = user.rename(columns={"user_id": "UserID", "username": "Name", "joined_time": "JoinedDate"})
    # Cast datatypes
    user['UserID'] = user['UserID'].fillna(0).astype(int)
    user['Name'] = user['Name'].fillna('Unknown').astype(str)
    user['JoinedDate'] = pd.to_datetime(user['JoinedDate'], errors='coerce')
    # Remove records with UserID = 0
    user = user[user['UserID'] != 0]
    # Function to select the appropriate record for each UserID group
    def select_best_record(group):
        # Get records with non-null JoinedDate
        valid_dates = group[group['JoinedDate'].notna()]
        
        if len(valid_dates) > 0:
            # If there are records with dates, take the earliest one
            return valid_dates.sort_values('JoinedDate').iloc[0]
        else:
            # If no dates available, take the first record
            return group.iloc[0]
    # Group by UserID and apply the selection function
    user = user.groupby('UserID', group_keys=False).apply(select_best_record)
    # Convert to DataFrame and select columns
    user = pd.DataFrame(user)
    def clean_user_data(df):
        """
        Clean user data by removing duplicates based on UserID with specific rules:
        1. Keep row with JoinedDate if exists
        2. If JoinedDate is same, keep row with longer Name
        3. If Name lengths are same, keep row with non-null Name
        """
        # Reset index to remove ambiguity
        df_clean = df.reset_index(drop=True)

        def get_priority_row(group):
            # If only one row, return it
            if len(group) == 1:
                return group.iloc[0]

            # First priority: Keep row with JoinedDate
            has_date = group['JoinedDate'].notna()
            if has_date.any():
                group = group[has_date]

            # If multiple rows remain, check name length
            if len(group) > 1:
                name_lengths = group['Name'].fillna('').str.len()
                max_length = name_lengths.max()
                group = group[name_lengths == max_length]

            # If still multiple rows, prefer non-null names
            if len(group) > 1:
                has_name = group['Name'].notna()
                if has_name.any():
                    group = group[has_name]

            return group.iloc[0]

        # Group by UserID and apply cleaning rules
        result = df_clean.groupby('UserID', as_index=False).apply(lambda x: pd.Series(get_priority_row(x)))

        # Ensure proper DataFrame format and reset index
        result = result.reset_index(drop=True)
        result = result.sort_values('UserID').reset_index(drop=True)

        return result
    user = clean_user_data(user)
    user = user[['UserID', 'Name', 'JoinedDate']]
    # Reset index
    user = user.reset_index(drop=True)   
     
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
    # Cast to suitable datatype
    general_feedback['GeneralFeedbackID'] = general_feedback['GeneralFeedbackID'].fillna(0).astype(int)
    general_feedback['OneStar'] = general_feedback['OneStar'].fillna(0).astype(int)
    general_feedback['TwoStar'] = general_feedback['TwoStar'].fillna(0).astype(int)
    general_feedback['ThreeStar'] = general_feedback['ThreeStar'].fillna(0).astype(int)
    general_feedback['FourStar'] = general_feedback['FourStar'].fillna(0).astype(int)
    general_feedback['FiveStar'] = general_feedback['FiveStar'].fillna(0).astype(int)
    general_feedback['ReviewCount'] = general_feedback['ReviewCount'].fillna(0).astype(int)
    # Finalize the general_feedback df
    general_feedback = general_feedback.drop_duplicates()
    general_feedback = pd.DataFrame(general_feedback)
    general_feedback = general_feedback[['GeneralFeedbackID', 'OneStar', 'TwoStar', 'ThreeStar', 'FourStar', 'FiveStar', 'ReviewCount', 'LastUpdated']]
    
    # feedback_detail df
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
    feedback_detail = pd.DataFrame(feedback_detail)
    feedback_detail = feedback_detail[['ProductID', 'UserID', 'GeneralFeedbackID', 'Title', 'Content', 'Upvote', 'Rating', 'CreatedDate']]
    # Cast to suitable datatype
    feedback_detail['ProductID'] = feedback_detail['ProductID'].astype(int)
    feedback_detail['UserID'] = feedback_detail['UserID'].fillna(0)
    feedback_detail['UserID'] = feedback_detail['UserID'].astype(int)
    feedback_detail['GeneralFeedbackID'] = feedback_detail['GeneralFeedbackID'].astype(int)
    feedback_detail['Title'] = feedback_detail['Title'].astype(str)
    feedback_detail['Content'] = feedback_detail['Content'].astype(str)
    feedback_detail['Upvote'] = feedback_detail['Upvote'].astype(int)
    feedback_detail['Rating'] = feedback_detail['Rating'].astype(int)
    # Transform CreatedDate to datetime
    feedback_detail['CreatedDate'] = pd.to_datetime(feedback_detail['CreatedDate'], unit='s') 
    feedback_detail['CreatedDate'] = feedback_detail['CreatedDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    feedback_detail = feedback_detail.drop_duplicates()
    # Clean content
    def clean_content(group):
        # Sort the group so contents without asterisks come first
        group = group.sort_values(by='Content', key=lambda x: x.str.count('\*'))
        # Keep the first row (which will be without asterisks if such version exists)
        return group.iloc[0]
    # Group by the identifying columns and apply the cleaning function
    feedback_detail = feedback_detail.groupby(['ProductID', 'UserID', 'GeneralFeedbackID', 'CreatedDate']).apply(clean_content).reset_index(drop=True)
    # Reset index
    feedback_detail = feedback_detail.reset_index(drop=True)
    # Add a column FeedbackDetailID
    feedback_detail['FeedbackDetailID'] = feedback_detail.index + 1
    # Finalize the feedback_detail df
    feedback_detail = feedback_detail.drop_duplicates()
    feedback_detail = pd.DataFrame(feedback_detail)
    feedback_detail = feedback_detail.drop_duplicates(subset='GeneralFeedbackID', keep='first')
    feedback_detail.reset_index(drop=True, inplace=True)
    feedback_detail = feedback_detail[['FeedbackDetailID', 'ProductID', 'UserID', 'GeneralFeedbackID', 'Title', 'Content', 'Upvote', 'Rating', 'CreatedDate']]

    # Print out notification
    print(f"[SUCCESS] Transformed {len(history)} user history records")
    print(f"[SUCCESS] Transformed {len(user)} user records")
    print(f"[SUCCESS] Transformed {len(general_feedback)} general feedback records")
    print(f"[SUCCESS] Transformed {len(feedback_detail)} feedback detail records")
    
    # Serialize the DataFrame to a CSV string
    history_csv = history.to_csv(index=False)
    user_csv = user.to_csv(index=False)
    general_feedback_csv = general_feedback.to_csv(index=False)
    feedback_detail_csv = feedback_detail.to_csv(index=False)
    # Push the CSV string as an XCom value
    context['task_instance'].xcom_push(key='history_df', value=history_csv)
    context['task_instance'].xcom_push(key='user_df', value=user_csv)
    context['task_instance'].xcom_push(key='general_feedback_df', value=general_feedback_csv)
    context['task_instance'].xcom_push(key='feedback_detail_df', value=feedback_detail_csv)

def load_specify_feedback_func(**context):
    # Retrieve the CSV string from XCom
    history_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='history_df')
    user_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='user_df')
    general_feedback_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='general_feedback_df')
    feedback_detail_df = context['task_instance'].xcom_pull(task_ids=f"transform_data_feedback_{context['brand'].lower()}", key='feedback_detail_df')
    
    # Deserialize the CSV string to a DataFrame
    history_df = pd.read_csv(io.StringIO(history_df))
    user_df = pd.read_csv(io.StringIO(user_df))
    general_feedback_df = pd.read_csv(io.StringIO(general_feedback_df))
    feedback_detail_df = pd.read_csv(io.StringIO(feedback_detail_df))
    
    # Convert to Dataframe
    history_df = pd.DataFrame(history_df)
    user_df = pd.DataFrame(user_df)
    general_feedback_df = pd.DataFrame(general_feedback_df)
    feedback_detail_df = pd.DataFrame(feedback_detail_df)
    
    # Config the collection folder
    collection = 'feedback'
    
    # For History   
    database = 'History'
    result = insert_data(base_dir, collection, database, history_df)
    print(result)
    
    # For User
    database = 'User'
    result = insert_data(base_dir, collection, database, user_df)
    print(result)
    
    # For GeneralFeedback
    database = 'GeneralFeedback'
    result = insert_data(base_dir, collection, database, general_feedback_df)
    print(result)
    
    # For FeedbackDetail
    database = 'FeedbackDetail'
    result = insert_data(base_dir, collection, database, feedback_detail_df)
    print(result)


















# Define the DAG
with DAG(dag_id="Local_ETL", 
         default_args=default_args, 
         schedule_interval="@daily", 
         catchup=False) as f:
    
    # Define Tasks
    extract_sub_category_id = PythonOperator(
        task_id='extract_sub_category_id',
        python_callable=extract_sub_category_id_func,
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email
    )

    transform_data_sub_category = PythonOperator(
        task_id='transform_data_sub_category',
        python_callable=transform_sub_category_func,
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email
    )

    load_data_sub_category = PythonOperator(
        task_id='load_data_sub_category',
        python_callable=load_sub_category_func,
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email
    )

    extract_all_product_id = PythonOperator(
        task_id='extract_all_product_id',
        python_callable=extract_all_product_id_func,
        op_kwargs={'sub_category_df': XComArg(extract_sub_category_id)},
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email
    )

    transform_data_all_product = PythonOperator(
        task_id='transform_data_all_product',
        python_callable=transform_all_product_func,
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email
    )

    load_data_all_product = PythonOperator(
        task_id='load_data_all_product',
        python_callable=load_all_product_func,
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email
    )

    list_of_brands = ['Apple', 'HP', 'Asus', 'Samsung']
    # Testing
    # list_of_brands = ['Asus']
    
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
            op_kwargs={'product_ids_df': XComArg(extract_all_product_id), 'brands': [brand]},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
        )
        extract_specify_product_id_tasks[brand] = extract_specify_product_id_task

        extract_product_data_task = PythonOperator(
            task_id=f'extract_{brand.lower()}_product_data',
            python_callable=extract_product_data_func,
            op_kwargs={'brand': brand},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
        )
        extract_product_data_tasks[brand] = extract_product_data_task

        transform_data_product_task = PythonOperator(
            task_id=f'transform_data_product_{brand.lower()}',
            python_callable=transform_specify_product_func,
            op_kwargs={'brand': brand},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
        )
        transform_data_product_tasks[brand] = transform_data_product_task

        load_data_product_task = PythonOperator(
            task_id=f'load_data_product_{brand.lower()}',
            python_callable=load_specify_product_func,
            op_kwargs={'brand': brand},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
        )
        load_data_product_tasks[brand] = load_data_product_task

        extract_feedback_data_task = PythonOperator(
            task_id=f'extract_{brand.lower()}_feedback_data',
            python_callable=extract_feedback_data_func,
            op_kwargs={'brand': brand},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
        )
        extract_feedback_data_tasks[brand] = extract_feedback_data_task

        transform_data_feedback_task = PythonOperator(
            task_id=f'transform_data_feedback_{brand.lower()}',
            python_callable=transform_specify_feedback_func,
            op_kwargs={'brand': brand},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
        )
        transform_data_feedback_tasks[brand] = transform_data_feedback_task

        load_data_feedback_task = PythonOperator(
            task_id=f'load_data_feedback_{brand.lower()}',
            python_callable=load_specify_feedback_func,
            op_kwargs={'brand': brand},
            on_success_callback=send_success_email,
            on_failure_callback=send_failure_email
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