import os
import pandas as pd

def upsert_data(base_dir, warehouse, database, data_df, check_columns):
    """
    Upload data from a pandas DataFrame to a local CSV file.
    Args:
        base_dir (str): The base directory path.
        warehouse (str): The warehouse name (formerly collection).
        database (str): The name of the CSV file (formerly file_name).
        data_df (pandas.DataFrame): The DataFrame containing the data to upload.
        check_columns (list): A list of column names to check for existing records.
    Returns:
        str: A summary message indicating the number of records inserted and updated.
    """
    
    # Ensure the file has a .csv extension
    if not database.endswith('.csv'):
        database += '.csv'
    
    # Construct the full file path
    full_path = os.path.join(base_dir, warehouse, database)
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    
    # Check if the file exists
    file_exists = os.path.isfile(full_path)
    
    if not file_exists:
        # Create a new CSV file with the data
        data_df.to_csv(full_path, index=False)
        print(f"[CREATE NEW] The file '{full_path}' created.")
        return f"[INSERT] There are {len(data_df)} records inserted."
    else:
        print(f"[EXIST] The file '{full_path}' already exists.")
        
        # Read existing data
        existing_df = pd.read_csv(full_path)
        
        # Ensure data types are consistent
        for col in check_columns:
            if col in data_df.columns:
                existing_df[col] = existing_df[col].astype(data_df[col].dtype)
        
        # Prepare for upsert
        update_count = 0
        insert_count = 0
        
        # Create a dictionary of existing records for faster lookup
        existing_records = {tuple(row[check_columns]): row for _, row in existing_df.iterrows()}
        
        # New records to be inserted or updated
        updated_records = []
        
        for _, row in data_df.iterrows():
            check_tuple = tuple(row[check_columns])
            if check_tuple in existing_records:
                # Update existing record
                existing_record = existing_records[check_tuple]
                updated_record = existing_record.copy()
                for col in data_df.columns:
                    if pd.notna(row[col]):  # Only update if new value is not NaN
                        updated_record[col] = row[col]
                updated_records.append(updated_record)
                update_count += 1
            else:
                # Prepare new record for insertion
                updated_records.append(row)
                insert_count += 1
        
        # Add any existing records that weren't updated
        for check_tuple, record in existing_records.items():
            if check_tuple not in {tuple(r[check_columns]) for r in updated_records}:
                updated_records.append(record)
        
        # Create new DataFrame with updated and new records
        result_df = pd.DataFrame(updated_records)
        
        # Write updated data back to CSV
        result_df.to_csv(full_path, index=False)
        
        result_message = ""
        if update_count > 0:
            result_message += f"[UPDATE] There are {update_count} records updated.\n"
        if insert_count > 0:
            result_message += f"[INSERT] There are {insert_count} records inserted."
        
        return result_message.strip()