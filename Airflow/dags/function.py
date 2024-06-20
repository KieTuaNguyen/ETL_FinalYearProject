def upsert_data(table_name, data_df, check_columns, conn):
    """
    Upload data from a pandas DataFrame to an Azure SQL table.
    Args:
        table_name (str): The name of the table.
        data_df (pandas.DataFrame): The DataFrame containing the data to upload.
        check_columns (list): A list of column names to check for existing records.
        conn (pyodbc.Connection): The database connection object.
    Returns:
        str: A summary message indicating the number of records inserted and updated.
    """

    # Mapping pandas data types to SQL Server data types
    dtype_map = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'bool': 'BIT',
        'datetime64[ns]': 'DATETIME',
        'object': 'NVARCHAR(255)'
    }

    # Create a cursor from the connection
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(f"SELECT 1 FROM sys.tables WHERE name = '{table_name}'")
    table_exists = cursor.fetchone()

    if not table_exists:
        # Create a new table with the specified columns
        columns = [f"{col} {dtype_map.get(str(data_df[col].dtype), 'NVARCHAR(255)')}" for col in data_df.columns]
        columns_str = ", ".join(columns)
        primary_key_columns = ", ".join(check_columns)
        create_table_query = f"""
            CREATE TABLE [{table_name}] (
                {columns_str},
                PRIMARY KEY ({primary_key_columns})
            )
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"[CREATE NEW] The table '[{table_name}]' created.")
    else:
        print(f"[EXIST] The table '[{table_name}]' already exists.")

    # Prepare the insert and update queries
    column_placeholders = ", ".join(["?"] * len(data_df.columns))
    insert_query = f"INSERT INTO [{table_name}] VALUES ({column_placeholders})"
    update_placeholders = ", ".join([f"{col} = ?" for col in data_df.columns])
    check_conditions = " AND ".join([f"{col} = ?" for col in check_columns])
    update_query = f"UPDATE [{table_name}] SET {update_placeholders} WHERE {check_conditions}"

    update_count = 0
    insert_count = 0

    for _, row in data_df.iterrows():
        check_values = [row[col] for col in check_columns]
        check_query = f"SELECT COUNT(*) FROM [{table_name}] WHERE {check_conditions}"
        cursor.execute(check_query, check_values)
        record_count = cursor.fetchone()[0]

        try:
            if record_count > 0:
                update_values = [row[col] for col in data_df.columns] + check_values
                cursor.execute(update_query, update_values)
                update_count += 1
            else:
                insert_values = [row[col] for col in data_df.columns]
                cursor.execute(insert_query, insert_values)
                insert_count += 1

            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error occurred while processing record: {row}")
            print(f"Error message: {str(e)}")

    result_message = ""
    if update_count > 0:
        result_message += f"[UPDATE] There are {update_count} records updated.\n"
    if insert_count > 0:
        result_message += f"[INSERT] There are {insert_count} records inserted."

    return result_message