import pandas as pd
import pyodbc
import os
import io
import requests
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import json
import base64
import sys
# ALL THE PAKAGES REQUIRED 
################################################################### SQL Connection #####################################################################################
# Define the path for the configuration files
sql_config_path = Path('THE PATH FOR THE CONFIGURATION JSON')
datalake_config_path = Path('ADD THE CONFIGURATION PATH CONTAINS THE API URL AND CERTIFICATE')

# Open the SQL config file and load it once
with open(sql_config_path, 'r', encoding='utf-8') as f: ## Using UTF-8 encoding to handle unusual characters in the SQL database 
    config = json.load(f)
sql_config = config['sqlconnection']
update_config = config['updateconnection']
# Open the datalake config file
with open(datalake_config_path, 'r') as f:
    datalake_config = json.load(f)

# Establish connection to the main SQL Server (using Windows Authentication)
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_config['server']};DATABASE={sql_config['database']};Trusted_Connection=yes;"
connection = pyodbc.connect(conn_str)

# Establish connection to the update SQL Server (using SQL Server Authentication)
update_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={update_config['update_server']};DATABASE={update_config['update_database']};UID={update_config['username']};PWD={update_config['password']};"
update_connection = pyodbc.connect(update_conn_str)

# Retrieve column names and data types from the database dynamically
table_name = 'COLUMN NAME IN DB'
schema_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
"""


schema_df = pd.read_sql_query(schema_query, connection)

################################################################### Data Type Mapping #####################################################################################
def sql_to_pandas_dtype(sql_type): #THIS FUNCTION LOOPS THROUGH THE DB COLUMNS AND GET THE DATA TYPE 
    if sql_type in ('bigint', 'int', 'smallint', 'tinyint'):
        return 'Int64'
    elif sql_type in ('decimal', 'numeric', 'float', 'real'):
        return 'float64'
    elif sql_type in ('varchar', 'nvarchar', 'char', 'nchar', 'text'):
        return 'string'
    elif sql_type in ('date', 'datetime', 'datetime2'):
        return 'datetime64[us]'  # to microseconds
    elif sql_type == 'timestamp':
        return 'object'  # Keep as object to preserve original type
    else:
        return 'object'
    

# Populate dtype_mapping based on schema information
dtype_mapping = {row['COLUMN_NAME']: sql_to_pandas_dtype(row['DATA_TYPE']) for _, row in schema_df.iterrows()}

################################################################### Data Fetching and Processing #####################################################################################
# Directly assign the max_timestamp value provided 
max_timestamp = 0  # Assign the value directly here IF FIRST INGEST THE TIMESTAMP WILL BE 0 THEN YOU ADD YOUR MAX TIMESTAMP IF YOU USE UPSERT MODE
print(f"Max Timestamp: {max_timestamp}")

# Fetch data from the table
sql_query = f"SELECT * FROM [{table_name}] WHERE CONVERT(bigint,[timestamp]) > '{max_timestamp}' " #THE TIMESTAMP ASSIGN AS HEXADECMIAL IN TABLES CONVERT TO INTEGER.
df = pd.read_sql_query(sql_query, connection)

# Advanced timestamp handling
def process_timestamp(x):
    """
    Convert timestamp to a consistent string representation
    """
    if x is None:
        return None
    
    if isinstance(x, bytes):
        # Convert bytes to base64 encoded string for consistent representation
        try:
            return base64.b64encode(x).decode('utf-8')
        except Exception as e:
            print(f"Error converting timestamp: {e}")
            return None
    
    # For non-bytes types, convert to string
    return str(x)

# Apply timestamp processing
if 'timestamp' in df.columns:
    df['timestamp'] = df['timestamp'].apply(process_timestamp)

# Identify the picture column (replace 'Picture' with the actual column name)
picture_column = 'Picture' # THIS COLUMNS CAUSE A PROBLEM IN MATCHING THE SCHEMA WITH THE FLOW AS IT WAS SAVED AS IMAGE AND DATA LAKE SUPPORT STRUCTURED DATA TYPE ONLY.

# Check if the picture column exists
if picture_column in df.columns:
    # Set the picture column to null for all rows
    df[picture_column] = pd.NA

# Apply data types to columns
for col, dtype in dtype_mapping.items():
    try:
        if col != 'timestamp':  # Skip timestamp to preserve processing
            df[col] = df[col].astype(dtype)
    except Exception as e:
        print(f"Warning: Could not convert column {col} to {dtype}: {str(e)}")

################################################################### Create Parquet File #####################################################################################
# Define schema for Arrow Table with proper datetime handling
fields = []
for col in df.columns:
    if col == 'timestamp':
        fields.append(pa.field(col, pa.string()))  # Explicitly define timestamp as string
    elif df[col].dtype == 'Int64':
        fields.append(pa.field(col, pa.int64()))
    elif df[col].dtype == 'float64':
        fields.append(pa.field(col, pa.float64()))
    elif df[col].dtype == 'datetime64[us]':
        fields.append(pa.field(col, pa.timestamp('us')))  # Use microsecond precision
    else:
        fields.append(pa.field(col, pa.string()))

schema = pa.schema(fields)

# Convert DataFrame to Arrow Table with explicit schema
table = pa.Table.from_pandas(df, schema=schema)

# Write to buffer with Spark-compatible settings
buffer = io.BytesIO()
pq.write_table(
    table, 
    buffer,
    coerce_timestamps='us',  # Force microsecond precision
    allow_truncated_timestamps=True  # Allow truncation of higher precision timestamps
)
buffer.seek(0)

################################################################### Update Timestamp #####################################################################################
def decode_and_convert_timestamp(x):
    try:
        # Decode base64 to bytes
        decoded_bytes = base64.b64decode(x)
        
        # Convert bytes to integer (big-endian)
        numeric_value = int.from_bytes(decoded_bytes, byteorder='big')
        return numeric_value
    except Exception as e:
        print(f"Error converting timestamp {x}: {e}")
        return None

# Apply decoding and conversion
if 'timestamp' in df.columns and not df['timestamp'].empty:
    try:
        # Decode and convert timestamps
        max_timestamp = max(
            decode_and_convert_timestamp(ts) for ts in df['timestamp'] 
            if decode_and_convert_timestamp(ts) is not None
        )
    except Exception as e:
        print(f"Could not determine max timestamp: {e}")
        max_timestamp = None
print(f"Max Timestamp: {max_timestamp}")

# Count records
Count = df['timestamp'].count()
print(f"Count: {Count}")

# Establish connection for updating ADD THE MAX TIMESTAMP TO THE LOCAL DB
update_sql_query = f"""
    UPDATE TblTimeStampNAVDatalake
    SET [timestamp] = {max_timestamp}, [Count] = {Count}
    WHERE [Table_Name] = 'YOUR TABLE NAME'         
"""

try:
    with update_connection.cursor() as cursor:
        cursor.execute(update_sql_query)
        update_connection.commit()
        print("Timestamp and count updated successfully.")
except pyodbc.ProgrammingError as e:
    print(f"SQL Error: {e}")

################################################################### Push Item to Datalake #####################################################################################
API_BASE_URL =datalake_config['api_base_url']
API_KEY = datalake_config['api_key']
cert_path =datalake_config['cert_path']
job_companion = Path('Customerjob.json').read_text()

# Send the ingestion request and save the response        
response = requests.post(
    url=f"{API_BASE_URL}/jobs", 
    headers={"x-api-key": API_KEY}, 
    data=job_companion,
    verify=cert_path    
)

body = response.json()
print(f"Job-id: {body['id']}")

# If ingestion request was successful, send the data based on the response information
if response.ok:
    url = body['url']
    headers = body['headers']
    
    # Convert buffer to bytes for upload
    buffer.seek(0)
    parquet_data = buffer.getvalue()  
    response = requests.put(url, data=parquet_data, headers=headers)
    print("Request URL:", response.url)
    print("Response Status Code:", response.status_code)
    print("Response Headers:", response.headers)
    print("Response Text:", response.text)
    
    if not response.ok:
        raise Exception(f"Unable to push dataset: {response} {response.json()}")
else:
    raise Exception(f"Unable to create job: {response} {response.json()}")

# Update the job ID in the database
sql_query_ID = f"""
 INSERT INTO [TABLE NAME] VALUES ('ADD YOUR VALUES')
        
"""

# Execute the update
with update_connection.cursor() as cursor:
    cursor.execute(sql_query_ID)
    update_connection.commit()

# Close connections
connection.close()
update_connection.close()

