from sqlalchemy import create_engine, text
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import os
from datetime import datetime
import pytz
import random
import shutil

def create_and_upload_request_csv_to_gcs(table_name, local_folder, bucket_name, gcs_path, gcp_conn_id):
    df = pd.DataFrame(table_name)
    df = df.rename(columns={'name': 'table'})
    df = df.drop(columns=["csv_path"])

    current_date = datetime.now().strftime("%Y_%m_%d")
    id = str(random.randint(100, 999))
    file_name = f"grade_master_request_{current_date}_{id}.csv"
    local_path = f"/{local_folder}/{file_name}"
    df.to_csv(local_path,index=False)

    gcs_path= f"grade-master-request/{file_name}"
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    gcs_hook.upload(bucket_name=bucket_name, object_name=gcs_path, filename=local_path)
    os.remove(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")

def download_gcs_folder(table_name,bucket_name, gcs_master_path, local_folder, gcp_conn_id):
    local_folder_path= f"{local_folder}/{gcs_master_path}" 

    if os.path.exists(local_folder_path):
        shutil.rmtree(local_folder_path)
        print(f"Deleted the folder: {local_folder_path}")

    os.makedirs(local_folder_path, exist_ok=True)
    print(os.listdir("/tmp"))

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    current_date = datetime.now().strftime("%Y_%m_%d")
  
    for item in table_name:
        table = item['name']
        table_tmp = table.replace("_","-")
        file = f"{gcs_master_path}/{table_tmp}/{table}_{current_date}.csv"
        local_file_path = f"{local_folder_path}/{table}.csv"
    
        try:
            gcs_hook.download(bucket_name=bucket_name, object_name=file, filename=local_file_path)
            print(f"Downloaded {file} to {local_file_path}")
        except Exception as e:
            print(f"Failed to download {file}. Error: {str(e)}")

def create_table_if_not_exists(conn_string,table_schemas,schema_name,table_name, **kwargs):
    schema = table_schemas.get(table_name)
    if not schema:
        raise ValueError(f"Schema not defined for table: {table_name}")
    
    table_name = table_name + "_tmp"

    ddl_query = ", ".join([f"{col} {map_dtype_to_sql(dtype)}" for col, dtype in schema.items()])
    create_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({ddl_query})"
    
    engine = create_engine(conn_string)
    with engine.connect() as conn:
        conn.execute(text(create_query))
    print(f"Table {schema_name}.{table_name} checked/created.")

def map_dtype_to_sql(dtype):
    mapping = {
        "int": "INT",
        "str": "VARCHAR(256)",
        "datetime": "TIMESTAMP"
    }
    return mapping.get(dtype, "VARCHAR(256)") 

def load_csv_to_table(conn_string,table_schemas,schema_name,table_name, csv_path, **kwargs):
    engine = create_engine(conn_string)
    schema = table_schemas.get(table_name)

    if not schema:
        raise ValueError(f"Schema not defined for table: {table_name}")

    try:
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        df = pd.read_csv(csv_path)
        df['processed_date'] = datetime.now(bangkok_tz)
        invalid_rows = pd.DataFrame()
        print("CSV file loaded successfully.")

    except FileNotFoundError:
        print(f"Error: The file {csv_path} was not found.")
        return 
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file {csv_path} for table {table_name} is empty or malformed.")
        return
    
    with engine.connect() as conn:          
        truncate_query = f"TRUNCATE TABLE {schema_name}.{table_name}_tmp"
        conn.execute(text(truncate_query))
        print(f"Table {schema_name}.{table_name} truncated for reload.")

    for column, dtype in schema.items():
        if column not in df.columns:
            raise ValueError(f"Missing column '{column}' in CSV for table: {table_name}")
        
        try:
            if dtype == "int":
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
            elif dtype == "float":
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
            elif dtype == "datetime":
                df[column] = pd.to_datetime(df[column], errors="coerce")
            elif dtype == "str":
                df[column] = df[column].astype(str).fillna("")
            else:
                raise ValueError(f"Unsupported data type '{dtype}' for column '{column}'")
        except Exception as e:
            print(f"Error processing column '{column}': {e}")
            invalid_rows = pd.concat([invalid_rows, df[df[column].isnull()]])

    df = df[list(schema.keys())]
    df.to_sql(table_name+"_tmp", schema=schema_name ,con=engine, if_exists="append", index=False)
    os.remove(csv_path)
    print(f"Successfully loaded data into table: {table_name}")
