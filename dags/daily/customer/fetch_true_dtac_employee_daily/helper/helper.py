from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import pandas as pd
from sqlalchemy import create_engine
import shutil
import logging
from airflow.exceptions import AirflowSkipException

def download_gcs_file(gcs_bucket_name, gcs_object_name, destination_file_path):
    """
    Download a file from GCS. If the file does not exist, skip downstream tasks.
    """
    try:
        os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
        gcs_hook = GCSHook(gcp_conn_id="gcp_conn")

        # Check if file exists in GCS
        if not gcs_hook.exists(bucket_name=gcs_bucket_name, object_name=gcs_object_name):
            logging.warning(f"File not found in GCS: gs://{gcs_bucket_name}/{gcs_object_name}")
            raise AirflowSkipException(f"Skipping DAG run as file is missing in GCS: {gcs_object_name}")

        # Download file
        gcs_hook.download(
            bucket_name=gcs_bucket_name,
            object_name=gcs_object_name,
            filename=destination_file_path,
        )
        logging.info(f"File successfully downloaded to: {destination_file_path}")

    except AirflowSkipException as e:
        logging.info(str(e))
        raise  # Re-raise to let Airflow handle the skip

    except Exception as e:
        logging.error(f"Error downloading file: {str(e)}")
        raise

def fetch_and_transform_employee_data(source_file_path, grading_schema , table_name, conn_string , percentage_threshold):
   
    engine = create_engine(conn_string)

    try:
        existing_count_query = f"SELECT COUNT(1) FROM {grading_schema}.{table_name}"
        existing_count = engine.execute(existing_count_query).scalar()

        df = pd.read_csv(source_file_path, encoding='utf-8', sep='|') 
        df.dropna(subset=["National_ID", "EmployeeID","PositionCode"], inplace=True)
        df.drop_duplicates(subset=["National_ID", "EmployeeID","PositionCode"], keep='first', inplace=True) 
        df.rename(columns={
            "National_ID": "national_id",
            "EmployeeID": "employee_id",
            "EngFirstName": "eng_firstname",
            "EngLastName": "eng_lastname",
            "ThaiFirstName": "thai_firstname",
            "ThaiLastName": "thai_lastname",
            "Company": "company_name",
            "PositionCode": "position_name",
        }, inplace=True)

        df["national_id"] = df["national_id"] .astype(str)
        df["employee_id"] = df["employee_id"] .astype(str)
        df["eng_firstname"] = df["eng_firstname"] .astype(str)
        df["eng_lastname"] = df["eng_lastname"] .astype(str)
        df["thai_firstname"] = df["thai_firstname"] .astype(str)
        df["thai_lastname"] = df["thai_lastname"] .astype(str)
        df["company_name"] = df["company_name"] .astype(str)
        df["position_name"] = df["position_name"].astype(str)
        df["create_at"] =  pd.Timestamp.now()
        df["partition_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

        new_count = len(df)
        
  
        if existing_count > 0:
            percentage_diff = abs(new_count - existing_count) / existing_count
            logging.info(
                f"Existing rows in database : {existing_count}, Rows in new data file: {new_count}, "
                f"Percentage difference: {percentage_diff * 100:.2f}%"
            )
            if percentage_diff > float(percentage_threshold):
                logging.info(f"Difference exceeds {float(percentage_threshold)*100}%. Data will not be appended.")
                return
            
        with engine.connect() as conn:
            conn.execute(f"TRUNCATE TABLE {grading_schema}.{table_name}")
        logging.info(f"Existing data in {table_name} has been truncated.")

        df.to_sql(
            table_name,
            con=engine,
            schema=grading_schema,
            if_exists="append",
            index=False
        )

        shutil.rmtree("/tmp/employee")
        logging.info("Data successfully loaded into EMPLOYEE table.")

    except Exception as e:
        logging.info(f"Error during processing: {e}")
        raise
    

