from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import pandas as pd
from sqlalchemy import create_engine
import shutil

def download_gcs_file(gcs_bucket_name, gcs_object_name, destination_file_path):
    os.makedirs("/tmp/employee", exist_ok=True)
    gcs_hook = GCSHook(gcp_conn_id="gcp_conn")
    gcs_hook.download(
        bucket_name=gcs_bucket_name,
        object_name=gcs_object_name,
        filename=destination_file_path,
    )
    print(f"File successfully downloaded to: {destination_file_path}")
    print(f"GCS path was: gs://{gcs_bucket_name}/{gcs_object_name}")

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

        df["create_at"] =  pd.Timestamp.now()
        df["partition_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

        new_count = len(df)
  
        if existing_count > 0:
            percentage_diff = abs(new_count - existing_count) / existing_count
            print(
                f"Existing rows in database : {existing_count}, Rows in new data file: {new_count}, "
                f"Percentage difference: {percentage_diff * 100:.2f}%"
            )
            if percentage_diff > float(percentage_threshold):
                print(f"Difference exceeds {float(percentage_threshold)*100}%. Data will not be appended.")
                return

        df.to_sql(
            table_name,
            con=engine,
            schema=grading_schema,
            if_exists="replace",
            index=False
        )

        shutil.rmtree("/tmp/employee")
        print("Data successfully loaded into EMPLOYEE table.")

    except Exception as e:
        print(f"Error during processing: {e}")
        raise
    

