from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.models import Variable
from grading.master.default_config import default_config
from grading.master.helper.helper import create_table_if_not_exists , load_csv_to_table ,create_and_upload_request_csv_to_gcs,download_gcs_folder

config = Variable.get("fetch_grade_master_to_vertica_monthly",default_var=default_config)
master_tables = config["master_tables"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_grade_master_to_vertica_monthly",
    default_args=default_args,
    description="",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["grading", "master" , "monthly"],
    max_active_runs=1
) as dag:
    
    create_and_upload_request_csv_to_gcs_task = PythonOperator(
            task_id=f"create_and_upload_request_csv_to_gcs_task",
            python_callable=create_and_upload_request_csv_to_gcs,
            op_kwargs={
                "table_name": master_tables["master_tables"],
                "local_folder": config["local_folder"],
                "bucket_name": config["gcs_serving_bucket"],
                "gcs_path": config["gcs_request_path"],
                "gcp_conn_id": "gcp_conn"
            },
        )
    
    download_gcs_folder_task = PythonOperator(
        task_id="download_gcs_folder_task",
        python_callable=download_gcs_folder,
        op_kwargs={
            "table_name": master_tables["master_tables"],
            "bucket_name": config["gcs_serving_bucket"],
            "gcs_master_path": config["gcs_master_path"], 
            "local_folder": config["local_folder"],
            "gcp_conn_id": "gcp_conn"
        }
    )
       
    for table in master_tables["master_tables"]:
        create_task = PythonOperator(
            task_id=f"create_{table['name']}_if_not_exists",
            python_callable=create_table_if_not_exists,
            op_kwargs={
                "conn_string": config["conn_string"],
                "table_schemas": config["table_schemas"],
                "schema_name": config["grading_schema"],
                "table_name": table["name"]},
        )
        
        load_task = PythonOperator(
            task_id=f"load_{table['name']}_from_csv",
            python_callable=load_csv_to_table,
            op_kwargs={
                "conn_string": config["conn_string"],
                "schema_name": config["grading_schema"],
                "table_schemas": config["table_schemas"],
                "table_name": table["name"], 
                "csv_path": table["csv_path"]
                },
        )
        
        create_and_upload_request_csv_to_gcs_task >> download_gcs_folder_task >> create_task >> load_task
