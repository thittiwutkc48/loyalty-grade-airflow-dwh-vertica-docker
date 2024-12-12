from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from daily.customer.fetch_true_dtac_employee_daily.default_config import default_config
from daily.customer.fetch_true_dtac_employee_daily.helper.helper import download_gcs_file , fetch_and_transform_employee_data

config = Variable.get("fetch_true_dtac_employee_daily",default_var=default_config)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_true_dtac_employee_daily",
    default_args=default_args,
    description="Ingestion and process daily employee data from the TRUE/DTAC system.",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["customer", "employee" , "daily"],
    max_active_runs=1
) as dag:

    download_gcs_task = PythonOperator(
        task_id="download_gcs_employee_data",
        python_callable=download_gcs_file,
        op_kwargs={
            "gcs_bucket_name": config['gcs_landing_bucket'],
            "gcs_object_name": config['gcs_object_name'],
            "destination_file_path": f"/tmp/{config['local_file_path']}"
        }
    )

    process_employee_data_task = PythonOperator(
        task_id="process_employee_data",
        python_callable=fetch_and_transform_employee_data,
        op_kwargs={
            "source_file_path": f"/tmp/{config['local_file_path']}",
            "table_name": config['table_name'],
            "grading_schema": config['grading_schema'],
            "conn_string": config['conn_string'],
            "percentage_threshold": config['percentage_threshold']
        },
    )

    download_gcs_task >> process_employee_data_task
