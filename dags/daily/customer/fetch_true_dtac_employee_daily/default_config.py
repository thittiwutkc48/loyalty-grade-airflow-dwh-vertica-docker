from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.base import BaseHook

postgres_conn = BaseHook.get_connection("postgres_conn")
conn_string = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "table_name": "employee",
    "gcs_landing_bucket": Variable.get("GCS_LANDING_BUCKET"),
    "gcs_object_name": "employee/Emp_{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y%m%d')) }}.txt",
    "local_file_path": "employee/employee_data.txt",
    "conn_string": conn_string,
    "percentage_threshold": Variable.get("PERCENTAGE_THRESHOLD")
}