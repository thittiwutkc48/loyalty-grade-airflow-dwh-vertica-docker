from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import json
postgres_conn = BaseHook.get_connection("postgres_conn")
conn_string = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "gcs_landing_bucket": Variable.get("GCS_LANDING_BUCKET"),
    "gcs_serving_bucket": Variable.get("GCS_SERVING_BUCKET"),
    "gcs_request_path": "grade-master-request",
    "gcs_master_path": "grade-master",
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "local_folder": "/tmp",
    "conn_string": conn_string,
    "master_tables": json.loads(Variable.get("MASTER_TABLES")),
    "table_schemas": json.loads(Variable.get("GRADE_MASTER_TABLE_SCHEMAS")),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}