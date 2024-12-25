from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "dwh_schema": Variable.get("DWH_SCHEMA"),
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "staging_table": "single_customer_staging",
    "employee_table": "employee",
    "single_table": "single_customer",
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}
