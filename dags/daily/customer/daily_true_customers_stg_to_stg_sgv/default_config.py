from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_TRUE_CUSTOMER_TABLE"),
    "source_error_table": Variable.get("DWH_TRUE_CUSTOMER_ERROR_TABLE"),
    "staging_table": Variable.get("TRUE_STAGING_CUSTOMER_TABLE"),
    "single_staging_table": Variable.get("SINGLE_VIEW_STAGING_CUSTOMER_TABLE"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}
