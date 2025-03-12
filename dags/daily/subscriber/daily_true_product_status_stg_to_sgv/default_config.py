from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_TRUE_PRODUCT_STATUS_TABLE"),
    "source_error_table": Variable.get("DWH_TRUE_PRODUCT_STATUS_ERROR_TABLE"),
    "staging_table": Variable.get("TRUE_STAGING_PRODUCT_STATUS_TABLE"),
    "single_view_table": Variable.get("SINGLE_TRUE_VIEW_PRODUCT_STATUS_TABLE"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}