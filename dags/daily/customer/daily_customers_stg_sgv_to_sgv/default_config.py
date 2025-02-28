from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "single_staging_table": Variable.get("SINGLE_VIEW_STAGING_CUSTOMER_TABLE"),
    "single_view_true_table": Variable.get("SINGLE_VIEW_TRUE_CUSTOMER_TABLE"),
    "single_view_dtac_table": Variable.get("SINGLE_VIEW_DTAC_CUSTOMER_TABLE"),
    "single_view_table": Variable.get("SINGLE_VIEW_CUSTOMER_TABLE"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}
