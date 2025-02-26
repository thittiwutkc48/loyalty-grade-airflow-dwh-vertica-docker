from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_DTAC_BALANCE_TABLE"),
    "staging_table": Variable.get("DTAC_STAGING_BALANCE_TABLE"),
    "single_view_table": Variable.get("SINGLE_DTAC_VIEW_BALANCE_TABLE"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}