from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_DTAC_SUBSCRIBER_TABLE"),
    "staging_table": Variable.get("DTAC_STAGING_SUBSCRIBER_TABLE"),
    "single_staging_table": Variable.get("SINGLE_VIEW_STAGING_SUBSCRIBER_TABLE"),
    "single_staging_unmatched_table": Variable.get("SINGLE_VIEW_STAGING_UNMATCHED_SUBSCRIBER_TABLE"),
    "single_view_subscriber_table": Variable.get("SINGLE_VIEW_SUBSCRIBER_TABLE"),
    "single_view_customer_table": Variable.get("SINGLE_VIEW_DTAC_CUSTOMER_TABLE"),
    "single_view_history_table": Variable.get("SINGLE_VIEW_SUBSCRIBER_HISTORY_TABLE"),
    "function_schema": Variable.get("FUNCTION_SCHEMA"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}