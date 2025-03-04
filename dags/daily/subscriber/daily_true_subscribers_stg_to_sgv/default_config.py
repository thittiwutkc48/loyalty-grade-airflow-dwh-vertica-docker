from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_TRUE_SUBSCRIBER_TABLE"),
    "source_error_table": Variable.get("DWH_TRUE_SUBSCRIBER_ERROR_TABLE"),
    "staging_table": Variable.get("TRUE_STAGING_SUBSCRIBER_TABLE"),
    "staging_product_table": Variable.get("TRUE_STAGING_SUBSCRIBER_PRODUCT_TABLE"),
    "single_staging_table": Variable.get("SINGLE_VIEW_STAGING_SUBSCRIBER_TABLE"),
    "single_view_table": Variable.get("SINGLE_VIEW_SUBSCRIBER_TABLE"),
    "single_view_history_table": Variable.get("SINGLE_VIEW_SUBSCRIBER_HISTORY_TABLE"),
    "temp_product_table": "vw_temp_product_id_inactive",
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}