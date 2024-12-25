from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "dwh_schema": Variable.get("DWH_SCHEMA"),
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": "kaf_product_crm_230002",
    "staging_table": "true_subscriber_staging",
    "single_view_subscriber": "single_subscriber",
    "single_view_customer": "single_customer",
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}