from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "dwh_schema": Variable.get("DWH_SCHEMA"),
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": "kaf_cus_crm_230001",
    "staging_table": "true_customer_staging",
    "single_staging_table": "single_customer_staging",
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}
