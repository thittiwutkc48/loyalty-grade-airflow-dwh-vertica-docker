from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "dwh_schema": Variable.get("DWH_SCHEMA"),
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": "kaf_charge_ccbs_220002",
    "staging_table": "true_invoice_staging",
    "single_view": "single_invoice",
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}

