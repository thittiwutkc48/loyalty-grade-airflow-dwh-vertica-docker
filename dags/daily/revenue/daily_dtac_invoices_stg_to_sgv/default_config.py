from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_DTAC_INVOICE_TABLE"),
    "staging_table": Variable.get("DTAC_STAGING_INVOICE_TABLE"),
    "single_view_balance_table": Variable.get("SINGLE_DTAC_VIEW_BALANCE_TABLE"),
    "single_view_invoice_table": Variable.get("SINGLE_DTAC_VIEW_INVOICE_TABLE"),
    "function_schema": Variable.get("FUNCTION_SCHEMA"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}