from datetime import datetime, timedelta
from airflow.models import Variable

default_config = {
    "grading_schema": Variable.get("GRADING_SCHEMA"),
    "source_table": Variable.get("DWH_TRUE_INVOICE_TABLE"),
    "source_error_table": Variable.get("DWH_TRUE_INVOICE_ERROR_TABLE"),
    "staging_table": Variable.get("TRUE_STAGING_INVOICE_TABLE"),
    "single_view_table": Variable.get("SINGLE_TRUE_VIEW_INVOICE_TABLE"),
    "discount_codes" :Variable.get("TRUE_INVOICE_DISCOUNT_CODE_FILTER"),
    "partition_date": "{{ dag_run.conf.get('partition_date', (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
}