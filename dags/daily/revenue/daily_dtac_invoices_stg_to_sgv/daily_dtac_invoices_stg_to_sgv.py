from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.revenue.daily_dtac_invoices_stg_to_sgv.default_config import default_config
from daily.revenue.daily_dtac_invoices_stg_to_sgv.sql.sql_query import (
    extract_to_staging,
    delete_duplicates_staging,
    delete_duplicates_single_view,
    transform_to_single_view,
)
# from plugins.sengrid.sendgrid_send_email import success_callback, failure_callback
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_dtac_invoices_stg_to_sgv",
    default_args=default_args,
    description="Extract and process daily invoice data from the DTAC system.",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["invoice", "daily"],
    max_active_runs=1, 
    on_success_callback=None, 
    on_failure_callback=None, 

) as dag:

    sql_context = { **default_config }

    delete_duplicates_staging_task = VerticaOperator(
        task_id="delete_duplicates_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(delete_duplicates_staging).render(sql_context),
    )

    extract_to_staging_task = VerticaOperator(
        task_id="extract_to_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(extract_to_staging).render(sql_context),
    )

    delete_duplicates_single_view_task = VerticaOperator(
        task_id="delete_duplicates_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(delete_duplicates_single_view).render(sql_context),
    )

    transform_to_single_view_task = VerticaOperator(
        task_id="transform_to_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_to_single_view).render(sql_context),
    )

    delete_duplicates_staging_task >> extract_to_staging_task >> delete_duplicates_single_view_task >> transform_to_single_view_task
    
