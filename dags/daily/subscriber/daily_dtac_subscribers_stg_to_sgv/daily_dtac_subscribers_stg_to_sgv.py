from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.subscriber.daily_dtac_subscribers_stg_to_sgv.default_config import default_config
from daily.subscriber.daily_dtac_subscribers_stg_to_sgv.sql.sql_query import (
    extract_to_staging,
    delete_duplicates_staging,
    transform_to_single_staging,
    transform_to_unmatched_single_staging,
    transform_inactive_to_single_view,
    insert_to_product_history,
    transform_active_to_single_view
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
    dag_id="daily_dtac_subscribers_stg_to_sgv",
    default_args=default_args,
    description="Extract and process daily subscriber data from the DTAC system.",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["subscriber", "daily"],
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

    transform_to_single_staging_task = VerticaOperator(
        task_id="transform_to_single_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_to_single_staging).render(sql_context),
    )

    transform_to_unmatched_single_staging_task = VerticaOperator(
        task_id="transform_to_unmatched_single_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_to_unmatched_single_staging).render(sql_context),
    )

    transform_inactive_to_single_view_task = VerticaOperator(
        task_id="transform_inactive_to_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_inactive_to_single_view).render(sql_context),
    )

    insert_to_product_history_task = VerticaOperator(
        task_id="insert_to_product_history_task",
        vertica_conn_id="vertica_conn",
        sql=Template(insert_to_product_history).render(sql_context),
    )

    transform_active_to_single_view_task = VerticaOperator(
        task_id="transform_active_to_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_active_to_single_view).render(sql_context),
    )

    delete_duplicates_staging_task >> extract_to_staging_task >> transform_to_single_staging_task >> transform_to_unmatched_single_staging_task >> transform_inactive_to_single_view_task >> insert_to_product_history_task >> transform_active_to_single_view_task
    