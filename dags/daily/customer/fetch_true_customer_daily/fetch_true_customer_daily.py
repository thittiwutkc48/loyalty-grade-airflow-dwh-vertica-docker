from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.customer.fetch_true_customer_daily.default_config import default_config
from daily.customer.fetch_true_customer_daily.sql.sql_query import (
    extract_to_staging,
    delete_duplicates_staging,
    delete_duplicates_single_staging,
    transform_to_single_staging,
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
    dag_id="fetch_true_customer_daily",
    default_args=default_args,
    description="Extract and process daily customer data from the TRUE system.",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["customer", "daily"],
    max_active_runs=1, 
    on_success_callback=None, 
    on_failure_callback=None, 

) as dag:

    sql_context = { **default_config }

    delete_duplicates_staging_task = PostgresOperator(
        task_id="delete_duplicates_staging",
        postgres_conn_id="postgres_conn",
        sql=Template(delete_duplicates_staging).render(sql_context),
    )

    extract_to_staging_task = PostgresOperator(
        task_id="extract_to_staging",
        postgres_conn_id="postgres_conn",
        sql=Template(extract_to_staging).render(sql_context),
    )

    delete_duplicates_single_staging_task = PostgresOperator(
        task_id="delete_duplicates_single_staging",
        postgres_conn_id="postgres_conn",
        sql=Template(delete_duplicates_single_staging).render(sql_context),
    )

    transform_to_single_staging_task = PostgresOperator(
        task_id="transform_to_single_staging",
        postgres_conn_id="postgres_conn",
        sql=Template(transform_to_single_staging).render(sql_context),
    )

    delete_duplicates_staging_task >> extract_to_staging_task >> delete_duplicates_single_staging_task >> transform_to_single_staging_task
