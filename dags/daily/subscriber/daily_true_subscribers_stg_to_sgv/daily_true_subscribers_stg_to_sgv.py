from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.subscriber.daily_true_subscribers_stg_to_sgv.default_config import default_config
from daily.subscriber.daily_true_subscribers_stg_to_sgv.sql.sql_query import (
    extract_to_staging,
    validate_json,
    delete_duplicates_staging,
    delete_duplicates_single_staging,
    transform_inactive_to_single_view,
    transform_non_inactive_to_single_view,
    extract_to_staging_product,
    create_view_temp_product_id_inactive,
    update_product_id_in_temp_inactive,
    insert_to_product_history,
    delete_inactive_in_single_view,
    transform_inactive_to_single_staging,
    transform_non_inactive_to_single_staging
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
    dag_id="daily_true_subscribers_stg_to_sgv",
    default_args=default_args,
    description="Extract and process daily subscriber data from the TRUE system.",
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

    validate_json_task = VerticaOperator(
        task_id="validate_json_task",
        vertica_conn_id="vertica_conn",
        sql=Template(validate_json).render(sql_context),
    )

    extract_to_staging_product_task = VerticaOperator(
        task_id="extract_to_staging_product_task",
        vertica_conn_id="vertica_conn",
        sql=Template(extract_to_staging_product).render(sql_context),
    )

    delete_duplicates_single_staging_task = VerticaOperator(
        task_id="delete_duplicates_single_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(delete_duplicates_single_staging).render(sql_context),
    )

    transform_inactive_to_single_staging_task = VerticaOperator(
        task_id="transform_inactive_to_single_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_inactive_to_single_staging).render(sql_context),
    )

    transform_non_inactive_to_single_staging_task = VerticaOperator(
        task_id="transform_active_to_single_staging_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_non_inactive_to_single_staging).render(sql_context),
    )

    transform_inactive_to_single_view_task = VerticaOperator(
        task_id="transform_inactive_to_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_inactive_to_single_view).render(sql_context),
    )

    create_view_temp_product_id_inactive_task = VerticaOperator(
        task_id="create_view_temp_product_id_inactive_task",
        vertica_conn_id="vertica_conn",
        sql=Template(create_view_temp_product_id_inactive).render(sql_context),
    )

    update_product_id_in_temp_inactive_task = VerticaOperator(
        task_id="update_product_id_in_temp_inactive_task",
        vertica_conn_id="vertica_conn",
        sql=Template(update_product_id_in_temp_inactive).render(sql_context),
    )

    insert_to_product_history_task = VerticaOperator(
        task_id="insert_to_product_history_task",
        vertica_conn_id="vertica_conn",
        sql=Template(insert_to_product_history).render(sql_context),
    )

    delete_inactive_in_single_view_task = VerticaOperator(
        task_id="delete_inactive_in_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(delete_inactive_in_single_view).render(sql_context),
    )
    
    transform_non_inactive_to_single_view_task = VerticaOperator(
        task_id="transform_non_inactive_to_single_view_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_non_inactive_to_single_view).render(sql_context),
    )

    delete_duplicates_staging_task >> extract_to_staging_task >> validate_json_task >> extract_to_staging_product_task >> delete_duplicates_single_staging_task >> transform_inactive_to_single_staging_task >> transform_non_inactive_to_single_staging_task >> transform_inactive_to_single_view_task >> create_view_temp_product_id_inactive_task >> update_product_id_in_temp_inactive_task >> insert_to_product_history_task >> delete_inactive_in_single_view_task >> transform_non_inactive_to_single_view_task

