from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.customer.daily_customers_stg_sgv_to_sgv.default_config import default_config
from daily.customer.daily_customers_stg_sgv_to_sgv.sql.sql_query import (
    transform_dtac_to_dtac_single,
    transform_true_to_true_single,
    create_view_true_dtac_single,
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
    dag_id="daily_customers_stg_sgv_to_sgv",
    default_args=default_args,
    description="Extract and priority process daily customer data from the TRUE/DTAC system.",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["customer", "daily"],
    max_active_runs=1, 
    on_success_callback=None, 
    on_failure_callback=None, 

) as dag:

    
    sql_context = { **default_config }
    
    # wait_for_daily_dtac_customers_stg_to_stg_sgv = ExternalTaskSensor(
    #     task_id='wait_for_daily_dtac_customers_stg_to_stg_sgv',
    #     external_dag_id='daily_dtac_customers_stg_to_stg_sgv',
    #     external_task_id='transform_to_single_staging_task',
    #     allowed_states=['success'],
    #     failed_states=['failed'],
    #     skipped_states=['skipped'],
    #     poke_interval=60,
    #     timeout=3600,
    #     mode='poke',
    #     dag=dag,
    # )

    # wait_for_daily_true_customers_stg_to_stg_sgv = ExternalTaskSensor(
    #     task_id='wait_for_daily_true_customers_stg_to_stg_sgv',
    #     external_dag_id='daily_true_customers_stg_to_stg_sgv',
    #     external_task_id='transform_to_single_staging_task',
    #     allowed_states=['success'],
    #     failed_states=['failed'],
    #     skipped_states=['skipped'],
    #     poke_interval=60,
    #     timeout=3600,
    #     mode='poke',
    #     dag=dag,
    # )

    transform_dtac_to_dtac_single_task = VerticaOperator(
        task_id="transform_dtac_to_dtac_single_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_dtac_to_dtac_single).render(sql_context),
    )

    transform_true_to_true_single_task = VerticaOperator(
        task_id="transform_true_to_true_single_task",
        vertica_conn_id="vertica_conn",
        sql=Template(transform_true_to_true_single).render(sql_context),
    )

    create_view_true_dtac_single_task = VerticaOperator(
        task_id="create_view_true_dtac_single_task",
        vertica_conn_id="vertica_conn",
        sql=Template(create_view_true_dtac_single).render(sql_context),
    )

    # wait_for_daily_dtac_customers_stg_to_stg_sgv >> wait_for_daily_true_customers_stg_to_stg_sgv >> transform_dtac_to_dtac_single_task >> transform_true_to_true_single_task >> create_view_true_dtac_single_task
    transform_dtac_to_dtac_single_task >> transform_true_to_true_single_task >> create_view_true_dtac_single_task

