from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.customer.transform_single_customer_merge_daily.default_config import default_config
from daily.customer.transform_single_customer_merge_daily.sql.sql_query import (
    transform_to_single,
    truncate_to_single
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
    dag_id="transform_single_customer_merge_daily",
    default_args=default_args,
    description="Merge daily customer data",
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["customer", "daily"],
    max_active_runs=1, 
    on_success_callback=None, 
    on_failure_callback=None, 

) as dag:

    sql_context = { **default_config }
    
    wait_for_fetch_true_customer_daily_task = ExternalTaskSensor(
        task_id='wait_for_fetch_true_customer_daily_task',
        external_dag_id='fetch_true_customer_daily',
        external_task_id='transform_to_single_staging_task',
        allowed_states=['success'],
        failed_states=['failed'],
        skipped_states=['skipped'],
        poke_interval=60,
        timeout=3600,
        mode='poke',
        dag=dag,
    )

    wait_for_fetch_dtac_customer_daily_task = ExternalTaskSensor(
        task_id='wait_for_fetch_dtac_customer_daily_task',
        external_dag_id='fetch_dtac_customer_daily',
        external_task_id='transform_to_single_staging_task',
        allowed_states=['success'],
        failed_states=['failed'],
        skipped_states=['skipped'],
        poke_interval=60,
        timeout=3600,
        mode='poke',
        dag=dag,
    )

    wait_for_fetch_true_dtac_employee_daily_task = ExternalTaskSensor(
        task_id='wait_for_fetch_true_dtac_employee_daily_task',
        external_dag_id='fetch_true_dtac_employee_daily', 
        external_task_id='process_employee_data_task', 
        allowed_states=['success'],
        failed_states=['failed'],
        skipped_states=['skipped'],
        poke_interval=60,
        timeout=3600,
        mode='poke',
        dag=dag,
    )

    truncate_to_single_task = PostgresOperator(
            task_id="truncate_to_single_task",
            postgres_conn_id="postgres_conn",
            sql=Template(truncate_to_single).render(sql_context),
        )
    
    transform_to_single_task = PostgresOperator(
            task_id="transform_to_single_task",
            postgres_conn_id="postgres_conn",
            sql=Template(transform_to_single).render(sql_context),
        )

    wait_for_fetch_true_customer_daily_task >> wait_for_fetch_dtac_customer_daily_task >> wait_for_fetch_true_dtac_employee_daily_task >> truncate_to_single_task >> transform_to_single_task

