from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from jinja2 import Template
from datetime import datetime, timedelta
from daily.customer.fetch_single_customer_merge_daily.default_config import default_config
from daily.customer.fetch_single_customer_merge_daily.sql.sql_query import (
    upsert_new_and_existing_to_single,
    emp_new_existing_to_single,
    update_emp_quit_single,
    update_emp_info_to_single,
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
    dag_id="fetch_single_customer_merge_daily",
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

    upsert_new_and_existing_to_single_task = PostgresOperator(
            task_id="upsert_new_and_existing_to_single_task",
            postgres_conn_id="postgres_conn",
            sql=Template(upsert_new_and_existing_to_single).render(sql_context),
        )
    
    emp_new_existing_to_single_task = PostgresOperator(
            task_id="emp_new_existing_to_single_task",
            postgres_conn_id="postgres_conn",
            sql=Template(emp_new_existing_to_single).render(sql_context),
        )
    
    update_emp_quit_single_task = PostgresOperator(
            task_id="update_emp_quit_single_task",
            postgres_conn_id="postgres_conn",
            sql=Template(update_emp_quit_single).render(sql_context),
        )
    
    update_emp_info_to_single_task = PostgresOperator(
            task_id="update_emp_info_to_single_task",
            postgres_conn_id="postgres_conn",
            sql=Template(update_emp_info_to_single).render(sql_context),
        )


    # wait_for_fetch_true_customer_daily_task >> wait_for_fetch_dtac_customer_daily_task >> wait_for_fetch_true_dtac_employee_daily_task >> 
    upsert_new_and_existing_to_single_task >> emp_new_existing_to_single_task >> update_emp_quit_single_task >> update_emp_info_to_single_task
