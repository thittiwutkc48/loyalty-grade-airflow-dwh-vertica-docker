from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from wrapper.default_config import default_config
from airflow.models import Variable

config = Variable.get("wrapper_trigger_backfill",default_var=default_config)
job_id = config["job_id"]
wrapper_start_date = config["start_date"]
wrapper_end_date = config["end_date"]


def trigger_backfill(**kwargs):
    start_date = datetime.strptime(kwargs['start_date'], "%Y-%m-%d")
    end_date = datetime.strptime(kwargs['end_date'], "%Y-%m-%d")
    dag_run_conf = []
    
    current_date = start_date
    while current_date <= end_date:
        partition_date = current_date + timedelta(days=1)
        dag_run_conf.append({
            "job_id": job_id,
            "partition_date": partition_date.strftime("%Y-%m-%d")
        })
        current_date += timedelta(days=1)
    
    return dag_run_conf

def trigger_each_date(**kwargs):
    ti = kwargs['ti']
    dag_run_confs = ti.xcom_pull(task_ids='generate_backfill_dates')
    for conf in dag_run_confs:
        TriggerDagRunOperator(
            task_id=f"trigger_backfill_{conf['partition_date']}",
            trigger_dag_id=job_id,
            conf=conf,
            wait_for_completion=True,
            dag=kwargs['dag']
        ).execute(context=kwargs)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "wrapper_trigger_backfill",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["wrapper", "backfill"],
) as dag:
    
    generate_backfill_dates = PythonOperator(
        task_id="generate_backfill_dates",
        python_callable=trigger_backfill,
        op_kwargs={"start_date": wrapper_start_date, "end_date": wrapper_end_date},
        provide_context=True
    )
    
    trigger_backfills = PythonOperator(
        task_id="trigger_backfills",
        python_callable=trigger_each_date,
        provide_context=True,
        dag=dag
    )
    
    generate_backfill_dates >> trigger_backfills
