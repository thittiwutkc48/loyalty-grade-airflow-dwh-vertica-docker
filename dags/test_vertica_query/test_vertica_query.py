from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "catchup": False
}

# Define the DAG
with DAG(
    dag_id="test_vertica_query",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    load_data = VerticaOperator(
        task_id="load_data",
        vertica_conn_id="vertica_conn",  # Use the connection ID you set in Airflow UI
        sql="""
        SELECT * FROM CLYMAPPO.stg_dtn_dwo_ccb_cs_subr_pcn Limit 100 ;
        """
    )

    load_data
