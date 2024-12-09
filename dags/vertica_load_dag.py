from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

#TRUNCATE grade.example_table ;
with DAG(
    "load_data_to_vertica",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="vertica_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS grade.example_table (
            id INT PRIMARY KEY,
            data VARCHAR(255)
        );
        """,
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="vertica_conn",
        sql="""
        INSERT INTO grade.example_table (id, data) VALUES (99, 'example');
        """,
    )

    create_table >> insert_data
