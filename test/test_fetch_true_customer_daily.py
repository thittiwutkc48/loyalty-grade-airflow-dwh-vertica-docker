from unittest.mock import patch, MagicMock
import pytest
import os
import sys
import logging
from airflow.models import DagBag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

dag_id = "fetch_true_customer_daily"

@pytest.fixture(scope="module")
def dagbag():
    project_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dags_folder = f"/{project_base_dir}/airflow-docker-vertica/dags"
    sys.path.append(dags_folder)
    
    with patch('airflow.models.Variable.get') as mock_get:
        mock_get.side_effect = lambda key, default_var=None: {
            "DWH_SCHEMA": "grade",
            "GRADING_SCHEMA": "grade",
            "SENDGRID_API_KEY": "SG.somekey",
            "FROM_EMAIL": "sender@example.com",
            "TO_EMAIL": "receiver@example.com",
        }.get(key, default_var)
    
        with patch('airflow.models.Connection.get_connection_from_secrets') as mock_get_connection_from_secrets, \
                patch.object(PostgresHook, 'get_conn') as mock_postgres_hook, \
                patch.object(GCSHook, 'get_conn') as mock_gcs_hook:

            mock_postgres_conn = MagicMock()
            mock_postgres_conn.conn_id = 'postgres_conn'
            mock_postgres_conn.host = 'localhost'
            mock_postgres_conn.schema = 'public'
            mock_postgres_conn.login = 'test_user'
            mock_postgres_conn.password = 'test_password'
            mock_postgres_conn.port = 5432

            mock_gcp_conn = MagicMock()
            mock_gcp_conn.conn_id = 'gcp_conn'
            mock_gcp_conn.login = 'gcp_user'
            mock_gcp_conn.password = 'gcp_password'

            mock_get_connection_from_secrets.side_effect = lambda conn_id: mock_postgres_conn if conn_id == 'postgres_conn' else mock_gcp_conn

            dag_folder = f"/{dags_folder}/daily/customer/{dag_id}"

            logger.info(f"DAG Folder exists: {os.path.exists(dag_folder)}")

            dagbag = DagBag(dag_folder=dag_folder, include_examples=False)

            logger.info(f"DAGBag Content: {dagbag.dags}")
            logger.info(f"Import Errors: {dagbag.import_errors}")
            
            return dagbag

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id=dag_id)
    
    logger.info(f"DAG Task IDs: {dag.task_ids}")    
    assert dag is not None, f"DAG 'fetch_true_customer_daily' failed to load. Import errors: {dagbag.import_errors}"
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert len(dag.tasks) == 4
    logger.info(f"DAG 'fetch_true_customer_daily' loaded successfully with tasks: {dag.task_ids}")

def test_task_ids(dagbag):
    dag = dagbag.get_dag(dag_id=dag_id)
    
    expected_task_ids = [
        'delete_duplicates_staging_task',
        'extract_to_staging_task',
        'delete_duplicates_single_staging_task',
        'transform_to_single_staging_task'
    ]

    for task_id in expected_task_ids:
        logger.info(f"Checking task ID: {task_id}")
        assert task_id in dag.task_ids, f"Task '{task_id}' is missing in the DAG."

def test_task_dependencies(dagbag):
    dag = dagbag.get_dag(dag_id=dag_id)
    
    logger.info("Checking task dependencies")
    assert dag.get_task('delete_duplicates_staging_task') >> dag.get_task('extract_to_staging_task')
    assert dag.get_task('extract_to_staging_task') >> dag.get_task('delete_duplicates_single_staging_task')
    assert dag.get_task('delete_duplicates_single_staging_task') >> dag.get_task('transform_to_single_staging_task')

def test_task_types(dagbag):
    dag = dagbag.get_dag(dag_id=dag_id)
    
    for task in dag.tasks:
        logger.info(f"Checking task type for task {task.task_id}")
        assert isinstance(task, PostgresOperator), f"Task '{task.task_id}' is not of type PostgresOperator, but {type(task)}"
