import unittest
from unittest.mock import patch, MagicMock
import pytest
import logging
from airflow.models import DagBag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import sys
import os
import pandas as pd
from airflow.exceptions import AirflowSkipException
from dags.daily.customer.fetch_true_dtac_employee_daily.helper.helper import download_gcs_file,fetch_and_transform_employee_data


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

dag_id = "fetch_true_dtac_employee_daily"

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
            "gcs_landing_bucket": "test-bucket",
            "gcs_object_name": "test-file.csv",
            "local_file_path": "test-file.csv",
            "grading_schema": "grading_schema",
            "table_name": "employee",
            "conn_string": "sqlite:///test.db",
            "percentage_threshold": 0.1
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
    """Test to check if the DAG is loaded properly."""
    dag = dagbag.get_dag(dag_id=dag_id)
    assert dag is not None
    assert dag.dag_id == dag_id
    assert len(dag.tasks) > 0

def test_task_ids(dagbag):
    """Test to check if the expected tasks exist in the DAG."""
    dag = dagbag.get_dag(dag_id=dag_id)
    
    expected_task_ids = [
        'download_gcs_task',
        'process_employee_data_task'
    ]
    
    for task_id in expected_task_ids:
        assert task_id in dag.task_ids

class TestDownloadGcsFile(unittest.TestCase):

    @patch('dags.daily.customer.fetch_true_dtac_employee_daily.helper.helper.GCSHook')  # Correct mock path
    @patch('os.makedirs')
    @patch('logging.info')  # Mocking logging.info
    def test_download_file_success(self, mock_logging_info, mock_makedirs, mock_gcs_hook):
        # Arrange
        mock_gcs = MagicMock()
        mock_gcs_hook.return_value = mock_gcs
        
        # Mock the exists method to return True (file exists)
        mock_gcs.exists.return_value = True
    
        # Mock the download method
        mock_gcs.download.return_value = None
    
        gcs_bucket_name = 'test-bucket'
        gcs_object_name = 'test-file.parquet'
        destination_file_path = '/local/path/test-file.parquet'
    
        # Act
        download_gcs_file(gcs_bucket_name, gcs_object_name, destination_file_path)
    
        # Assert
        mock_gcs.exists.assert_called_once_with(bucket_name=gcs_bucket_name, object_name=gcs_object_name)
        mock_gcs.download.assert_called_once_with(
            bucket_name=gcs_bucket_name,
            object_name=gcs_object_name,
            filename=destination_file_path
        )
        mock_makedirs.assert_called_once_with(os.path.dirname(destination_file_path), exist_ok=True)
        mock_logging_info.assert_called_with(f"File successfully downloaded to: {destination_file_path}")  # Assert logging call


class TestFetchAndTransformEmployeeData(unittest.TestCase):

    @patch('dags.daily.customer.fetch_true_dtac_employee_daily.helper.helper.create_engine')  # Mock create_engine
    @patch('pandas.read_csv')  # Mock pandas read_csv
    @patch('shutil.rmtree')  # Mock shutil.rmtree
    @patch('logging.info')  # Mock logging.info
    def test_fetch_and_transform_employee_data_not_load(self, mock_logging_info, mock_rmtree, mock_read_csv, mock_create_engine):
        # Arrange: Mock the database engine and execute method
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = None
        
        # Mock the existing_count query to return a value
        mock_engine.execute.return_value.scalar.return_value = 100  # Existing rows
        
        # Generate mock data with 2 rows for the test
        mock_data = {
            "National_ID": ["001", "002"],
            "EmployeeID": ["E001", "E002"],
            "PositionCode": ["P01", "P02"],
            "EngFirstName": ["John", "Jane"],
            "EngLastName": ["Doe", "Smith"],
            "ThaiFirstName": ["จอห์น", "เจน"],
            "ThaiLastName": ["โด", "สมิธ"],
            "Company": ["ABC Corp", "XYZ Ltd"],
        }
        
        # Create DataFrame with 2 rows
        mock_df = pd.DataFrame(mock_data)
        
        # Mocking dropna and drop_duplicates to return the DataFrame itself (or a modified version)
        mock_df.dropna = MagicMock(return_value=mock_df)
        mock_df.drop_duplicates = MagicMock(return_value=mock_df)
        
        # Mock the return of read_csv
        mock_read_csv.return_value = mock_df
        
        # Act: Call the function with test parameters
        fetch_and_transform_employee_data(
            source_file_path='/path/to/file.csv',
            grading_schema='grading_schema',
            table_name='employee_table',
            conn_string='postgresql://user:password@localhost/dbname',
            percentage_threshold=10  # 10% threshold for the test
        )
        
        print(mock_logging_info.call_args_list)
        # mock_logging_info.assert_any_call("Existing rows in database: 100, Rows in new data file: 2, Percentage difference: 98.00%")
        mock_logging_info.assert_any_call("Existing rows in database : 100, Rows in new data file: 2, Percentage difference: 98.00%")
        mock_connection.execute.assert_called_once()  # Ensure a query is executed
    

    @patch('dags.daily.customer.fetch_true_dtac_employee_daily.helper.helper.create_engine')  # Mock create_engine
    @patch('pandas.read_csv')  # Mock pandas read_csv
    @patch('shutil.rmtree')  # Mock shutil.rmtree
    @patch('logging.info')  # Mock logging.info
    def test_fetch_and_transform_employee_data_load(self, mock_logging_info, mock_rmtree, mock_read_csv, mock_create_engine):
        # Arrange: Mock the database engine and execute method
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value = None
        
        # Mock the existing_count query to return a value
        mock_engine.execute.return_value.scalar.return_value = 2  # Existing rows
        
        # Generate mock data with 2 rows for the test
        mock_data = {
            "National_ID": ["001", "002"],
            "EmployeeID": ["E001", "E002"],
            "PositionCode": ["P01", "P02"],
            "EngFirstName": ["John", "Jane"],
            "EngLastName": ["Doe", "Smith"],
            "ThaiFirstName": ["จอห์น", "เจน"],
            "ThaiLastName": ["โด", "สมิธ"],
            "Company": ["ABC Corp", "XYZ Ltd"],
        }
        
        # Create DataFrame with 2 rows
        mock_df = pd.DataFrame(mock_data)
        
        # Mocking dropna and drop_duplicates to return the DataFrame itself (or a modified version)
        mock_df.dropna = MagicMock(return_value=mock_df)
        mock_df.drop_duplicates = MagicMock(return_value=mock_df)
        
        # Mock the return of read_csv
        mock_read_csv.return_value = mock_df
        
        # Act: Call the function with test parameters
        fetch_and_transform_employee_data(
            source_file_path='/path/to/file.csv',
            grading_schema='grading_schema',
            table_name='employee_table',
            conn_string='postgresql://user:password@localhost/dbname',
            percentage_threshold=10  # 10% threshold for the test
        )
        
        log_calls = [call[0][0] for call in mock_logging_info.call_args_list]
        
        expected_log_sequence = [
            "Existing rows in database : 2, Rows in new data file: 2, Percentage difference: 0.00%",
            "Existing data in employee_table has been truncated.",
            "Data successfully loaded into EMPLOYEE table."
        ]
        self.assertEqual(log_calls[0], expected_log_sequence[0])
        self.assertEqual(log_calls[1], expected_log_sequence[1])
        self.assertEqual(log_calls[2], expected_log_sequence[2])
        