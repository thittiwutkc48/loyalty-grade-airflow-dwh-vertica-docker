from airflow.models import Variable

default_config = {
    "job_id": Variable.get("WRAPPER_JOB_ID"),
    "start_date": Variable.get("WRAPPER_JOB_START_DATE"),
    "end_date": Variable.get("WRAPPER_JOB_END_DATE"),
}
