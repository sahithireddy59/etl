from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
import json

# Add the project root to sys.path so we can import as a package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl_tool.etl_app.etl.etl_executor import run_etl_job

# WARNING: Executing arbitrary Python code is dangerous! Only use this if you trust all job creators.
def run_etl_from_conf(**context):
    conf = context['dag_run'].conf
    print(f"[Airflow DAG] Received conf: {conf}")
    job_id = conf.get("job_id")
    name = conf.get("name")
    source_table = conf.get("source_table")
    target_table = conf.get("target_table")
    transformation_rule = conf.get("transformation_rule")

    missing = []
    for field, value in [("source_table", source_table), ("target_table", target_table)]:
        if not value:
            missing.append(field)
    if "transformation_rule" not in conf:
        missing.append("transformation_rule")
    if missing:
        raise ValueError(f"Missing job configuration in conf: {', '.join(missing)}. Full conf: {conf}")

    # Build a job-like object (can be a simple class or dict with attributes)
    class Job:
        pass
    job = Job()
    job.source_table = source_table
    job.target_table = target_table
    job.transformation_rule = transformation_rule
    job.id = job_id
    job.name = name

    run_etl_job(job)

with DAG(
    dag_id="django_etl_monitor",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["django", "etl"]
) as dag:
    execute_etl = PythonOperator(
        task_id="run_etl_from_django",
        python_callable=run_etl_from_conf,
        provide_context=True
    )
