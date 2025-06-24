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
    job_id = conf.get("job_id") # Optional, but good for tracking
    source_table = conf.get("source_table")
    target_table = conf.get("target_table")
    transformation_rule = conf.get("transformation_rule")

    # Basic validation for core parameters
    if not source_table or not target_table or transformation_rule is None: # transformation_rule can be empty string
        raise ValueError("Missing core job configuration (source_table, target_table, transformation_rule) in conf")

    # Build a job-like object
    class Job:
        pass
    job = Job()
    job.job_id = job_id
    job.source_table = source_table
    job.target_table = target_table
    job.transformation_rule = transformation_rule

    # SCD Type 2 specific parameters
    # These will be None if not provided in conf, and etl_executor will use defaults or handle them
    job.scd_type = conf.get("scd_type")
    job.business_key_column = conf.get("business_key_column")
    job.tracked_attribute_columns = conf.get("tracked_attribute_columns") # This will be a JSON string
    job.scd_start_date_column = conf.get("scd_start_date_column")
    job.scd_end_date_column = conf.get("scd_end_date_column")
    job.scd_is_active_column = conf.get("scd_is_active_column")

    # Print out the job configuration for logging purposes
    print(f"Received job configuration:")
    print(f"  job_id: {job.job_id}")
    print(f"  source_table: {job.source_table}")
    print(f"  target_table: {job.target_table}")
    print(f"  transformation_rule: {job.transformation_rule}")
    print(f"  scd_type: {job.scd_type}")
    print(f"  business_key_column: {job.business_key_column}")
    print(f"  tracked_attribute_columns: {job.tracked_attribute_columns}")
    print(f"  scd_start_date_column: {job.scd_start_date_column}")
    print(f"  scd_end_date_column: {job.scd_end_date_column}")
    print(f"  scd_is_active_column: {job.scd_is_active_column}")

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
