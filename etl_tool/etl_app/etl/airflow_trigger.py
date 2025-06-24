import requests
import json

def trigger_airflow_dag_with_job(job):
    trigger_url = f"http://localhost:8081/api/v1/dags/django_etl_monitor/dagRuns"
    auth = ("airflow", "airflow") # Default Airflow auth

    # Basic payload
    conf_payload = {
        "job_id": getattr(job, 'id', None) or getattr(job, 'job_id', None), # Handle if job.id or job.job_id
        "source_table": job.source_table,
        "target_table": job.target_table,
        "transformation_rule": job.transformation_rule
    }

    # Add SCD Type 2 parameters if they exist on the job object
    # These attributes might not exist if the job object is for a non-SCD job
    if hasattr(job, 'scd_type') and job.scd_type:
        conf_payload["scd_type"] = job.scd_type
    if hasattr(job, 'business_key_column') and job.business_key_column:
        conf_payload["business_key_column"] = job.business_key_column
    if hasattr(job, 'tracked_attribute_columns') and job.tracked_attribute_columns:
        # Ensure this is passed as a string if it's not already (e.g. if it's a list)
        # The DAG expects a JSON string for this field in conf.
        # However, if it's already a JSON string from the model/form, that's fine.
        tracked_attrs = job.tracked_attribute_columns
        if isinstance(tracked_attrs, list):
            conf_payload["tracked_attribute_columns"] = json.dumps(tracked_attrs)
        else: # Assume it's a string (hopefully JSON formatted) or None
            conf_payload["tracked_attribute_columns"] = tracked_attrs

    if hasattr(job, 'scd_start_date_column') and job.scd_start_date_column:
        conf_payload["scd_start_date_column"] = job.scd_start_date_column
    if hasattr(job, 'scd_end_date_column') and job.scd_end_date_column:
        conf_payload["scd_end_date_column"] = job.scd_end_date_column
    if hasattr(job, 'scd_is_active_column') and job.scd_is_active_column:
        conf_payload["scd_is_active_column"] = job.scd_is_active_column

    payload = {"conf": conf_payload}

    print(f"Triggering Airflow DAG with payload: {json.dumps(payload, indent=2)}")

    trigger_response = requests.post(
        trigger_url,
        json=payload,
        auth=auth,
        headers={"Content-Type": "application/json"}
    )

    print("✅ Airflow trigger response:", trigger_response.status_code)
    print("➡️", trigger_response.text)
