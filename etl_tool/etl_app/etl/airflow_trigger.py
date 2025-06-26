import requests

def trigger_airflow_dag_with_job(job):
    trigger_url = f"http://localhost:8081/api/v1/dags/django_etl_monitor/dagRuns"
    auth = ("airflow", "airflow")

    # Stricter checks and logging (allow empty transformation_rule for one-to-one loads)
    required_fields = [job.source_table, job.target_table]
    if not all(required_fields):
        print(f"❌ Missing required job fields: source_table={job.source_table}, target_table={job.target_table}, transformation_rule={job.transformation_rule}")
        raise ValueError("Missing required job fields for Airflow trigger")

    payload = {
        "conf": {
            "job_id": getattr(job, 'id', None),
            "name": getattr(job, 'name', ''),
            "source_table": job.source_table,
            "target_table": job.target_table,
            "transformation_rule": job.transformation_rule
        }
    }
    print(f"➡️ Triggering Airflow DAG with payload: {payload}")
    trigger_response = requests.post(
        trigger_url,
        json=payload,
        auth=auth,
        headers={"Content-Type": "application/json"}
    )
    print("✅ Airflow trigger response:", trigger_response.status_code)
    print("➡️", trigger_response.text)
