import requests

def trigger_airflow_dag_with_job(job):
    trigger_url = f"http://localhost:8081/api/v1/dags/django_etl_monitor/dagRuns"
    auth = ("airflow", "airflow")

    payload = {
        "conf": {
            "job_id": job.id,
            "source_table": job.source_table,
            "target_table": job.target_table,
            "transformation_rule": job.transformation_rule
        }
    }

    trigger_response = requests.post(
        trigger_url,
        json=payload,
        auth=auth,
        headers={"Content-Type": "application/json"}
    )

    print("✅ Airflow trigger response:", trigger_response.status_code)
    print("➡️", trigger_response.text)
