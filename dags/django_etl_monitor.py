import sys
import os
sys.path.append('/opt/airflow/etl_tool')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_tool.settings')
print("PYTHON EXECUTABLE:", sys.executable)
print("DAG FILE IS BEING PARSED")

import django
django.setup()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import re
from etl_app.models import ETLJob
from etl_app.etl.etl_executor import run_etl_job

# NOTE: For node-wise DAGs, use the generator script to create a new DAG file for each pipeline.
# This file is now a placeholder/example only.
def run_node(node_id, node_data, **context):
    print(f'Running node {node_id} of type {node_data.get("type")}', flush=True)
    if node_data.get("type") == "output":
        job = ETLJob.objects.latest('id')
        run_etl_job(job)

        
def make_task_id(label, node_id):
    base = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', label)[:40]
    return f"{base}_{node_id}"

# Read the latest pipeline JSON at DAG parse time, with error handling
pipeline_path = '/opt/airflow/shared/etl_jobs.json'
try:
    with open(pipeline_path) as f:
        pipeline = json.load(f)
    nodes = pipeline["nodes"]
    edges = pipeline["edges"]
    print(f"[DAG DEBUG] Loaded nodes: {nodes}")
    print(f"[DAG DEBUG] Loaded edges: {edges}")
except Exception as e:
    print(f"[DAG WARNING] Error loading pipeline JSON: {e}")
    nodes = []
    edges = []

def get_label(node):
    return node['data'].get('label') or node['data'].get('type') or f"node_{node['id']}"

with DAG(
    dag_id="django_etl_monitor",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"]
) as dag:
    # Dynamically create tasks
    node_id_to_task = {}
    for node in nodes:
        label = get_label(node)
        task_id = make_task_id(label, node['id'])
        node_id_to_task[node['id']] = task_id
        locals()[task_id] = PythonOperator(
            task_id=task_id,
            python_callable=run_node,
            op_kwargs={'node_id': node['id'], 'node_data': node['data']}
        )
    # Set dependencies
    for edge in edges:
        src = node_id_to_task[edge['source']]
        tgt = node_id_to_task[edge['target']]
        locals()[src] >> locals()[tgt]
