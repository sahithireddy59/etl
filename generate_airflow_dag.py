import json
import re
from pathlib import Path

def make_task_id(label, node_id):
    # Airflow task_ids must be alphanumeric, dashes, dots, or underscores, and <= 250 chars
    # Replace spaces and special chars with underscores, append node_id for uniqueness
    base = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', label)[:40]
    return f"{base}_{node_id}"

def run_node(node_id, node_data, **context):
    # Here you can implement your ETL logic for each node type
    print(f'Running node {node_id} of type {node_data.get("type")}', flush=True)
    # Example: if node_data['type'] == 'filter': ...


def generate_dag(pipeline_json_path, output_dag_path):
    with open(pipeline_json_path) as f:
        pipeline = json.load(f)

    dag_id = pipeline.get("dag_id", "etl_pipeline_dynamic")
    nodes = pipeline["nodes"]
    edges = pipeline["edges"]

    dag_code = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import re

def run_node(node_id, node_data, **context):
    # Here you can implement your ETL logic for each node type
    print(f'Running node {{node_id}} of type {{node_data.get("type")}}', flush=True)
    # Example: if node_data['type'] == 'filter': ...

def make_task_id(label, node_id):
    base = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', label)[:40]
    return f"{{base}}_{{node_id}}"

with DAG(
    dag_id="{dag_id}",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"]
) as dag:
"""

    # Create tasks for each node
    node_id_to_task = {}
    for node in nodes:
        label = node['data'].get('label') or node['data'].get('type') or f"node_{node['id']}"
        task_id = make_task_id(label, node['id'])
        node_id_to_task[node['id']] = task_id
        dag_code += f"    {task_id} = PythonOperator(task_id='{task_id}', python_callable=run_node, op_kwargs={{'node_id': '{node['id']}', 'node_data': {json.dumps(node['data'])}}})\n"

    # Set dependencies based on edges
    for edge in edges:
        src = node_id_to_task[edge['source']]
        tgt = node_id_to_task[edge['target']]
        dag_code += f"    {src} >> {tgt}\n"

    with open(output_dag_path, "w") as f:
        f.write(dag_code)

# Example usage:
# generate_dag("shared/etl_jobs.json", "dags/etl_pipeline_dynamic.py")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python generate_airflow_dag.py <pipeline_json_path> <output_dag_path>")
        sys.exit(1)
    generate_dag(sys.argv[1], sys.argv[2]) 