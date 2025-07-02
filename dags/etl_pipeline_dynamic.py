
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import re

def run_node(node_id, node_data, **context):
    # Here you can implement your ETL logic for each node type
    print(f'Running node {node_id} of type {node_data.get("type")}', flush=True)
    # Example: if node_data['type'] == 'filter': ...

def make_task_id(label, node_id):
    base = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', label)[:40]
    return f"{base}_{node_id}"

with DAG(
    dag_id="etl_pipeline_dynamic",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"]
) as dag:
    source_data_1 = PythonOperator(task_id='source_data_1', python_callable=run_node, op_kwargs={'node_id': '1', 'node_data': {"label": "source_data", "type": "input"}})
    xyz1_2 = PythonOperator(task_id='xyz1_2', python_callable=run_node, op_kwargs={'node_id': '2', 'node_data': {"label": "xyz1", "type": "output"}})
    Filter_3 = PythonOperator(task_id='Filter_3', python_callable=run_node, op_kwargs={'node_id': '3', 'node_data': {"label": "Filter", "type": "filter", "condition": "age>=40"}})
    Rollup_4 = PythonOperator(task_id='Rollup_4', python_callable=run_node, op_kwargs={'node_id': '4', 'node_data': {"label": "Rollup", "type": "rollup", "groupBy": ["name"], "aggregations": {}}})
    source_data_1 >> Filter_3
    Filter_3 >> Rollup_4
    Rollup_4 >> xyz1_2
