
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def run_node(node_id, **context):
    print(f'Running node {node_id}')

with DAG(
    dag_id="etl_pipeline_sample",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"]
) as dag:
    node_1 = PythonOperator(task_id='node_1', python_callable=run_node, op_kwargs={'node_id': '1'})
    node_2 = PythonOperator(task_id='node_2', python_callable=run_node, op_kwargs={'node_id': '2'})
    node_3 = PythonOperator(task_id='node_3', python_callable=run_node, op_kwargs={'node_id': '3'})
    node_1 >> node_2
    node_2 >> node_3
