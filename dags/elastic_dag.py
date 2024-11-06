from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from hooks.elastic.elastic_hook import ElasticHook

def _print_es_info():
    hook = ElasticHook()
    print(hook.info())

with DAG(
    dag_id = 'elastic_dag',
    start_date=datetime(2023,1,1),
    schedule="@daily",
    catchup=False
) as dag:
    
    print_es_info = PythonOperator(
        task_id = 'elastic_info',
        python_callable=_print_es_info
    )