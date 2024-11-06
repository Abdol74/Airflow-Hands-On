from airflow import DAG
from airflow.operators.bash import BashOperator
from groups.transforms_group import transforms
from groups.downloads_group import downloads
from datetime import datetime
 
with DAG(dag_id='group_dag_task_group', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
    args = {
        'start_date': dag.start_date,
        'schedule_interval':dag.schedule_interval,
        'catchup': dag.catchup
    }
    # Note: child dag id must be same as task id 
    downloads = downloads()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 5'
    )

    transforms = transforms()
    downloads >> check_files >> transforms
