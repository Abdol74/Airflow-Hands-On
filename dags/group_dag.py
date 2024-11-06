from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdags_downloads import subdag_downloads
from subdags.subdag_transforms import subdag_transforms
from datetime import datetime
 
with DAG(dag_id='group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
    args = {
        'start_date': dag.start_date,
        'schedule_interval':dag.schedule_interval,
        'catchup': dag.catchup
    }
    # Note: child dag id must be same as task id 
    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(
            parent_dag_id=dag.dag_id,
            child_dag_id='downloads',
            args=args
        )
    )
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 5'
    )

    transforms = SubDagOperator(
        task_id='transforms',
        subdag=subdag_transforms(
            parent_dag_id=dag.dag_id,
            child_dag_id='transforms',
            args=args
        )
    )
    downloads >> check_files >> transforms
