from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from audiopipeline import main  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'metavoice_pipeline_dag',
    default_args=default_args,
    description='Run metavoice pipeline',
    schedule_interval=timedelta(days=1),
)

def run_metavoice_pipeline():
    main()

execute_pipeline = PythonOperator(
    task_id='execute_pipeline',
    python_callable=run_metavoice_pipeline,
    dag=dag,
)

execute_pipeline
