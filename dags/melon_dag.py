import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from extract_genre import create_database

#Dag has one node and on an as need basis 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['ianschulte02@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spotify_melon_dag',
    default_args=default_args,
    description="Monitor genre fetch and write to json file."
)

run_etl = PythonOperator(
    task_id='entire_genre_fetch_etl',
    python_callable=create_database,
    dag=dag
)

run_etl