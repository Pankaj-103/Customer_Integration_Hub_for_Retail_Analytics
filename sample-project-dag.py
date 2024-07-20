from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ensure the scripts folder is in the PATH
dag_dir = os.path.dirname(os.path.realpath(__file__))
scripts_dir = os.path.join(dag_dir, 'scripts')
sys.path.append(scripts_dir)

from scripts.superstore_script import main
from scripts.combine_csv_files import combine_csv_files
from scripts.Database_Creation import insert_into_sqlite

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def profile_creation():
    
    main()


def combine_csv_files_execution():

    combine_csv_files()


def insert_into_sqlite_task():

    insert_into_sqlite()


dag = DAG('profile_creation_dag',
          description='DAG for Profile Creation',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 4, 20),
          catchup=False)

task1 = PythonOperator(task_id='create_profiles', 
                       python_callable=profile_creation,
                       dag=dag)

task2 = PythonOperator(task_id='combine_csv_files_task',
                       python_callable=combine_csv_files_execution,
                       dag=dag)


task3 = PythonOperator(task_id='insert_into_sqlite_task',
                       python_callable=insert_into_sqlite,
                       dag=dag)

task2 >> task3 >> task1



