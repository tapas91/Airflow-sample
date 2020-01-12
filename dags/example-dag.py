"""Dag to fetch failed tasks records from a table in AirflowDB and raise
   ServiceNow Incident using REST POST request."""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from plugins.utilites.raise_incident import raise_servnow_inc
from datetime import datetime, timedelta


default_args = {
    'owner': 'todd',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='example_dag',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

#Task to call python function
t1 = PythonOperator(
    task_id='servnow_task',
    python_callable=raise_servnow_inc,
    dag=dag)

#Task to execute a bash command
t2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t1 >> t2
