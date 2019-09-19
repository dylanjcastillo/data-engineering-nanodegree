from datetime import datetime
from airflow import DAG
from airflow.operators import ExecuteQueryFromFileOperator

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Create required tables',
          schedule_interval=None
        )

create_tables = ExecuteQueryFromFileOperator(
    task_id="Create_tables",
    dag=dag,
    query_file="/home/workspace/airflow/create_tables.sql",
    redshift_conn_id="redshift"
)
