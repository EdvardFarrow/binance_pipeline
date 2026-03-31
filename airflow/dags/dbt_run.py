from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_binance_transform',
    default_args=default_args,
    start_date=datetime(2026, 3, 27), 
    schedule='*/5 * * * *',  # Every 5 min 
    catchup=False,                    
    tags=['dbt', 'binance', 'clickhouse'],
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='''
            export DBT_LOG_PATH=/tmp/dbt_logs
            export DBT_TARGET_PATH=/tmp/dbt_target
            cd /opt/airflow/dbt_project
            /home/airflow/.local/bin/dbt run --profiles-dir .
        '''
    )

    run_dbt