from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'owner',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 6, 1, 0),
    'email': '',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'run_bash',
    default_args = default_args,
    description = 'run_bash',
    schedule_interval = "@once",
    max_active_runs = 1,
    catchup = False
)


start_DAG = DummyOperator(
    task_id = 'start',
    dag = dag
)

fin_DAG = DummyOperator(
    task_id = 'end',
    dag = dag
)

script = """

#yarn application -kill application_1111111_888500

hive -e "drop table stg.table_details"

hdfs dfs -rm -r -skipTrash /warehouse/tablespace/external/hive/stg.db/table_details

"""

bash = BashOperator(
    task_id='bash',
    bash_command=script,
    dag=dag
)


start_DAG >> bash >> fin_DAG
