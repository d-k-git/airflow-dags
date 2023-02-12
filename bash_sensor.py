import os
import pendulum

from datetime import datetime, timedelta
from airflow import DAG, macros
from airflow.operators.hive_operator  import HiveOperator
from airflow.contrib.sensors.bash_sensor import BashSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor


local_tz = pendulum.timezone("Europe/Moscow")


SPARK_MAIN_CONF = {
            'master': 'yarn',
            'spark.submit.deployMode': 'cluster'
}


default_args = {
    'owner': Variable.get('owner'),
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 15, tzinfo=local_tz),
    'email': Variable.get('de_email'),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=50)

}

dag = DAG(
    dag_id='bash_test_partitions',
    default_args=default_args,
    description='Test 4 hive',
    schedule_interval="* 15 * * *",
    max_active_runs=2

)

start_DAG = DummyOperator(
    task_id="start",
    dag=dag
)

#curl_brace_open = "{"
#curl_brace_close = "}"

sensor_bash = BashSensor(
    bash_command=f"""
         if [ $(hdfs dfs -ls /warehouse/tablespace/external/hive/datamarts.db/table_name/_SUCCESS | awk -F ' ' ' {print $6}') \> "{{{{ ds }}}}" ]; then true; else false; fi
       """,
    task_id='sensor_bash',
    dag=dag,
    poke_interval=180,
    timeout=60*60*8
    )



submit_spark_job = SparkSubmitOperator(
    application="hdfs://tmp/jars/test_partitions1.jar",
    name=f"dk_demo_spark",
    conf = { **SPARK_MAIN_CONF},
    task_id='demo_spark',
    conn_id='hdp_spark',
    java_class="com.myproject.scripts.Main",
    queue='dev',
    dag=dag

)


start_DAG >> sensor_bash >> submit_spark_job