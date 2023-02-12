import pendulum


from datetime import datetime, timedelta
from airflow import DAG, macros
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor




local_tz = pendulum.timezone("Europe/London")

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
    dag_id='test_partitions',
    default_args=default_args,
    description='',
    schedule_interval="* 15 * * *",
    max_active_runs=2,
    #concurrenscy=1,
    tags=['']
)

start_DAG = DummyOperator(
    task_id="start",
    dag=dag
)


sensors = NamedHivePartitionSensor(
    partition_names = [f"datamarts_db.table_name/partition_key={{{{(execution_date - macros.timedelta(days=3)).strftime('%Y-%m-%d')}}}}"],
    metastore_conn_id = 'hive_metastore',
    task_id = 'sensor_for_test',
    dag = dag,
    poke_interval = 60 * 5,
    timeout = 60 * 60 * 9
)


SPARK_MAIN_CONF = {
            'master': 'yarn',
            'spark.submit.deployMode': 'cluster'
}



submit_spark_job = SparkSubmitOperator(
    application="hdfs://tmp/jars/test_partitions1.jar",
    name=f"dk_demo_spark",
    conf = { **SPARK_MAIN_CONF},
    task_id='demo_spark',
    conn_id='hdp1_spark',
    java_class="com.myproject.scripts.Main",
    queue='dev',
    dag=dag

)


start_DAG >> sensors >> submit_spark_job