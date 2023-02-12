import pendulum
from datetime import timedelta, datetime
from airflow import DAG, macros

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from beeline.airflow.hashicorp_vault.VaultOperator import VaultOperator
from airflow.operators.bash_operator import BashOperator

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator


local_tz = pendulum.timezone("Europe/Moscow")

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
    'kafka_log',
    default_args=default_args,
    description='ETL for table datamarts_db.kafka_log',
    schedule_interval="0 6 * * *",
    #dt.timedelta(hours=8),
    tags=[''],
    catchup=False,
    on_success_callback=VaultOperator.cleanup_xcom,
    on_failure_callback=VaultOperator.cleanup_xcom
)


set_secrets1 = VaultOperator(
    task_id='set_secrets1',
    provide_context=True,
    secret_path="",
    secret_name="airflow-loader",
    dag=dag
)


set_secrets2 = VaultOperator(
    task_id='set_secrets2',
    provide_context=True,
    secret_path="",
    secret_name="server_truststore_jks",
    dag=dag
)

start_DAG = DummyOperator(
    task_id='start',
    dag=dag
)

fin_DAG = DummyOperator(
    task_id='fin',
    dag=dag)

spark_conf = {
            'spark.submit.deployMode': 'cluster',
             'spark.driver.memory': '10g',
             'spark.driver.maxResultSize': '4g',
             'spark.executor.memory': '3g',
             'spark.executor.cores': '4',
             'spark.dynamicAllocation.enabled':'true',
             'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
             'spark.dynamicAllocation.maxExecutors': '30',
             'spark.driver.extraJavaOptions':'-Djava.security.auth.login.config=${kafka_client_jaas.conf}',
             'spark.executor.extraJavaOptions':'-Djava.security.auth.login.config=${kafka_client_jaas.conf}'}


submit_spark_job = SparkSubmitOperator(
   task_id = "load_kafka_log",
   name = "kafka_log",
   conn_id = "hdp_spark3",
   application="hdfs://apps/kafka_consumer/version/latest/kafka-consumer-assembly.jar",
   java_class="com.myproject.scripts.KafkaConsumer",
   queue='prod',
   conf = spark_conf,
   application_args = [
   "--kafka-bootstrap", "10.00.100.11:0000","10.00.100.12:0000",
   "--kafka-topics", "prod.kafka.topic.1",
   "--kafka-username", "{{task_instance.xcom_pull(key='prod-login', task_ids=['set_secrets1'])[0]}}",
   "--kafka-password", "{{task_instance.xcom_pull(key='prod-password', task_ids=['set_secrets1'])[0]}}",
   "--truststore-location", "server.truststore.jks",
   "--truststore-password", "{{task_instance.xcom_pull(key='server_truststore_jks-pass_trust', task_ids=['set_secrets2'])[0]}}",
   "--jaas-conf-location","kafka_client_jaas.conf"   ],
   jars="hdfs://apps/spark-sql-kafka-0-10_2.12-3.0.1.jar,hdfs://apps/kafka-clients-2.4.1.jar,hdfs://apps/spark-token-provider-kafka-0-10_2.12-3.0.1.jar,hdfs://apps/commons-pool2-2.6.2.jar,hdfs://apps/scallop_2.12-3.0.1.jar",
   files="hdfs://apps/kafka_consumer/version/latest/kafka_client_jaas.conf#kafka_client_jaas.conf,hdfs://apps/kafka_consumer/version/latest/server.truststore.jks#server.truststore.jks",
   dag=dag)



start_DAG  >> set_secrets1 >> set_secrets2 >> submit_spark_job >> fin_DAG
