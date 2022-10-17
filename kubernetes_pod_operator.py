# Dag to launch a pod in a k8s cluster with the custom Python dependencies for an app.py
# https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html


from datetime import timedelta, datetime
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import DAG, macros
from airflow.models import Variable, XCom
from airflow.operators.dummy_operator import DummyOperator
from airflow.hashicorp_vault.VaultOperator import VaultOperator
from kubernetes.client import V1VolumeMount
from kubernetes.client.models.v1_container import V1Container




default_args = {
    'owner':  Variable.get('owner'),
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 1),
    'email': Variable.get('e-mail'),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


IMAGE = 'nexus-repo.site.com/my-domain/jiraapi:3'


dag = DAG(
    'jira_api_',
    default_args=default_args,
    description='',
    schedule_interval="0 6 * * *",
    max_active_runs=1,    
    on_success_callback=VaultOperator.cleanup_xcom
)


start_DAG = DummyOperator(
    task_id='start',
    dag=dag)


set_secrets = VaultOperator(
    task_id='set_secrets',
    provide_context=True,
    secret_path="airflow-secrets",
    secret_name="jira-user",
    dag=dag
)

fin_DAG = DummyOperator(
    task_id='fin',
    dag=dag)

volume_config = {'emptyDir': {}}
tmp_volume = Volume(name='tmp', configs=volume_config)
tmp_volume_mount = VolumeMount('tmp', mount_path='/tmp', sub_path=None, read_only=False)

pod_resources = {
    'request_memory': '512Mi',
    'request_cpu': '0.5',
    'limit_memory': '1G',
    'limit_cpu': '1',
}

init_containers = [
    V1Container(
        image_pull_policy="Always",
        name="kinit",
        image=IMAGE,
        command=["bash"],
        args=[],
        volume_mounts=[V1VolumeMount(name="tmp", mount_path='/tmp')]),
]

loader_args = [
    '--conf', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python',
    '--conf', 'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python', 
    '--master', 'yarn',  
    '--conf', 'spark.submit.deployMode=cluster',
    '--conf', 'spark.yarn.queue=default',
    '--conf', 'spark.driver.memory=8g',
    '--conf', 'spark.executor.memory=8g',
    '--conf', 'spark.executor.cores=4',
    '--conf', 'spark.dynamicAllocation.enabled=true',
    '--conf', 'spark.shuffle.service.enabled=true',  
    '--conf', 'spark.dynamicAllocation.maxExecutors=30',  
    '--conf', 'spark.yarn.appMasterEnv.JIRA_LOGIN=$"{{task_instance.xcom_pull(key="jira-user-login", task_ids=["set_secrets"])[0]}}"',
    '--conf', 'spark.yarn.appMasterEnv.JIRA_PASSWORD=$"{{task_instance.xcom_pull(key="jira-user-password", task_ids=["set_secrets"])[0]}}"',
    '--py-files', 'JiraAPI.py',
    '--archives', 'env.tar.gz#environment',  
    'JiraAPI.py'
]


loader_jira = KubernetesPodOperator(
    namespace="my-domain",
    image=IMAGE,    
    cmds=['bash', '-cx', f'spark-submit {" ".join(loader_args)}'],     
    labels={"app": "jira_app"},
    image_pull_secrets='ttds',
    init_containers=init_containers,   
    name='loader_jira',
    task_id='loader_jira',
    in_cluster=True,
    get_logs=True,
    service_account_name='airflow',    
    is_delete_operator_pod=True,
    volumes=[tmp_volume],
    volume_mounts=[tmp_volume_mount],
    resources=pod_resources,
    dag=dag
   
)



start_DAG  >> set_secrets >> loader_jira >> fin_DAG
