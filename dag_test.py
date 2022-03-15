from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# Constants
# KUBE_CONFIG = '/mnt/airflow/dags/kube_config.yaml'
# KUBE_SERVICE_ACCOUNT = 'mwaa-sa'


# DAG
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': datetime(2021, 1, 1),
   'provide_context': True
}

dag = DAG('kubernetes_pod_example',default_args=default_args, schedule_interval=None)

# Task
dbt_test = KubernetesPodOperator(
                       task_id="dbt-test",
                       name="dbt-test",
                       namespace="airflow",
#                        image="170108258435.dkr.ecr.ap-southeast-2.amazonaws.com/dbt_image_test:latest",
                       image = "dbt_image_test",
                       image_pull_policy = "Never",
                       cmds=["dbt"],
                       arguments=["seed", "--profiles-dir", "."],
                       ## no change on below
                       get_logs=True,
                       dag=dag,
                       is_delete_operator_pod=True,
                     #   config_file=KUBE_CONFIG,
                       in_cluster=True,
                     #   service_account_name=KUBE_SERVICE_ACCOUNT
                       env={
                              'dbt_user': '{{ var.value.dbt_user }}',
                              'dbt_password': '{{ var.value.dbt_password }}',
                              **os.environ
                           }
                       )


# from email.policy import default
# import logging
# from datetime import datetime, timedelta
# from pathlib import Path

# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# log = logging.getLogger(__name__)

# dag = DAG(
#     "example_using_k8s_pod_operator",
#     schedule_interval=None,
#     catchup=False,
#     default_args={
#         "owner": "admin",
#         "depends_on_past": False,
#         "start_date": datetime(2021, 8, 7),
#         "email_on_failure": False,
#         "email_on_retry": False,
#         "retries": 2,
#         "retry_delay": timedelta(seconds=30),
#         "sla": timedelta(hours=23),
#     },
# )

# with dag:
#     task_1 = KubernetesPodOperator(
#         image="170108258435.dkr.ecr.ap-southeast-2.amazonaws.com/dbt_image",
#         namespace=default,
#         cmds=["dbt"],
#         arguments=["seed", "--profiles-dir", ".", "--fail-fast"],
#         labels={"foo": "bar"},
#         name="dbt-test",
#         task_id="task-1-echo",
#         is_delete_operator_pod=False,
#         in_cluster=True,
#     )

# task_1
