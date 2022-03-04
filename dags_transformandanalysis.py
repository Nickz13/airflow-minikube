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
with dag:
      task1 = KubernetesPodOperator(
                       task_id="dbt-transform",
                       name="dbt-transform",
                       namespace="airflow",
                       image="170108258435.dkr.ecr.ap-southeast-2.amazonaws.com/dbt_image_test:latest",
                       cmds=["dbt"],
                       arguments=["run", "--models transform", "--profiles-dir", "."],
                       ## no change on below
                       get_logs=True,
                       dag=dag,
                       is_delete_operator_pod=False,
                     #   config_file=KUBE_CONFIG,
                       in_cluster=True,
                     #   service_account_name=KUBE_SERVICE_ACCOUNT
                       )
    task2 = KubernetesPodOperator(
                       task_id="dbt-analysis",
                       name="dbt-analysis",
                       namespace="airflow",
                       image="170108258435.dkr.ecr.ap-southeast-2.amazonaws.com/dbt_image_test:latest",
                       cmds=["dbt"],
                       arguments=["run", "--models analysis", "--profiles-dir", "."],
                       ## no change on below
                       get_logs=True,
                       dag=dag,
                       is_delete_operator_pod=False,
                     #   config_file=KUBE_CONFIG,
                       in_cluster=True,
                     #   service_account_name=KUBE_SERVICE_ACCOUNT
                       )
task1>>task2
