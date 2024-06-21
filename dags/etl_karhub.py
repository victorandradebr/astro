from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Victor Andrade',
}

with DAG(
    dag_id='etl_gcp_astro',
    default_args=default_args,
    tags=['etl_gcp_astro'],
    description='ETL Karhub',
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('processamento', tooltip="Taks of ETL") as processamento:

        upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_to_gcs',
            src='/home/victor/Área de Trabalho/APOENA/AWS_ETL/astro/data/',
            dst='files',
            bucket='gs://etl-gcp-airflow',
            gcp_conn_id='gcp_conn',
        )

        invoke_gcf = CloudFunctionInvokeFunctionOperator(
            task_id = "invoke_cf",
            project_id = 'etl-teste-karhub',
            location = 'us-central1',
            gcp_conn_id = 'gcp_conn',
            input_data = {},
            function_id = 'etl_gcp_function',
            dag = dag
        )

    end = DummyOperator(task_id='end')

    start >> processamento >> end
