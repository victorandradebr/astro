from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from dags.main import ETLKarhub

default_args = {
    'owner': 'Victor Andrade',
}

def run_etl():
    etl = ETLKarhub()
    etl.etlprocess()

with DAG(
    dag_id='etl_gcp_astro',
    default_args=default_args,
    tags=['etl_gcp_astro'],
    description='ETL Karhub',
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
) as dag:

    #upload_to_gcs = LocalFilesystemToGCSOperator(
     #   task_id='upload_to_gcs',
      #  src='/home/victor/Área de Trabalho/APOENA/AWS_ETL/astro/data/gdvDespesasExcel.csv',
       # dst='files',
        #bucket='gs://etl-gcp-airflow',
        #gcp_conn_id='gcp_conn',
    #)

    run_etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl,
    )

    run_etl_task
