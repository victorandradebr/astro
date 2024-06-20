from google.cloud import bigquery
import logging
import pandas as pd


class Logging:
    def __init__(self,):
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO,
            force=True,
            datefmt='%Y-%b-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

PROJECT_ID = 'etl-teste-karhub'
DATASET_ID = 'orcamento_saopaulo'
TABLE_ID = 'orcamento_etl_saopaulo'

class InsetBigquery(Logging):

    def __init__(self):
        super().__init__()
        self.client = bigquery.Client()

    def insert(self, dataframe: pd.DataFrame):
        self.logger.info('Insert on BigQuery')

        try:
            self.client.get_dataset(f"{self.client.project}.{DATASET_ID}")
        except Exception as e:
            dataset = bigquery.Dataset(f"{self.client.project}.{DATASET_ID}")
            dataset.location = "US"
            self.client.create_dataset(dataset, timeout=30)
            self.logger.info(f"Dataset {self.client.project}.{DATASET_ID} criado.")

        try:
            table = self.client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
            schema = [
                bigquery.SchemaField("id_recurso", "STRING"),
                bigquery.SchemaField("recurso_name_x", "STRING"),
                bigquery.SchemaField("total_liquidado", "FLOAT"),
                bigquery.SchemaField("total_arrecadado", "FLOAT"),
                bigquery.SchemaField("dt_insert", "TIMESTAMP"),
            ]
            if table.schema != schema:
                self.client.delete_table(f"{self.client.project}.{DATASET_ID}.{TABLE_ID}")
                table = bigquery.Table(f"{self.client.project}.{DATASET_ID}.{TABLE_ID}", schema=schema)
                self.client.create_table(table, timeout=30)
                self.logger.info(f"Tabela {TABLE_ID} recriada com o novo esquema.")
        except Exception as e:
            schema = [
                bigquery.SchemaField("id_recurso", "STRING"),
                bigquery.SchemaField("recurso_name_x", "STRING"),
                bigquery.SchemaField("total_liquidado", "FLOAT"),
                bigquery.SchemaField("total_arrecadado", "FLOAT"),
                bigquery.SchemaField("dt_insert", "TIMESTAMP"),
            ]
            table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=schema)
            self.client.create_table(table, timeout=30)
            self.logger.info(f"Tabela {TABLE_ID} criada.")

        job = self.client.load_table_from_dataframe(dataframe, f"{self.client.project}.{DATASET_ID}.{TABLE_ID}")
        job.result()  
        self.logger.info('ETL finalizado com sucesso!')
