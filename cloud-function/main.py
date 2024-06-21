import requests
from google.cloud import bigquery
import logging
import pandas as pd
from datetime import datetime
import os

class Logging:
    def __init__(self):
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO,
            force=True,
            datefmt='%Y-%b-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

class API(Logging):

    def __init__(self) -> None:
        super().__init__()

    def get_request(self, URL: str) -> requests.Response:
        try:
            response = requests.get(URL)
            self.logger.info('Request executado com sucesso!')
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro ao fazer a requisição API: {e}")
            return None

class InsetBigquery(Logging):

    PROJECT_ID = 'etl-teste-karhub'
    DATASET_ID = 'orcamento_saopaulo'
    TABLE_ID = 'orcamento_etl_saopaulo'

    def __init__(self):
        super().__init__()
        self.client = bigquery.Client()

    def insert(self, dataframe: pd.DataFrame):
        self.logger.info('Insert on BigQuery')

        try:
            self.client.get_dataset(f"{self.client.project}.{self.DATASET_ID}")
        except Exception as e:
            dataset = bigquery.Dataset(f"{self.client.project}.{self.DATASET_ID}")
            dataset.location = "US"
            self.client.create_dataset(dataset, timeout=30)
            self.logger.info(f"Dataset {self.client.project}.{self.DATASET_ID} criado.")

        try:
            table = self.client.get_table(f"{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID}")
            schema = [
                bigquery.SchemaField("id_recurso", "STRING"),
                bigquery.SchemaField("recurso_name_x", "STRING"),
                bigquery.SchemaField("total_liquidado", "FLOAT"),
                bigquery.SchemaField("total_arrecadado", "FLOAT"),
                bigquery.SchemaField("dt_insert", "TIMESTAMP"),
            ]
            if table.schema != schema:
                self.client.delete_table(f"{self.client.project}.{self.DATASET_ID}.{self.TABLE_ID}")
                table = bigquery.Table(f"{self.client.project}.{self.DATASET_ID}.{self.TABLE_ID}", schema=schema)
                self.client.create_table(table, timeout=30)
                self.logger.info(f"Tabela {self.TABLE_ID} recriada com o novo esquema.")
        except Exception as e:
            schema = [
                bigquery.SchemaField("id_recurso", "STRING"),
                bigquery.SchemaField("recurso_name_x", "STRING"),
                bigquery.SchemaField("total_liquidado", "FLOAT"),
                bigquery.SchemaField("total_arrecadado", "FLOAT"),
                bigquery.SchemaField("dt_insert", "TIMESTAMP"),
            ]
            table = bigquery.Table(f"{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID}", schema=schema)
            self.client.create_table(table, timeout=30)
            self.logger.info(f"Tabela {self.TABLE_ID} criada.")

        job = self.client.load_table_from_dataframe(dataframe, f"{self.client.project}.{self.DATASET_ID}.{self.TABLE_ID}")
        job.result()
        self.logger.info('ETL finalizado com sucesso!')

class ETLKarhub(Logging):

    URL_DESPESAS = 'gs://etl-gcp-airflow/files/gdvDespesasExcel.csv'
    URL_RECEITAS = 'gs://etl-gcp-airflow/files/gdvReceitasExcel.csv'
    COTACAO = 'https://economia.awesomeapi.com.br/json/daily/USD-BRL/1?start_date=20220622&end_date=20220622'

    def __init__(self) -> None:
        super().__init__()
        self.request_api = API()
        self.bigquery = InsetBigquery()
        self.api = self.request_api.get_request(URL=self.COTACAO)

    def etlprocess(self,) -> None:
        """
        Lendo o dados usando a biblioteca Pandas e adicionando a dica deixada no README.md
        :param None
        """
        self.logger.info('ETL iniciado!')
        despesas_df = pd.read_csv(self.URL_DESPESAS, encoding='utf-8', delimiter=',', usecols={'Fonte de Recursos', 'Despesa', 'Liquidado'})
        receitas_df = pd.read_csv(self.URL_RECEITAS, encoding='utf-8', delimiter=',')

        columns_receita = {
            'Fonte de Recursos': 'recurso_name',
            'Receitas': 'receita',
            'Arrecadado': 'total_arrecadado'
        }
        columns_despesas = {
            'Fonte de Recursos': 'recurso_name',
            'Despesa': 'despesa',
            'Liquidado': 'total_liquidado'
        }

        dados_api = self.api.json()

        self.logger.info("Inicio do processo de processamento")
        despesas_df = despesas_df.rename(columns_despesas, axis=1)
        receitas_df = receitas_df.rename(columns_receita, axis=1)

        receitas_df['total_arrecadado'] = receitas_df['total_arrecadado'].str.replace(',', '.').str.strip()
        receitas_df['total_arrecadado'] = pd.to_numeric(receitas_df['total_arrecadado'], errors='coerce')

        despesas_df['total_liquidado'] = despesas_df['total_liquidado'].str.strip() \
            .str.replace('.', '', regex=False) \
            .str.replace(',', '.', regex=False) \
            .astype(float)

        despesas_df['total_liquidado'] = despesas_df['total_liquidado'] * float(dados_api[0]['high'])
        receitas_df['total_arrecadado'] = receitas_df['total_arrecadado'] * float(dados_api[0]['high'])

        despesas_df = despesas_df.loc[despesas_df['despesa'] != 'TOTAL']
        receitas_df = receitas_df.loc[receitas_df['Receita'] != 'TOTAL']

        receitas_df['id_recurso'] = receitas_df['recurso_name'].str[:3]
        despesas_df['id_recurso'] = despesas_df['recurso_name'].str[:3]
        despesas_df['recurso_name'] = despesas_df['recurso_name'].str[6:]
        receitas_df['recurso_name'] = receitas_df['recurso_name'].str[6:]

        merged_df = pd.merge(despesas_df, receitas_df, on='id_recurso', how='outer')

        merged_df['dt_insert'] = datetime.now()

        final_df = merged_df[['id_recurso', 'recurso_name_x', 'total_liquidado', 'total_arrecadado', 'dt_insert']]
        final_df['dt_insert'] = pd.to_datetime(final_df['dt_insert'])

        self.bigquery.insert(dataframe=final_df)

if __name__ == "__main__":
    def execute_gcf():
        etl = ETLKarhub()
        etl.etlprocess()
