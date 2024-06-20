import pandas as pd
from datetime import datetime
from dags.api_data import API
from dags.insert import InsetBigquery, Logging

URL_DESPESAS = 'gs://etl-gcp-airflow/files/gdvDespesasExcel.csv'
URL_RECEITAS = 'gs://etl-gcp-airflow/files/gdvReceitasExcel.csv'
COTACAO = 'https://economia.awesomeapi.com.br/json/daily/USD-BRL/1?start_date=20220622&end_date=20220622'

class ETLKarhub(Logging):

  def __init__(self) -> None:
    super().__init__()
    self.request_api = API()
    self.bigquery = InsetBigquery()
    self.api = self.request_api.get_request(URL=COTACAO)


  def etlprocess(self,) -> None: 
    """
    Lendo o dados usando a biblioteca Pandas e adicionando a dica deixada no README.md
    :params None
    """ 
    self.logger.info('ETL iniciado!')   
    despesas_df = pd.read_csv(URL_DESPESAS, encoding='utf-8', delimiter=',', usecols={'Fonte de Recursos', 'Despesa', 'Liquidado'})
    receitas_df = pd.read_csv(URL_RECEITAS, encoding='utf-8', delimiter=',')

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
    x9 = ETLKarhub()
    x9.etlprocess()
