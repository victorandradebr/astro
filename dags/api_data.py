import requests
from insert import Logging


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