from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import os

load_dotenv(".env")

API_KEY = os.getenv("API_KEY")

#Criando funções necessárias para requisição
def gerar_datas_ate(data_final):
    """
    Função para gerar uma lista com datas no ano de 2024
    """
    datas = []
    data_atual = datetime(2024, 1, 1).date()
    while data_atual <= data_final:
        datas.append(data_atual.strftime('%Y-%m-%d'))
        data_atual += timedelta(days=1)
    return datas

def requisitar_dados(local, data):
    """
    Função para realizar requisições em uma API
    """
    url = f'http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={local}&dt={data}'
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        print(f'Erro na requisição para {data}: {resposta.status_code}')
        return None