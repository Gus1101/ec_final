import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import pandas as pd

from send_aws_s3 import upload_file_to_s3
from .functions_connections import gerar_datas_ate, requisitar_dados

load_dotenv(".env")

#Variável de ambiente para requisição de API
API_KEY = os.getenv("API_KEY")

# Gera a lista de datas até data final
data_final = datetime.now().date()
datas = gerar_datas_ate(data_final)

#Gera a lista de locais
df_local = pd.read_excel("database/subestacoes_coord.xlsx")
locais = df_local["lat/long"].tolist()

# Realiza as requisições e armazena os resultados
resultados = []
for local in locais:
    for data in datas:
        dados = requisitar_dados(local, data)
        if dados:
            resultados.append(dados)

# Converter a lista de dicionários em um DataFrame
df = pd.json_normalize(resultados)

# Normalizar a parte 'forecastday' dentro de 'forecast'
df_forecast = pd.json_normalize(df['forecast.forecastday'].explode())

# Normalizar a parte 'hour' dentro de 'forecastday'
if 'hour' in df_forecast.columns:
    df_hour = pd.json_normalize(df_forecast['hour'].explode())
    df_forecast = df_forecast.drop(columns=['hour']).join(df_hour, rsuffix='_hour')

# Concatenar com o DataFrame principal
df_final = pd.concat([df.drop(columns='forecast.forecastday'), df_forecast], axis=1)

# Convertendo em parquet e enviando para o raw storage na aws
df_final = df_final.to_parquet("transient/api_weather_data.parquet")

if __name__ == "__main__":
    upload_file_to_s3(file_path="transient/api_weather_data.parquet",bucket_name="rawdatafiapec",object_name="raw_data_weather_api.parquet")