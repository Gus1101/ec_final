#Importa as bibliotecas
import pandas as pd
import numpy as np
from connections.send_aws_s3 import upload_file_to_s3

#Importa as bases
data_api = pd.read_parquet("transient/data_api_gold.parquet")
ocorrencias = pd.read_parquet("transient/interrupcoes-energia-eletrica-2024-gold.parquet")

#Trata a base de ocorrencias
ocorrencias["date"] = ocorrencias["data_inicio_interrup"].dt.date
ocorrencias["date_fim"] = ocorrencias["data_fim_interrup"].dt.date

ocorrencias["date"] = pd.to_datetime(ocorrencias["date"])
ocorrencias["date_fim"] = pd.to_datetime(ocorrencias["date_fim"])

#Une as bases tratadas
data_api = data_api.merge(ocorrencias,left_on=["sigla","date"], right_on=["sigla","date"], how="left")

#Adiciona o discricionario de ocorrencia
data_api["ocorrencia"] = np.where(data_api["tipo_interrupcao"].isna(),0,1)
data_api = data_api.drop_duplicates()

#Salvando a base no ambiente transient
data_api.to_parquet("transient/ml-ready-df-gold.parquet")

#Carrega a base para a camada gold no S3
upload_file_to_s3(
    file_path="transient/ml-ready-df-gold.parquet",
    bucket_name="golddatafiapec",
    object_name="ml_ready_data.parquet")          