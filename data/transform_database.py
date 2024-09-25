#Importa as bibliotecas
import pandas as pd
import sys
import os

from connections.send_aws_s3 import upload_file_to_s3
from .functions_data import round_datetime_to_nearest_hour

# Adiciona o caminho do diretório raiz ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#Importa a base
dados_interrupcoes_energia = pd.read_parquet("database/interrupcoes-energia-eletrica-2024.parquet")

#Carrega a base para a camada raw no S3
upload_file_to_s3(
    file_path="database/interrupcoes-energia-eletrica-2024.parquet",
    bucket_name="rawdatafiapec",
    object_name="raw_data_ocorrencias.parquet")

#Processo de limpeza de dados
#Dropando valores nulos
dados_interrupcoes_energia = dados_interrupcoes_energia.dropna()

#Tratamento das colunas
dados_interrupcoes_energia_columns_mantain = ["dscsubestacaodistribuicao","dsctipointerrupcao","idemotivointerrupcao",
                                   "datiniciointerrupcao","datfiminterrupcao","dscfatogeradorinterrupcao"]

dados_interrupcoes_energia_columns_rename = {"dscsubestacaodistribuicao":"sigla","dsctipointerrupcao":"tipo_interrupcao",
                                  "idemotivointerrupcao":"motivo_interrup","datiniciointerrupcao":"data_inicio_interrup",
                                  "datfiminterrupcao":"data_fim_interrup","dscfatogeradorinterrupcao":"desc_fato_gerador"}

dados_interrupcoes_energia = dados_interrupcoes_energia[dados_interrupcoes_energia_columns_mantain]
dados_interrupcoes_energia = dados_interrupcoes_energia.rename(columns=dados_interrupcoes_energia_columns_rename)

#Criando colunas e tratando valores
dados_interrupcoes_energia["desc_fato_gerador"] = dados_interrupcoes_energia["desc_fato_gerador"].apply(lambda x:x.replace(";"," - ").replace("/"," - "))
dados_interrupcoes_energia["split_fato"] = dados_interrupcoes_energia["desc_fato_gerador"].str.split("-")

dados_interrupcoes_energia["tipo_desc"] = dados_interrupcoes_energia["split_fato"].str[0]
dados_interrupcoes_energia["programacao_desc"] = dados_interrupcoes_energia["split_fato"].str[1]
dados_interrupcoes_energia["classificacao_desc"] = dados_interrupcoes_energia["split_fato"].str[2]
dados_interrupcoes_energia["motivo_desc"] = dados_interrupcoes_energia["split_fato"].str[3]

dados_interrupcoes_energia["tipo_desc"] = dados_interrupcoes_energia["tipo_desc"].apply(lambda x:x.upper().strip())
dados_interrupcoes_energia["programacao_desc"] = dados_interrupcoes_energia["programacao_desc"].astype(str)
dados_interrupcoes_energia["programacao_desc"] = dados_interrupcoes_energia["programacao_desc"].apply(lambda x:x.upper().strip())
dados_interrupcoes_energia["classificacao_desc"] = dados_interrupcoes_energia["classificacao_desc"].astype(str)
dados_interrupcoes_energia["classificacao_desc"] = dados_interrupcoes_energia["classificacao_desc"].apply(lambda x:x.upper().strip())
dados_interrupcoes_energia["motivo_desc"] = dados_interrupcoes_energia["motivo_desc"].astype(str)
dados_interrupcoes_energia["motivo_desc"] = dados_interrupcoes_energia["motivo_desc"].apply(lambda x:x.upper().strip())

dados_interrupcoes_energia["tipo_interrupcao"] = dados_interrupcoes_energia["tipo_interrupcao"].astype(str)
dados_interrupcoes_energia["tipo_interrupcao"] = dados_interrupcoes_energia["tipo_interrupcao"].apply(lambda x:x.upper().strip())
dados_interrupcoes_energia["tipo_interrupcao"] = dados_interrupcoes_energia["tipo_interrupcao"].apply(lambda x:x.replace("NÃO PROGRAMADA","NAO PROGRAMADA"))

#Trativas Finais
dados_interrupcoes_energia["tipo_desc"] = dados_interrupcoes_energia["tipo_desc"].apply(lambda x:x.replace("INTERNO","INTERNA").replace("EXTERNO","EXTERNA"))
dados_interrupcoes_energia["programacao_desc"] = dados_interrupcoes_energia["programacao_desc"].apply(lambda x:x.replace("NÃO PROGRAMADA","NAO PROGRAMADA").replace("NAO PROGAMADA","NAO PROGRAMADA"))

substituicoes = {
    "": "NAO CLASSIFICADA",
    "NÃO CLASSIFICADA": "NAO CLASSIFICADA",
    "PRÓPRIAS DO SISTEMA": "PROPRIAS DO SISTEMA",
    "ALTERAÇÃO": "ALTERACAO",
    "MANUTENÇÃO": "MANUTENCAO",
    "MANUTENÇAO": "MANUTENCAO",
    "ALTERAÇAO": "ALTERACAO",
    "ALÍVIO DE CARGA": "ALIVIO DE CARGA"
}

# Aplicando as substituições usando map
dados_interrupcoes_energia["classificacao_desc"] = dados_interrupcoes_energia["classificacao_desc"].replace(substituicoes)
dados_interrupcoes_energia = dados_interrupcoes_energia.drop(columns=["desc_fato_gerador","split_fato","motivo_interrup"])

#Salvando a base no ambiente transient
dados_interrupcoes_energia.to_parquet("transient/interrupcoes-energia-eletrica-2024.parquet")

#Carrega a base para a camada bronze no S3
upload_file_to_s3(
    file_path="transient/interrupcoes-energia-eletrica-2024.parquet",
    bucket_name="bronzedatafiapec",
    object_name="bronze_data_ocorrencias.parquet")

#Arredondando os dados de datetime
dados_interrupcoes_energia["data_inicio_interrup"] = pd.to_datetime(dados_interrupcoes_energia["data_inicio_interrup"])
dados_interrupcoes_energia["data_inicio_interrup"] = dados_interrupcoes_energia["data_inicio_interrup"].apply(lambda x:round_datetime_to_nearest_hour(x))

dados_interrupcoes_energia["data_fim_interrup"] = pd.to_datetime(dados_interrupcoes_energia["data_fim_interrup"])
dados_interrupcoes_energia["data_fim_interrup"] = dados_interrupcoes_energia["data_fim_interrup"].apply(lambda x:round_datetime_to_nearest_hour(x))

#Salvando a base no ambiente transient
dados_interrupcoes_energia.to_parquet("transient/interrupcoes-energia-eletrica-2024-silver.parquet")

#Carrega a base para a camada bronze no S3
upload_file_to_s3(
    file_path="transient/interrupcoes-energia-eletrica-2024-silver.parquet",
    bucket_name="silverdatafiapec",
    object_name="silver_data_ocorrencias.parquet")

#Filtrando os dados necessários para a camada gold

classificacao_desc = ["MEIO AMBIENTE","FALHA OPERACIONAL","FALHA DE MATERIAL OU EQUIPAMENTO"
                      ,"DESCARGA ATMOSFERICA","VENDAVAL","INTERRUPCAO EMERGENCIAL DO SUPRIMENTO","ARVORE OU VEGETACAO"
                      ,"ACIDENTE","ABALROAMENTO"]

dados_interrupcoes_energia = dados_interrupcoes_energia[(dados_interrupcoes_energia["classificacao_desc"].isin(classificacao_desc))]

#Salvando a base no ambiente transient
dados_interrupcoes_energia.to_parquet("transient/interrupcoes-energia-eletrica-2024-gold.parquet")

#Carrega a base para a camada gold no S3
upload_file_to_s3(
    file_path="transient/interrupcoes-energia-eletrica-2024-gold.parquet",
    bucket_name="golddatafiapec",
    object_name="gold_data_ocorrencias.parquet")                                                    