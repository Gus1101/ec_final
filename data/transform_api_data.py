#Importa as bibliotecas
import pandas as pd
import numpy as np
from connections.send_aws_s3 import upload_file_to_s3

#Importa a base
data_api = pd.read_parquet("transient/api_weather_data.parquet")
coord = pd.read_excel("database/subestacoes_coord.xlsx")

#Tratamento das Colunas
data_api.columns = data_api.columns.str.replace(".","_")
coord.columns = coord.columns.str.lower().str.strip()

#Tratamento dos valores da base de dados de api
data_api["localtion_name"] = data_api["location_name"].apply(lambda x:x.upper().strip())
data_api["location_region"] = data_api["location_region"].apply(lambda x:x.upper().strip())
data_api["location_country"] = data_api["location_country"].apply(lambda x:x.upper().strip())
data_api["location_tz_id"] = data_api["location_tz_id"].apply(lambda x:x.upper().strip())

data_api["date"] = pd.to_datetime(data_api["date"])

#Salvando a base no ambiente transient
data_api.to_parquet("transient/data_api_bronze.parquet")

#Carrega a base para a camada bronze no S3
upload_file_to_s3(
    file_path="transient/data_api_bronze.parquet",
    bucket_name="bronzedatafiapec",
    object_name="bronze_data_api.parquet")

#Tratando as coordenadas da base de coordenadas
coord["split"] = coord["lat/long"].str.split(",")
coord["lat"] = coord["split"].str[0].astype(str)
coord["lon"] = coord["split"].str[1].astype(str).apply(lambda x:x.strip())

coord["new_lat"] = coord["lat"].astype(str).str[:6]
coord["new_lon"] = coord["lon"].astype(str).str[:6]

coord["new_lat/long"] = coord["new_lat"].astype(str) + " , " + coord["new_lon"].astype(str)
coord = coord.drop(columns=["lat/long","split","lat","lon","new_lat","new_lon"])

#Merge das bases e criação de coluna para nome da subestacoes
data_api["location_lat"] = data_api["location_lat"].astype(str).apply(lambda x:x.strip())
data_api["location_lon"] = data_api["location_lon"].astype(str).apply(lambda x:x.strip())
data_api["new_lat/long"] = data_api["location_lat"] + " , " + data_api["location_lon"]
data_api = data_api.merge(coord[["new_lat/long","nome","sigla"]],left_on="new_lat/long",right_on="new_lat/long",how="left")

data_api["location_name_final"] = np.where(data_api["nome"].isna(),data_api["location_name"],data_api["nome"])
data_api["location_name_final"] = data_api["location_name_final"].astype(str).apply(lambda x:x.upper().strip())

#Salvando a base no ambiente transient
data_api.to_parquet("transient/data_api_silver.parquet")

#Carrega a base para a camada silver no S3
upload_file_to_s3(
    file_path="transient/data_api_silver.parquet",
    bucket_name="silverdatafiapec",
    object_name="silver_data_api.parquet")

#Filtrando a base para camada gold
data_api_mantain = ["location_name_final","sigla","date","day_avgtemp_c","day_maxwind_kph","day_totalprecip_mm","day_avghumidity"]
data_api = data_api[data_api_mantain]

#Salvando a base no ambiente transient
data_api.to_parquet("transient/data_api_gold.parquet")

#Carrega a base para a camada gold no S3
upload_file_to_s3(
    file_path="transient/data_api_gold.parquet",
    bucket_name="golddatafiapec",
    object_name="gold_data_api.parquet")