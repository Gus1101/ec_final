from dotenv import load_dotenv
import boto3
import os

def upload_file_to_s3(file_path, bucket_name, object_name):
    """
    Faz o upload de um arquivo para um bucket S3.

    :param file_path: Caminho local do arquivo a ser enviado.
    :param bucket_name: Nome do bucket S3.
    :param object_name: Nome do objeto no S3.
    """
    # Variáveis de ambiente
    load_dotenv(".env")

    AWS_KEY = os.getenv('AWS_KEY')
    AWS_S_KEY = os.getenv('AWS_S_KEY')
    AWS_REGION = os.getenv('AWS_REGION')

    # Criar uma sessão com as credenciais padrão
    session = boto3.Session(
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_S_KEY,
        region_name=AWS_REGION  
    )

    # Crie um cliente S3
    s3 = session.client('s3')

    # Faça o upload
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"Upload bem-sucedido: {file_path} para {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Erro ao fazer upload: {e}")