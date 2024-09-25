from connections.send_aws_s3 import upload_file_to_s3

#Carrega a base para a camada gold no S3
upload_file_to_s3(
    file_path="transient/ml_enhanced_gold.parquet",
    bucket_name="mllayerbucket",
    object_name="ml_enhanced.parquet")