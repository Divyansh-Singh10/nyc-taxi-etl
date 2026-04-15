import requests
import os
from azure.storage.filedatalake import DataLakeServiceClient

CONNECTION_STRING = CONNECTION_STRING = "your_connection_string_here"
CONTAINER_NAME = "bronze"
FILE_NAME = "yellow_taxi_2024-01.parquet"
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

def download_data():
    print("Downloading NYC Taxi data...")
    response = requests.get(URL, stream=True)
    with open(FILE_NAME, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {FILE_NAME}")

def upload_to_bronze():
    print("Uploading to Azure bronze container...")
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(CONTAINER_NAME)
    file_client = file_system_client.get_file_client(FILE_NAME)
    with open(FILE_NAME, "rb") as f:
        data = f.read()
        file_client.upload_data(data, overwrite=True)
    print(f"Uploaded {FILE_NAME} to bronze container successfully!")

download_data()
upload_to_bronze()