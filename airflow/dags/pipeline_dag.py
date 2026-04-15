from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import io
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

CONNECTION_STRING = "your_connection_string_here"
FILE_NAME = "yellow_taxi_2024-01.parquet"
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

default_args = {
    "owner": "divyansh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def ingest():
    print("Downloading NYC Taxi data...")
    response = requests.get(URL, stream=True)
    with open(f"/tmp/{FILE_NAME}", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client("bronze")
    file_client = file_system_client.get_file_client(FILE_NAME)
    with open(f"/tmp/{FILE_NAME}", "rb") as f:
        file_client.upload_data(f.read(), overwrite=True)
    print(f"Ingested {FILE_NAME} to bronze successfully!")

def transform():
    print("Starting transformation...")
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Read from bronze
    file_client = service_client.get_file_system_client("bronze").get_file_client(FILE_NAME)
    data = file_client.download_file().readall()
    df = pd.read_parquet(io.BytesIO(data))

    # Clean data
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_distance"])
    df = df[(df["fare_amount"] > 0) & (df["trip_distance"] > 0) & (df["passenger_count"] > 0)]

    # Add columns
    df["trip_duration_mins"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_dayofweek"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["payment_method"] = df["payment_type"].map({1: "Credit Card", 2: "Cash"}).fillna("Other")

    # Gold aggregation
    df_gold = df.groupby(["pickup_hour", "payment_method"]).agg(
        total_trips=("fare_amount", "count"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean"),
        avg_duration_mins=("trip_duration_mins", "mean"),
        total_revenue=("total_amount", "sum")
    ).reset_index().round(2)

    # Upload silver
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    service_client.get_file_system_client("silver").get_file_client("yellow_taxi_silver_2024-01.parquet").upload_data(buffer.read(), overwrite=True)
    print("Uploaded silver!")

    # Upload gold
    buffer = io.BytesIO()
    df_gold.to_parquet(buffer, index=False)
    buffer.seek(0)
    service_client.get_file_system_client("gold").get_file_client("yellow_taxi_gold_2024-01.parquet").upload_data(buffer.read(), overwrite=True)
    print("Uploaded gold!")

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC Taxi ELT Pipeline — Bronze to Silver to Gold",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=ingest
    )

    transform_task = PythonOperator(
        task_id="transform_to_silver_gold",
        python_callable=transform
    )

    ingest_task >> transform_task