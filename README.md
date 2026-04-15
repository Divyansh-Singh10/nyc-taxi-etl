# NYC Taxi ELT Pipeline

An end-to-end data engineering pipeline built on Azure that processes 2.96 million NYC taxi trips using the medallion architecture (Bronze → Silver → Gold). Built with PySpark, Apache Airflow, and Azure Data Lake Storage Gen2.

---

## Architecture

Raw Data → Bronze Layer → Silver Layer → Gold Layer
- **Bronze:** Raw NYC taxi parquet files landed in ADLS Gen2 as-is
- **Silver:** Cleaned, validated and enriched data with engineered features
- **Gold:** Business-ready aggregations by hour and payment method

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow | Pipeline orchestration and scheduling |
| PySpark + Databricks | Large-scale data transformation |
| Azure Data Lake Storage Gen2 | Cloud storage (medallion layers) |
| Python + Pandas | Data processing and Azure SDK |
| Docker | Airflow containerisation |

---

## What the Pipeline Does

**Step 1 — Ingest (Bronze)**
Downloads NYC Yellow Taxi trip data from the TLC public dataset and lands it raw into the bronze container in ADLS Gen2. No transformations applied at this stage.

**Step 2 — Transform (Silver)**
Cleans the raw data by removing nulls and invalid records, reducing 2.96M rows to 2.72M validated records. Adds engineered columns: trip duration in minutes, pickup hour, day of week, and a readable payment method label.

**Step 3 — Aggregate (Gold)**
Produces a business-ready summary table grouped by pickup hour and payment method, including total trips, average fare, average distance, average trip duration, and total revenue per group.

---

## Results

- 2,964,624 raw records ingested from January 2024
- 2,723,805 clean records after validation
- 240,819 invalid records removed automatically
- 72 aggregated gold layer rows covering all hours and payment types
- Pipeline scheduled to run automatically every month via Airflow

---

## Project Structure

nyc-taxi-etl/
├── ingest.py                        # Downloads and uploads raw data to bronze
├── airflow/
│   ├── dags/
│   │   └── pipeline_dag.py          # Airflow DAG — orchestrates full pipeline
│   ├── docker-compose.yaml          # Airflow + Postgres Docker setup
│   ├── Dockerfile                   # Custom image with PySpark and Azure libraries
│   └── requirements.txt             # Python dependencies
└── .gitignore

---

## How to Run

**Prerequisites:** Python 3.x, Docker Desktop, Azure Storage Account with ADLS Gen2 enabled

**1. Clone the repository**

git clone https://github.com/Divyansh-Singh10/nyc-taxi-etl.git
cd nyc-taxi-etl

**2. Add your Azure connection string**

Replace the placeholder in both `ingest.py` and `airflow/dags/pipeline_dag.py`:
CONNECTION_STRING = "your_azure_connection_string_here"

**3. Install dependencies**
pip install azure-storage-file-datalake requests

**4. Run the ingestion script**
python ingest.py

**5. Start Airflow**
cd airflow
docker-compose up airflow-init
docker-compose up airflow-webserver airflow-scheduler

**6. Trigger the pipeline**

Open `http://localhost:8080`, login with admin/admin, and trigger the `nyc_taxi_pipeline` DAG.

---

## Data Source

NYC Taxi and Limousine Commission (TLC) Trip Record Data — publicly available at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page