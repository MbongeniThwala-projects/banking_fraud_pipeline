from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator


#Infrastructure configurations providioned vai Terraform
GCP_PROJECT_ID = "banking-fraud-dev-00215"
DATASET_ID = "raw_banking"
TABLE_ID = "transactions_landing"
MOCK_API_URL = "http://mock-banking-api:8000/v1/transactions"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False, #we'll read on cost of being true
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

#taking the data and loading it to tables in BQ
def fetch_and_load_transactions(**kwargs):
    '''Fetches batch payload from local mock api and streams into bigquery landing table'''
    response = requests.get(MOCK_API_URL, timeout=10)
    response.raise_for_status()
    payload = response.json()

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    #Stream JSON payload directly into BQ raw landing table
    errors = client.insert_rows_json(table_ref, payload)
    if errors:
        raise RuntimeError(f"BigQuery ingestion failed with errors: {errors}")

with DAG(
    dag_id="ingest_banking_transactions",
    default_args=default_args,
    description="Batch extraction of mock bankoing transactioins into GCP Bigquery raw landing zone",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["banking", "ingestions", "raw"]
) as dag:
    ingest_task = PythonOperator(task_id="fetch_api_and_load_bigquery", python_callable=fetch_and_load_transactions)

