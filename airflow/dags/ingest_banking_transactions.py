from datetime import datetime, timedelta, timezone
import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator


#Infrastructure configurations providioned vai Terraform
GCP_PROJECT_ID = "banking-fraud-dev-00215"
DATASET_ID = "raw_banking"
TABLE_ID = "transactions_landing"
BQ_LOCATION = "africa-south1"
MOCK_API_URL = "http://mock-banking-api:8000/v1/transactions"
BATCH_SIZE = 100

SCHEMA = [
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("transaction_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("transaction_amount", "FLOAT"),
    bigquery.SchemaField("transaction_type", "STRING"),
    bigquery.SchemaField("merchant_category_code", "STRING"),
    bigquery.SchemaField("device_ip", "STRING"),
    bigquery.SchemaField("ip_country_code", "STRING"),
    bigquery.SchemaField("card_present_flag", "BOOLEAN"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP")
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "email_on_failure": False, #we'll read on cost of being true
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

#taking the data and loading it to tables in BQ
def fetch_and_load_transactions(**context):
    '''Fetches batch payload from local mock api and streams into bigquery landing table'''
    response = requests.get(MOCK_API_URL, params={"limit": BATCH_SIZE}, timeout=30)
    response.raise_for_status()
    records = response.json().get("data", [])

    if not records:
        print("API returned no records, nothing to load.")
        return 0

    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["ingested_at"] = ingested_at


    client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    #Stream JSON payload directly into BQ raw landing table
    # errors = client.insert_rows_json(table_ref, records)
    # if errors:
    #     raise RuntimeError(f"BigQuery ingestion failed with errors: {errors}")
    '''this config how bigquery should handle loading data into a table using python SDK'''
    """==>> Load this data in to the table using my custom SCHEMA. If the table doesn't exist yet, create it first. If it already exists, append the new records to the existing data"""
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA, #defines explicit col names, data types, and mode for the des table (rather than relying on BQ auto-detection, which can misinterpret)
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, #controls what happens if the target table already has data, `WRITE_APPENDS` adds a new incoming row to the bottom of the existing table without overwriting or deleting current data (WRITE_TRUNCATE: Clears out/deletes all existing data in the table before inserting the new data. WRITE_EMPTY: Fails the job if the target table already contains any data)
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED # controls what hapens if the target table does not exist. `Create_if_needed` tells BQ to create the table based the schema provided (if table not found in the dataset), (CREATE_NEVER: Requires the table to already exist. If it doesn't, the load job will raise an error.)
    )
    job = client.load_table_from_json(records, table_ref, job_config=job_config)
    job.result()

    print(f"Loaded {len(records)} records into {table_ref}.")
    return len(records)

with DAG(
    dag_id="ingest_banking_transactions",
    default_args=default_args,
    description="Batch extraction of mock bankoing transactioins into GCP Bigquery raw landing zone",
    schedule_interval="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["banking", "ingestions", "raw"]
) as dag:
    # ingest_task = PythonOperator(task_id="fetch_api_and_load_bigquery", python_callable=fetch_and_load_transactions)
    fetch_api_and_load_bigquery = PythonOperator(
        task_id="fetch_api_and_load_bigquery",
        python_callable=fetch_and_load_transactions
    )

