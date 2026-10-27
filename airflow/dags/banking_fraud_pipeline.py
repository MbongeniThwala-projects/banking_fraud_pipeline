import os

from datetime import datetime, timedelta, timezone
import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode


#Infrastructure configurations providioned vai Terraform
GCP_PROJECT_ID = "banking-fraud-dev-00215"
DATASET_ID = "raw_banking"
TABLE_ID = "transactions_landing"
BQ_LOCATION = "africa-south1"
MOCK_API_URL = "http://mock-banking-api:8000/v1/transactions"
BATCH_SIZE = 100

DBT_PROJECT_PATH = os.environ.get("DBT_PROJECT_PATH", "/opt/airflow/dbt_banking")
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt_venv/bin/dbt")

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

ALERT_EMAIL = os.environ.get("ALERT_EMAIL")
FAILURES_TABLE_ID = "pipeline_failures"
FAILURES_SCHEMA = [
    bigquery.SchemaField("failed_at", "TIMESTAMP"),
    bigquery.SchemaField("dag_id", "STRING"),
    bigquery.SchemaField("task_id", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("try_number", "INTEGER"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("log_url", "STRING"),
]

def notify_failure(context):
    '''runs once a task has failed andd used up its retries: logs to BigQuery and emails.'''
    ti = context["task_instance"]
    exception = context.get("exception")
    row = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "error_message": str(exception)[:1000] if exception else None,
        "log_url": ti.log_url,
    }

    # each channel is wrapped separately so one failing does not block the other
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
        job_config = bigquery.LoadJobConfig(
            schema=FAILURES_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        client.load_table_from_json(
            [row], f"{GCP_PROJECT_ID}.{DATASET_ID}.{FAILURES_TABLE_ID}", job_config=job_config
        ).result()
    except Exception as err:
        print(f"Could not write failure to BigQuery: {err}")

    try:
        if ALERT_EMAIL:
            send_email(
                to=ALERT_EMAIL,
                subject=f"[Airflow] {ti.dag_id}.{ti.task_id} failed",
                html_content=(
                    f"<p>Task <b>{ti.task_id}</b> in DAG <b>{ti.dag_id}</b> failed.</p>"
                    f"<p>Run: {row['run_id']}<br>Error: {row['error_message']}</p>"
                    f"<p><a href=\"{row['log_url']}\">Open the task log</a></p>"
                ),
            )
    except Exception as err:
        print(f"Could not send failure email: {err}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "email_on_failure": False, #we'll read on cost of being true
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure
}

profile_config = ProfileConfig(
    profile_name="banking_fraud",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
)

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
    dag_id="banking_fraud_pipeline",
    default_args=default_args,
    description="Ingests mock banking transactions, then runs and tests the dbt models",
    schedule_interval="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "ingestions", "dbt"]
) as dag:
    # ingest_task = PythonOperator(task_id="fetch_api_and_load_bigquery", python_callable=fetch_and_load_transactions)
    fetch_api_and_load_bigquery = PythonOperator(
        task_id="fetch_api_and_load_bigquery",
        python_callable=fetch_and_load_transactions
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_executable_path=DBT_EXECUTABLE,
            dbt_deps=False,
            emit_datasets=False
        )
    )

    fetch_api_and_load_bigquery >> dbt_transform
