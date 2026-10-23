from datetime import datetime, timedelta

GCP_PROJECT_ID = "banking-fraud-dev-00215"
DATASET_ID = "raw_banking"
TABLE_ID = "transactions_landing"
MOCK_API_URL = "http://mock_api:8000/v1/transactions"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False, #we'll read on cost of being true
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}