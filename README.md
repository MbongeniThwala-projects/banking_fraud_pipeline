This project aims to simulate a banking data pipeline.

TOOLS USED:
    1. pyhton
    2. docker and docker compose
    3. apache airflow
    4. dbt-core and dbt-postgres
    5. terraform
    6. venv
    7. astronomer-cosmos
    8. faker

We will start by creating a virtual enviroment and activating it
-   python3 -m venv venv
-   source venv/bin/activate

**requirements.txt** in root folder
We will then create a requirements.txt file that lists the packages this project will need (for reproducibility and deployment=> build tools, docker images and CI pipelines can read it to set up the enviroment automatically)
*Something worth noting:* 
    Generate one from your current environment with pip freeze > requirements.txt. This captures every
    installed package, including indirect dependencies, so it’s best run inside a virtual environment to
    avoid dumping unrelated packages

**.gitignore** in root folder
This is to make sure we do not push sensitive information into out repo(github), most of the time, i do not write these, a simply google search has such files but make sure to thoroughly read and proof check them before any commits.

**mock_api/main.py**
This file aims to generate fake banking transactions to simulate real card swipts, transfers... in real time so we can use airflow DAG to query it like a real production API. We will biuld a lightweight FastAPI service that runs on docker.

**mock_api/dockerfile**
create a dockerfile, a lightweight python container => builds a container image: a packaged enviroment that contains your code and its dependencies so that out applicatioin runs the same way anywhere docker runs.

_--------------_________------------------________________------------
Some commands as we go along:

                # Build the mock API image
                -   docker build -t mock-banking-api ./mock_api

                # Run the container locally on port 8000
                -   docker run -d --name banking_api -p 8000:8000 mock-banking-api

                verify it is active by hitting the health check endpoint:
                -   curl http://localhost:8000/health

                once verifies that we are hitting a healthy check point, stop and remove the standalone container:
                -   docker stop banking_api && docker rm banking_api

**docker-compose.yaml** in the root dir
Did not create this file.

**Terraform (IaC)**
Infrastructure as Code with Terraform: we will use Terraform to define local GCP infrastructure that airflow pipeline will target for data ingestion and dbt transformation models. After creating the file, we will use the following commands to initialize and create a plan then create and init a GCP project:

                Initialize terraform
                -   cd terraform && terraform init

                Validate terraform configuration (for sybtax errors or missing arguments)
                -   terraform validate

                Show the plan
                -   terraform plan (probably returns a error if not logged in to GCP)

                Connect to a GCP project
                -   gcloud auth appllication-default login

                Activate the GCP project or create a global unique GCP project ID
                -   gcloud config set project banking-fraud-pipeline-dev-00215 (should be the same as at in the main.tf)
                OR
                - gcloud projects create banking-fraud-dev-00215 --name="Banking Fraud Dev"

                Set the active project and ADC (Application Default Credentials) Quata
                -   gcloud config set project banking-fraud-dev-00215
                -   gcloud auth applicatioin-default set-quota-project banking-fraud-dev-00215  

                Enable BigQuery API in GCP
                gcloud services enable bigquery.googleapis.com --project banking-fraud-dev-00215

                Run Terraform plan
                -   terrafrom plan

                To provision these datasets on GCP
                -   terraform apply

                These can be seen in the GCP UI or you can check on the terminal by:
                -   bq ls --project_id=banking-fraud-dev-00215
                To veiw details:
                -   bq show banking-fraud-dev-00215:raw_banking


We now have our cloud infrastructure which is live, so we can build an ingestion pipeline (using airflow)

**airflow/dags/ingest_banking_transactions.py**
Inside this dir/file, we will create a DAG that handles:
    1. Extracting transaction batches from our local REST API (http://mock_api:8000/v1/transactions)
    2. Ingesting raw JSON payloads directly into BQ dataset `raw_banking`, landing table `transactions_landing`
    3. we exlicitly define the SCHEMA here not not rely on BQ auto schema

    You can run the following comand to verify that Airflow can load and parse the new DAG without syntax or import errors:
    -   docker-compose exec airflow-webserver airflow dag list-import-errors (Expected output: No data found=> this comfirms zero parsing/import errors)
    confirm that the DAG is registered in the Airflow metadata database:
    -   docker-compose exec airflow-webserver airflow dags list | grep ingest_banking_transactions (expected output: youll see a listed output table)

Create the service account and grant roles
Run from the project root:

-   gcloud config set project banking-fraud-dev-00215

-   gcloud iam service-accounts create airflow-ingest \
  --display-name="Airflow ingestion"

-   gcloud projects add-iam-policy-binding banking-fraud-dev-00215 \
  --member="serviceAccount:airflow-ingest@banking-fraud-dev-00215.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

-   gcloud projects add-iam-policy-binding banking-fraud-dev-00215 \
  --member="serviceAccount:airflow-ingest@banking-fraud-dev-00215.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

Create the key and keep it out of Git
-   mkdir -p secrets
gcloud iam service-accounts keys create secrets/gcp-key.json \
  --iam-account=airflow-ingest@banking-fraud-dev-00215.iam.gserviceaccount.com
echo "secrets/" >> .gitignore

Recreate the containers and verify
A plain restart won’t pick up new volumes or environment variables, so recreate them:
-    docker-compose up -d
-    docker-compose exec airflow-scheduler ls /opt/airflow/keys
-    docker-compose exec airflow-scheduler env | grep GOOGLE

One safety check on the key: if .gitignore had no trailing newline, secrets/ may have merged into the last line. Run this to be sure it’s ignored:
-   git check-ignore -v secrets/gcp-key.json

From here I’ll give you each step as soon as you paste the previous output, without stopping to ask first. If you want me to pause somewhere, say so.
Step 8: verify the landed data
-    bq query --use_legacy_sql=false --location=africa-south1 \
'SELECT
   COUNT(*) AS total_rows,
   COUNT(DISTINCT transaction_id) AS distinct_ids,
   MIN(transaction_timestamp) AS earliest,
   MAX(transaction_timestamp) AS latest,
   COUNTIF(ip_country_code = "RU") AS suspicious_rows
 FROM `banking-fraud-dev-00215.raw_banking.transactions_landing`'

Then check the schema BigQuery created:
-   bq show --schema --format=prettyjson banking-fraud-dev-00215:raw_banking.transactions_landing

* Step 8 passed. All 100 rows landed with 100 distinct transaction_id values, and the schema has all 11 fields with TIMESTAMP onctransaction_timestamp and ingested_at. The timestamps span about 24 hours, as the mock API intends.
One number is off from my estimate: suspicious_rows is 11, and I said to expect around 5. The API injects anomalies with a 5% chance per row, so 11 out of 100 is unlikely (roughly a 1 in 100 event) but possible. If you want to rule out a problem, this query checks that the 11 are exactly the injected anomalies:

-   bq query --use_legacy_sql=false --location=africa-south1 \
'SELECT
   COUNTIF(ip_country_code = "RU") AS ru_rows,
   COUNTIF(transaction_amount >= 5000) AS high_value_rows,
   COUNTIF(merchant_category_code = "GAMBLING") AS gambling_rows
 FROM `banking-fraud-dev-00215.raw_banking.transactions_landing`' *

**confirm a scheduler-run DAG succeeds**
This time the scheduler runs the task, as it will every hour, instead of the one-off tasks test. Each run appends 100 new rows with new random IDs, so the table will go from 100 to 200 rows.

    1. Trigger a run
    -   docker-compose exec airflow-scheduler airflow dags trigger ingest_banking_transactions

    The run should show as queued in the output. Your DAG is unpaused, so the scheduler picks it up on its own.

    2. Wait about 30 seconds, then check the run state
    -   docker-compose exec airflow-scheduler airflow dags list-runs -d ingest_banking_transactions

    The newest manual__ run should show success. Your older scheduled__ runs from before the credentials fix will still show failed. That’s expected and doesn’t matter.

    3. Confirm the rows landed
    -   bq query --use_legacy_sql=false --location=africa-south1 \
    'SELECT COUNT(*) AS total_rows, COUNT(DISTINCT ingested_at) AS batches
    FROM `banking-fraud-dev-00215.raw_banking.transactions_landing`'