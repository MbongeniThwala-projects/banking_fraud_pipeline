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
This inside this dir/file, we will create a DAG that handles:
    1. Extracting transaction batches from our local REST API
    2. Ingesting raw JSON payloads directly into BQ dataset raw_banking, landing table transactions_landing