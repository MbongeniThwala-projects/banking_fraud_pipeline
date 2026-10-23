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
Infrastructure as Code with Terraform: we will use Terraform to define local GCP infrastructure that airflow pipeline will target for data ingestion and dbt transformation models.