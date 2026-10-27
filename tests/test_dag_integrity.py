from airflow.models import DagBag

def load_dag_bag():
    return DagBag(dag_folder="airflow/dags", include_examples=False)

def test_dags_import_without_errors():
    assert load_dag_bag().import_errors == {}

def test_pipeline_structure():
    dag = load_dag_bag().dags["banking_fraud_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "fetch_api_and_load_bigquery" in task_ids
    assert any(t.startswith("dbt_transform.") and t.endswith(".run") for t in task_ids)
    ingest = dag.get_task("fetch_api_and_load_bigquery")
    assert ingest.downstream_task_ids, "dbt tasks must run after ingestion"