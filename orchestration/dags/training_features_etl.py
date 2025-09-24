from datetime import datetime, timezone, timedelta

from airflow.sdk import dag, task


default_args = {
    'owner': 'etl',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='training_features_etl_pipeline',
     default_args=default_args,
     start_date=datetime(2010, 10, 1, tzinfo=timezone.utc),
     schedule="@monthly",
     max_active_runs=1,
     catchup=True)
def reviews_etl_pipeline():

    @task.bash(cwd='/opt/airflow/dbt', env={
        "HOME": "/opt/airflow",
        "MINIO_ENDPOINT": "minio:9000"
    })
    def run_dbt_models():
        return "uv run dbt run --select tag:training --vars 'current_month: \"{{ data_interval_start | ds }}\"'"

    run_dbt_models()


dag_instance = reviews_etl_pipeline()
