from datetime import datetime, timedelta
from functools import partial

from airflow.sdk import task, dag

from utils import table_exists

default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}


@dag(dag_id='reviews_etl_pipeline',
     default_args=default_args,
     start_date=datetime(2025, 8, 10),
     catchup=False)
def reviews_etl_pipeline():

    @task.skip_if(partial(table_exists, "raw_games"))
    @task.bash(
        cwd='/opt/airflow/scraping',
        env={"FIRESTORE_EMULATOR_HOST": "firestore-emulator:8080", "MINIO_ENDPOINT_URL": "http://minio:9000"}
    )
    def start_reviews_scraping():
        return "uv run python -m steam_reviews.steam_reviews"

    @task.bash(cwd='/opt/airflow/dbt', env={"MINIO_ENDPOINT": "minio:9000"})
    def run_dbt_models():
        return "uv run dbt run --exclude tag:scraping"

    start_game_scraping = start_reviews_scraping()
    run_dbt_models = run_dbt_models()

    start_game_scraping >> run_dbt_models


dag_instance = reviews_etl_pipeline()
