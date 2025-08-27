from datetime import datetime, timedelta
from functools import partial

from airflow.sdk import task, dag
from airflow.utils.trigger_rule import TriggerRule
from utils import table_exists


default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='games_etl_pipeline',
     default_args=default_args,
     start_date=datetime(2025, 8, 10),
     catchup=False)
def games_etl_pipeline():
    @task.bash(cwd='/opt/airflow/scraping', env={"MINIO_ENDPOINT_URL": "http://minio:9000"})
    def get_all_candidate_ids():
        return "uv run python -m steam_games.get_all_steam_games"

    @task.skip_if(partial(table_exists, "raw_games"))
    @task.bash(cwd='/opt/airflow/dbt', env={"MINIO_ENDPOINT": "minio:9000"})
    def run_dbt_antijoin():
        return "uv run dbt run --select tag:scraping"

    @task.bash(
        cwd='/opt/airflow/scraping',
        env={"MINIO_ENDPOINT_URL": "http://minio:9000"},
        trigger_rule=TriggerRule.NONE_FAILED
    )
    def start_game_scraping():
        return "uv run python -m steam_games.game_data"

    @task.skip_if(partial(table_exists, "raw_reviews"))
    @task.bash(cwd='/opt/airflow/dbt', env={"MINIO_ENDPOINT": "minio:9000"})
    def run_dbt_models():
        return "uv run dbt run --exclude tag:scraping"

    get_all_candidate_ids = get_all_candidate_ids()
    run_dbt_antijoin = run_dbt_antijoin()
    start_game_scraping = start_game_scraping()
    run_dbt_models = run_dbt_models()

    get_all_candidate_ids >> run_dbt_antijoin >> start_game_scraping >> run_dbt_models


dag_instance = games_etl_pipeline()
