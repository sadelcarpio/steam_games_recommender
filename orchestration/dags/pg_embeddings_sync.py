from datetime import datetime, timedelta

from airflow.sdk import task, dag

default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='pg_embeddings_sync',
     default_args=default_args,
     start_date=datetime(2025, 8, 10),
     schedule="0 10 * * WED#1",
     catchup=False)
def pg_embeddings_sync():
    @task.bash(cwd='/opt/airflow/dbt')
    def load_dim_games_into_postgres():
        return "uv run python -m steam_games.get_all_steam_games"

dag_instance = pg_embeddings_sync()