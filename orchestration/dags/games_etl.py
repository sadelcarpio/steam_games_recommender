import os
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.decorators import task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import BaseSensorOperator
from docker.types import Mount


class DockerContainerSensor(BaseSensorOperator):
    def poke(self, context):
        container_id = context['ti'].xcom_pull(task_ids='start_game_scraping')
        client = docker.from_env()
        container = client.containers.get(container_id)
        if container.status in ["exited", "dead"]:
            exit_code = container.wait()["StatusCode"]
            if exit_code != 0:
                raise Exception(f"Container exited with non-zero exit code: {exit_code}")
            return True
        return False

default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
        dag_id='games_etl_pipeline',
        default_args=default_args,
        start_date=datetime(2025, 8, 10),
        catchup=False
) as dag:
    get_all_candidate_ids = DockerOperator(
        task_id='get_all_candidate_ids',
        image='steam-games-rec-scraping:latest',
        api_version='auto',
        auto_remove='success',
        command='-m scripts.get_all_steam_games',
        network_mode='steam-games-rec-net',
        docker_url='unix://var/run/docker.sock',
        environment={
            "MINIO_ENDPOINT_URL": "http://minio:9000"
        },
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False
    )

    run_dbt_antijoin = DockerOperator(
        task_id='run_dbt_antijoin',
        image='steam-games-rec-dbt:latest',
        api_version='auto',
        auto_remove='success',
        command='run --select tag:scraping',
        network_mode='steam-games-rec-net',
        docker_url='unix://var/run/docker.sock',
        environment={
            "MINIO_ENDPOINT": "minio:9000"
        },
        mounts=[
            Mount(source=f'{os.getenv("DATA_DIR")}/steam.duckdb',
                  target='/data/steam.duckdb',
                  type='bind')
        ],
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False
    )

    start_game_scraping = DockerOperator(
        task_id='start_game_scraping',
        image='steam-games-rec-scraping:latest',
        api_version='auto',
        auto_remove='success',
        command='-m steam_games.game_data',
        network_mode='steam-games-rec-net',
        docker_url='unix://var/run/docker.sock',
        environment={
            "MINIO_ENDPOINT_URL": "http://minio:9000"
        },
        mounts=[
            Mount(source=f'{os.getenv("DATA_DIR")}/steam.duckdb',
                  target='/data/steam.duckdb',
                  type='bind')
        ],
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        do_xcom_push=True
    )

    run_dbt_models = DockerOperator(
        task_id='run_dbt_models',
        image='steam-games-rec-dbt:latest',
        api_version='auto',
        auto_remove='success',
        command='run --exclude tag:scraping',
        network_mode='steam-games-rec-net',
        docker_url='unix://var/run/docker.sock',
        environment={
            "MINIO_ENDPOINT": "minio:9000"
        },
        mounts=[
            Mount(source=f'{os.getenv("DATA_DIR")}/steam.duckdb',
                  target='/data/steam.duckdb',
                  type='bind')
        ],
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False
    )

    get_all_candidate_ids >> run_dbt_antijoin >> start_game_scraping >> run_dbt_models
