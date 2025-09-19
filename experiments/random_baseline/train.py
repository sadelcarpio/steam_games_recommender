import argparse
import os
import tempfile

import duckdb
import mlflow

from gamerec.models.random_model_baseline import RandomModel

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--games_data", type=str, required=True)
    args = parser.parse_args()
    duckdb_conn = duckdb.connect(args.games_data, read_only=True)
    games = duckdb_conn.sql("SELECT game_id, DATE_TRUNC('month', game_prerelease_date) AS game_release_month FROM dim_games").pl()
    duckdb_conn.close()
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "games.parquet")
        games.write_parquet(path)
        model_info = mlflow.pyfunc.log_model(
            python_model=RandomModel(k=10),
            artifacts={"games": path},
            name="baseline-random-model-2"
        )
