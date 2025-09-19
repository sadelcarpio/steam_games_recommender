import argparse
import os
import tempfile

import duckdb
import mlflow

from gamerec.models.pop_model_baseline import PopularityTrainer, PopularityPyFunc

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--games_data", type=str, required=True)
    args = parser.parse_args()
    duckdb_conn = duckdb.connect(args.games_data, read_only=True)
    duckdb_conn.sql(f"""SET s3_region='us-east-1';
                    SET s3_url_style='path';
                    SET s3_use_ssl=false;
                    SET s3_endpoint='localhost:9000';
                    SET s3_access_key_id='';
                    SET s3_secret_access_key='';""")
    recommended_games = duckdb_conn.sql(
        "SELECT game_id, game_name, game_review_month, game_num_reviews, game_num_positive_reviews,"
        "game_num_negative_reviews, game_weighted_score FROM game_features").pl()
    trainer = PopularityTrainer()
    games_score = trainer.fit(recommended_games)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "games_score.parquet")
        games_score.write_parquet(path)
        model_info = mlflow.pyfunc.log_model(
            python_model=PopularityPyFunc(k=10),
            artifacts={"games_score": path},
            name="baseline-pop-model"
        )
