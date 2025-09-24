import logging
import os
import tempfile

import duckdb
import mlflow
import pandas as pd

# from gamerec.data import load_data, split_data
from gamerec.models.pop_model_baseline import PopularityTrainer, PopularityPyFunc

# Basic training loop
if __name__ == "__main__":
    # Data loading
    logging.basicConfig(level=logging.INFO)
    logging.info("Loading data ...")
    with duckdb.connect("../../data/steam.duckdb", read_only=True) as duckdb_conn:
        duckdb_conn.sql(f"""SET s3_region='us-east-1';
                        SET s3_url_style='path';
                        SET s3_use_ssl=false;
                        SET s3_endpoint='localhost:9000';
                        SET s3_access_key_id='';
                        SET s3_secret_access_key='';""")
        mlflow.set_tracking_uri("http://localhost:5000")
        training_data = duckdb_conn.sql(
            "SELECT game_id, game_name, game_review_month, game_num_positive_reviews FROM game_features").pl()
        ground_truth_users: pd.DataFrame = duckdb_conn.sql(
            "SELECT user_id, current_month, future_game_ids FROM user_features WHERE current_month < '2020-01-01'").df()
    logging.info("Splitting data ...")
    train_df = ground_truth_users[ground_truth_users["current_month"] <= "2017-01-01"].copy()
    test_df = ground_truth_users[ground_truth_users["current_month"] >= "2017-01-01"].copy()
    # Model training
    logging.info("Training model ...")
    trainer = PopularityTrainer()
    output_artifact = trainer.fit(training_data)  # the whole dataset is needed to look up previous month
    # Log model
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "games_score.parquet")
        output_artifact.write_parquet(path)
        model_info = mlflow.pyfunc.log_model(
            python_model=PopularityPyFunc(k=10),
            artifacts={"games_score": path},
            name="baseline-pop-model"
        )
    # Load model
    my_model = mlflow.pyfunc.load_model(model_info.model_uri)
    # Compute predictions
    logging.info("Computing predictions ...")
    train_df["predictions"] = my_model.predict(ground_truth_users).to_pandas().map(lambda x: x.tolist())
    train_df["future_game_ids"] = ground_truth_users["future_game_ids"].map(lambda x: x.tolist())
    test_df["predictions"] = my_model.predict(ground_truth_users).to_pandas().map(lambda x: x.tolist())
    test_df["future_game_ids"] = ground_truth_users["future_game_ids"].map(lambda x: x.tolist())
    # Log datasets
    logging.info("Logging datasets ...")
    train_dataset = mlflow.data.from_pandas(
        train_df,
        targets="future_game_ids",
        predictions="predictions",
        name="ground_truth_users_train_2010-2017"
    )
    test_dataset = mlflow.data.from_pandas(
        test_df,
        targets="future_game_ids",
        predictions="predictions",
        name="ground_truth_users_test_2017-2020"
    )
    mlflow.log_input(train_dataset, context="training")
    mlflow.log_input(test_dataset, context="testing")
    with mlflow.start_run(run_name="train-eval-full", nested=True):
        logging.info("Evaluating model on train data ...")
        mlflow.models.evaluate(
            data=train_dataset,
            extra_metrics=[
                mlflow.metrics.precision_at_k(5),
                mlflow.metrics.precision_at_k(10),
                mlflow.metrics.recall_at_k(5),
                mlflow.metrics.recall_at_k(10),
                mlflow.metrics.ndcg_at_k(5),
                mlflow.metrics.ndcg_at_k(10)
            ]
        )
    with mlflow.start_run(run_name="eval-only-full", nested=True):
        logging.info("Evaluating model on test data ...")
        mlflow.models.evaluate(
            data=test_dataset,
            extra_metrics=[
                mlflow.metrics.precision_at_k(5),
                mlflow.metrics.precision_at_k(10),
                mlflow.metrics.recall_at_k(5),
                mlflow.metrics.recall_at_k(10),
                mlflow.metrics.ndcg_at_k(5),
                mlflow.metrics.ndcg_at_k(10)
            ]
        )
